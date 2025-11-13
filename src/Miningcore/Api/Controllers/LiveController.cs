// miningcore/src/Miningcore/Api/Controllers/LiveController.cs
using System;
using System.Net;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Miningcore.Api.Responses.Live;
using Miningcore.Configuration;
using Miningcore.Extensions;
using Miningcore.Live;
using Miningcore.Persistence;
using Miningcore.Persistence.Repositories;
using Miningcore.Time;
using Miningcore.Mining;
using Miningcore.Blockchain;
using System.Linq;
using System.Collections.Generic;

namespace Miningcore.Api.Controllers;

[ApiController]
[Route("api/live")]
public class LiveController : ControllerBase
{
    private readonly ClusterConfig clusterConfig;
    private readonly IConnectionFactory cf;
    private readonly IStatsRepository statsRepo;
    private readonly IMasterClock clock;
    private readonly MiningPoolRegistry poolRegistry;

    // ---- Live defaults & safety limits ----
    private const int DefaultWindowSec = 600;   // 10 minutes
    private const int MinWindowSec = 1;
    private const int MaxWindowSec = 1800;      // 30 minutes

    private const int DefaultLimit = 100;
    private const int MaxLimit = 500;

    public LiveController(
        ClusterConfig clusterConfig,
        IConnectionFactory cf,
        IStatsRepository statsRepo,
        IMasterClock clock,
        MiningPoolRegistry poolRegistry)
    {
        this.clusterConfig = clusterConfig;
        this.cf = cf;
        this.statsRepo = statsRepo;
        this.clock = clock;
        this.poolRegistry = poolRegistry;
    }

    // ---------- Helpers ----------
    private static bool IsEquihash(CoinFamily family) =>
        family == CoinFamily.Equihash ||
        family.ToString().Contains("Equihash", StringComparison.OrdinalIgnoreCase);

    private const double Diff1Hash = 4294967296d; // 2^32

    private static string ResolveUnit(CoinFamily family) =>
        IsEquihash(family) ? "Sol/s" : "H/s";

    private static ulong ToU64(long? v) =>
    v.HasValue && v.Value > 0 ? (ulong) v.Value : 0UL;

    private PoolConfig GetPool(string poolId)
    {
        var pool = clusterConfig.Pools?.FirstOrDefault(x =>
            string.Equals(x.Id, poolId, StringComparison.OrdinalIgnoreCase));

        if(pool == null)
            throw new ApiException($"Pool '{poolId}' not found", HttpStatusCode.NotFound);

        return pool;
    }

    private IMiningPool TryGetPoolInstance(string poolId) => poolRegistry.Get(poolId);

    private double DiffToHashrate(PoolConfig poolCfg, double diffSum, int windowSec, IMiningPool poolInst)
    {
        var perSec = diffSum / Math.Max(1d, windowSec);
        if(poolInst != null)
            return poolInst.HashrateFromShares(diffSum, windowSec);

        return IsEquihash(poolCfg.Template.Family) ? perSec : perSec * Diff1Hash;
    }

    private static object MapCoinMeta(PoolConfig poolCfg)
    {
        var t = poolCfg.Template;

        // pick a single "best" block link template for convenience
        string explorerBlockLink = null;
        if(t.ExplorerBlockLinks != null && t.ExplorerBlockLinks.Count > 0)
        {
            // prefer "block", else first available
            if(!t.ExplorerBlockLinks.TryGetValue("block", out explorerBlockLink))
                explorerBlockLink = t.ExplorerBlockLinks.Values.FirstOrDefault();
        }

        return new
        {
            name = t.Name ?? t.Symbol,
            symbol = t.Symbol,
            family = t.Family.ToString(),

            website = t.Website,
            market = t.Market,
            twitter = t.Twitter,
            telegram = t.Telegram,
            discord = t.Discord,

            // payments
            explorerTxLink = t.ExplorerTxLink,
            explorerAccountLink = t.ExplorerAccountLink,

            explorerBlockLink = explorerBlockLink,

            explorerBlockLinks = t.ExplorerBlockLinks
        };
    }

    private static bool IsOnlineFromLast(long lastSeenSec, int windowSec)
    {
        if (lastSeenSec <= 0)
            return false;

        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        var grace = Math.Max(windowSec, Live.LiveHashrateState.OnlineGraceSec);

        return (now - lastSeenSec) <= grace;
    }

    private static object MapPoolStatic(PoolConfig poolCfg)
    {
        // Ports -> ordered array
        var ports = (poolCfg.Ports ?? new Dictionary<int, PoolEndpoint>())
            .OrderBy(kv => kv.Key)
            .Select(kv =>
            {
                var p = kv.Value;
                var vd = p.VarDiff ?? new VarDiffConfig();

                return new
                {
                    port = kv.Key,
                    diff = p.Difficulty,
                    tls = p.Tls,
                    vardiff = new
                    {
                        min = vd.MinDiff,
                        max = vd.MaxDiff,
                        target = vd.TargetTime,
                        retargetSeconds = vd.RetargetTime,
                        delta = vd.VariancePercent
                    }
                };
            })
            .ToArray();

        var pay = poolCfg.PaymentProcessing;
        var feePercent = poolCfg.RewardRecipients != null
            ? (float) poolCfg.RewardRecipients.Sum(x => x.Percentage)
            : 0f;

        return new
        {
            id = poolCfg.Id,
            enabled = poolCfg.Enabled,
            feePercent = feePercent,
            payout = pay == null ? null : new
            {
                scheme = pay.PayoutScheme.ToString(),
                minimumPayment = pay.MinimumPayment,
                recipients = (poolCfg.RewardRecipients ?? Array.Empty<RewardRecipient>())
                    .Select(r => new { address = r.Address, percentage = r.Percentage })
            },
            ports
        };
    }


    private async Task<object> BuildPoolSnapInfoObjectAsync(PoolConfig poolCfg, int windowSec, CancellationToken ct)
    {
        var now = clock.Now;

        // persisted
        var persisted = await cf.Run(con => statsRepo.GetLastPoolStatsAsync(con, poolCfg.Id, ct));

        // live window
        var ring = LiveHashrateState.ForPool(poolCfg.Id);
        var diffSum = ring.SumWindow(windowSec);
        var sharesPerSec = diffSum / Math.Max(1d, windowSec);

        var poolInst = TryGetPoolInstance(poolCfg.Id);
        var unit = ResolveUnit(poolCfg.Template.Family);
        var currentHashrate = DiffToHashrate(poolCfg, diffSum, windowSec, poolInst);

        // round
        var (startedAt, roundHeight, actualShares) = LiveRoundState.Snapshot(poolCfg.Id);
        var expectedShares = persisted?.NetworkDifficulty ?? 0d;
        var luckPercent = expectedShares > 0 ? (actualShares / expectedShares) * 100.0 : 0.0;

        return new
        {
            poolId = poolCfg.Id,

            coin = MapCoinMeta(poolCfg),
            pool = MapPoolStatic(poolCfg),

            live = new
            {
                unit,
                windowSec,
                currentHashrate,
                sharesPerSec,
                minersOnline = persisted?.ConnectedMiners ?? 0
            },

            network = new
            {
                height = ToU64(persisted?.BlockHeight),
                difficulty = persisted?.NetworkDifficulty ?? 0d,
                hashrate = persisted?.NetworkHashrate ?? 0d
            },

            round = new
            {
                height = roundHeight.HasValue ? roundHeight.Value : ToU64(persisted?.BlockHeight),
                startedAt,
                actualShares,
                expectedShares,
                luckPercent
            }
        };
    }


    private async Task<object> BuildPoolSnapshotObjectAsync(PoolConfig poolCfg, int windowSec, CancellationToken ct)
    {
        var now = clock.Now;

        var persisted = await cf.Run(con =>
            statsRepo.GetLastPoolStatsAsync(con, poolCfg.Id, ct));

        var ring = LiveHashrateState.ForPool(poolCfg.Id);
        var diffSum = ring.SumWindow(windowSec);
        var sharesPerSec = diffSum / Math.Max(1d, windowSec);

        var poolInst = TryGetPoolInstance(poolCfg.Id);
        var currentHashrate = DiffToHashrate(poolCfg, diffSum, windowSec, poolInst);
        var unit = ResolveUnit(poolCfg.Template.Family);

        // Round info
        var (startedAt, roundHeight, actualShares) = LiveRoundState.Snapshot(poolCfg.Id);
        var expectedShares = persisted?.NetworkDifficulty ?? 0d;
        var luckPercent = expectedShares > 0 ? (actualShares / expectedShares) * 100.0 : 0.0;

        // Top miners (aggregated by address)
        var topMiners = LiveHashrateState.EnumeratePoolAddresses(poolCfg.Id, windowSec)
            .Select(m =>
            {
                var h = DiffToHashrate(poolCfg, m.diffSum, windowSec, poolInst);
                return new
                {
                    address = m.address,
                    hashrate = h,
                    online = IsOnlineFromLast(m.lastSeenMax, windowSec),
                        lastShareAt = m.lastSeenMax > 0
                            ? DateTimeOffset.FromUnixTimeSeconds(m.lastSeenMax).UtcDateTime
                            : (DateTime?) null
                };
            })
            .OrderByDescending(x => x.hashrate)
            .Take(25)
            .ToArray();

        return new
        {
            poolId = poolCfg.Id,
            asOf = now,
            windowSec = windowSec,
            unit = unit,
            currentHashrate = currentHashrate,
            sharesPerSec = sharesPerSec,
            minersOnline = persisted?.ConnectedMiners ?? 0,
            round = new
            {
                height = roundHeight.HasValue ? roundHeight.Value : ToU64(persisted?.BlockHeight),
                startedAt,
                actualShares = actualShares,
                expectedShares = expectedShares,
                luckPercent = luckPercent
            },
            network = new
            {
                height = ToU64(persisted?.BlockHeight),
                difficulty = persisted?.NetworkDifficulty ?? 0d,
                hashrate = persisted?.NetworkHashrate ?? 0d
            },
            topMinersNow = topMiners,
            hashrate = currentHashrate
        };
    }

    private static int? TryGetConnectedMiners(IMiningPool poolInst)
    {
        if(poolInst == null) return null;

        var statsProp = poolInst.GetType().GetProperty("Stats");
        if(statsProp != null)
        {
            var stats = statsProp.GetValue(poolInst);
            if(stats != null)
            {
                var cmProp = stats.GetType().GetProperty("ConnectedMiners");
                if(cmProp != null && cmProp.PropertyType == typeof(int))
                    return (int) cmProp.GetValue(stats);
            }
        }

        var direct = new[] { "ConnectedMiners", "MinerCount", "ConnectedClients" };
        foreach(var name in direct)
        {
            var p = poolInst.GetType().GetProperty(name);
            if(p != null && p.PropertyType == typeof(int))
                return (int) p.GetValue(poolInst);
        }

        return null;
    }


    private static (int addressesOnline, int workersOnline) CountOnlineNow(string poolId, int windowSec)
    {
        // We consider online if lastSeen within max(windowSec, OnlineGraceSec)
        var grace = Math.Max(windowSec, Live.LiveHashrateState.OnlineGraceSec);
        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

        var addrSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        int workers = 0;

        // Iterate live worker table (address.miner granularity)
        foreach(var w in Live.LiveHashrateState.EnumeratePoolWorkers(poolId))
        {
            if(w.lastSeen <= 0) continue;
            var alive = (now - w.lastSeen) <= grace;
            if(!alive) continue;

            workers++;
            if(!string.IsNullOrEmpty(w.address))
                addrSet.Add(w.address);
        }

        return (addrSet.Count, workers);
    }

    //************************* HEAVY COST *************************\\

    // ----------------------------------------------------------------
    // GET /api/live/pools/snapshot
    // ----------------------------------------------------------------
    [HttpGet("pools/snapshot")]
    public async Task<ActionResult<object>> GetAllPoolsSnapshotAsync([FromQuery] int? windowSec)
    {
        var ct = HttpContext.RequestAborted;
        var win = Math.Clamp(windowSec ?? DefaultWindowSec, MinWindowSec, MaxWindowSec);
        var now = clock.Now;

        var enabled = clusterConfig.Pools?.Where(p => p.Enabled) ?? Enumerable.Empty<PoolConfig>();
        var items = new List<object>();

        foreach(var poolCfg in enabled)
            items.Add(await BuildPoolSnapshotObjectAsync(poolCfg, win, ct));

        Response.Headers["Cache-Control"] = "no-store";
        return Ok(new { asOf = now, windowSec = win, pools = items });
    }

    // ----------------------------------------------------------------
    // GET /api/live/pools/{poolId}/snapshot
    // ----------------------------------------------------------------
    [HttpGet("pools/{poolId}/snapshot")]
    public async Task<PoolSnapshotResponse> GetPoolSnapshotAsync(string poolId,
        [FromQuery] int windowSec = DefaultWindowSec)
    {
        windowSec = Math.Clamp(windowSec, MinWindowSec, MaxWindowSec);

        var poolCfg = GetPool(poolId);
        var ct = HttpContext.RequestAborted;

        var persisted = await cf.Run(con => statsRepo.GetLastPoolStatsAsync(con, poolCfg.Id, ct));
        var unit = ResolveUnit(poolCfg.Template.Family);

        var ring = LiveHashrateState.ForPool(poolCfg.Id);
        var diffSum = ring.SumWindow(windowSec);
        var sharesPerSec = diffSum / Math.Max(1d, windowSec);

        var poolInst = TryGetPoolInstance(poolCfg.Id);
        var current = DiffToHashrate(poolCfg, diffSum, windowSec, poolInst);

        var (startedAt, roundHeight, actualShares) = LiveRoundState.Snapshot(poolCfg.Id);
        var expected = persisted?.NetworkDifficulty ?? 0d;
        var luck = expected > 0 ? (actualShares / expected) * 100.0 : 0.0;

        var resp = new PoolSnapshotResponse
        {
            PoolId = poolCfg.Id,
            AsOf = clock.Now,
            WindowSec = windowSec,
            Unit = unit,
            CurrentHashrate = current,
            SharesPerSec = sharesPerSec,
            MinersOnline = persisted?.ConnectedMiners ?? 0,
            Network = new PoolNetworkInfo
            {
                Height = ToU64(persisted?.BlockHeight),
                Difficulty = persisted?.NetworkDifficulty ?? 0,
                Hashrate = persisted?.NetworkHashrate ?? 0
            },
            Round = new PoolRoundInfo
            {
                Height = roundHeight.HasValue ? roundHeight.Value : ToU64(persisted?.BlockHeight),
                StartedAt = startedAt,
                ActualShares = actualShares,
                ExpectedShares = expected,
                LuckPercent = luck
            }
        };

        Response.Headers["Cache-Control"] = "no-store";
        return resp;
    }

    // ----------------------------------------------------------------
    // GET /api/live/pools/{poolId}/miners/{address}/snapshot
    // (aggregates all workers under this address)
    // ----------------------------------------------------------------
    [HttpGet("pools/{poolId}/miners/{address}/snapshot")]
    public ActionResult<MinerSnapshotResponse> GetMinerSnapshotAsync(
        string poolId, string address,
        [FromQuery] int windowSec = DefaultWindowSec)
    {
        windowSec = Math.Clamp(windowSec, MinWindowSec, MaxWindowSec);

        var poolCfg = GetPool(poolId);

        if(string.IsNullOrWhiteSpace(address))
            throw new ApiException("Invalid or missing miner address", HttpStatusCode.BadRequest);

        address = address.Trim();

        var unit = ResolveUnit(poolCfg.Template.Family);

        var (diffSum, lastMax) = LiveHashrateState.GetAddressWindow(poolCfg.Id, address, windowSec);
        var sharesPerSec = diffSum / Math.Max(1d, windowSec);

        var poolInst = TryGetPoolInstance(poolCfg.Id);
        var current = DiffToHashrate(poolCfg, diffSum, windowSec, poolInst);

        var online = IsOnlineFromLast(lastMax, windowSec);

        var resp = new MinerSnapshotResponse
        {
            PoolId = poolCfg.Id,
            Address = address,
            AsOf = clock.Now,
            WindowSec = windowSec,
            Unit = unit,
            Online = online,
            LastShareAt = lastMax > 0 ? DateTimeOffset.FromUnixTimeSeconds(lastMax).UtcDateTime : null,
            CurrentHashrate = current,
            SharesPerSec = sharesPerSec,

            DifficultyAssigned = 0,
            RejectPercentWindow = 0,
            StalePercentWindow = 0
        };

        Response.Headers["Cache-Control"] = "no-store";
        return resp;
    }

    // ----------------------------------------------------------------
    // GET /api/live/pools/snapinfo
    // ----------------------------------------------------------------
    [HttpGet("pools/snapinfo")]
    public async Task<ActionResult<object>> GetAllPoolsSnapInfoAsync([FromQuery] int? windowSec)
    {
        var ct = HttpContext.RequestAborted;
        var win = Math.Clamp(windowSec ?? DefaultWindowSec, MinWindowSec, MaxWindowSec);
        var now = clock.Now;

        var enabled = clusterConfig.Pools?.Where(p => p.Enabled) ?? Enumerable.Empty<PoolConfig>();
        var items = new List<object>();

        foreach(var poolCfg in enabled)
            items.Add(await BuildPoolSnapInfoObjectAsync(poolCfg, win, ct));

        Response.Headers["Cache-Control"] = "no-store";
        return Ok(new { asOf = now, windowSec = win, pools = items });
    }

    // ----------------------------------------------------------------
    // GET /api/live/pools/{poolId}/snapinfo
    // Single-pool snapinfo: mixes persisted stats + live state
    // ----------------------------------------------------------------
    [HttpGet("pools/{poolId}/snapinfo")]
    public async Task<ActionResult<object>> GetPoolSnapInfoAsync(
        string poolId,
        [FromQuery] int? windowSec)
    {
        var ct = HttpContext.RequestAborted;
        var now = clock.Now;

        // Clamp window similarly to multi-pool snapinfo
        var win = Math.Clamp(windowSec ?? DefaultWindowSec, MinWindowSec, MaxWindowSec);

        var poolCfg = GetPool(poolId);

        if(!poolCfg.Enabled)
            return NotFound(new { error = "Pool disabled", poolId });

        var payload = await BuildPoolSnapInfoObjectAsync(poolCfg, win, ct);

        Response.Headers["Cache-Control"] = "no-store";

        return Ok(new
        {
            asOf = now,
            windowSec = win,
            pool = payload
        });
    }

    // ----------------------------------------------------------------
    // GET /api/live/pools/{poolId}/round
    // ----------------------------------------------------------------
    [HttpGet("pools/{poolId}/round")]
    public async Task<ActionResult<object>> GetRoundNowAsync(string poolId)
    {
        var poolCfg = GetPool(poolId);
        var ct = HttpContext.RequestAborted;

        var persisted = await cf.Run(con => statsRepo.GetLastPoolStatsAsync(con, poolCfg.Id, ct));
        var (startedAt, height, actualShares) = LiveRoundState.Snapshot(poolCfg.Id);

        var expectedShares = persisted?.NetworkDifficulty ?? 0d;
        var luckPercent = expectedShares > 0 ? (actualShares / expectedShares) * 100.0 : 0.0;

        Response.Headers["Cache-Control"] = "no-store";
        return Ok(new
        {
            poolId = poolCfg.Id,
            startedAt,
            height,
            actualShares,
            expectedShares,
            luckPercent
        });
    }

    // ----------------------------------------------------------------
    // GET /api/live/status
    // ----------------------------------------------------------------
    [HttpGet("status")]
    public async Task<ActionResult<object>> GetClusterStatusAsync([FromQuery] int? windowSec)
    {
        var now = clock.Now;
        var pools = (clusterConfig.Pools ?? Array.Empty<PoolConfig>())
            .Where(p => p.Enabled)
            .ToArray();

        var ct = HttpContext.RequestAborted;
        var win = Math.Clamp(windowSec ?? DefaultWindowSec, MinWindowSec, MaxWindowSec);

        var summaries = await Task.WhenAll(pools.Select(async poolCfg =>
        {
            var persisted = await cf.Run(con => statsRepo.GetLastPoolStatsAsync(con, poolCfg.Id, ct));
            var unit = ResolveUnit(poolCfg.Template.Family);

            var diffSum = LiveHashrateState.ForPool(poolCfg.Id).SumWindow(win);
            var poolInst = TryGetPoolInstance(poolCfg.Id);
            var current = DiffToHashrate(poolCfg, diffSum, win, poolInst);

            return new
            {
                poolId = poolCfg.Id,
                coin = poolCfg.Template.Symbol,
                algo = poolCfg.Template.Family.ToString(),
                currentHashrate = current,
                minersOnline = persisted?.ConnectedMiners ?? 0,
                blockHeight = ToU64(persisted?.BlockHeight),
                difficulty = persisted?.NetworkDifficulty ?? 0d,
                unit,
                windowSec = win
            };
        }));

        var resultPools = summaries.Select(s => new
        {
            poolId = s.poolId,
            coin = s.coin,
            algo = s.algo,
            currentHashrate = s.currentHashrate,
            minersOnline = s.minersOnline,
            blockHeight = s.blockHeight,
            difficulty = s.difficulty,
            unit = s.unit,
            windowSec = win
        }).ToArray();

        Response.Headers["Cache-Control"] = "no-store";

        return Ok(new
        {
            asOf = now,
            pools = resultPools,
            totalPools = resultPools.Length,
            totalMiners = resultPools.Sum(p => p.minersOnline),
            totalHashrate = resultPools.Sum(p => p.currentHashrate)
        });
    }


    // ----------------------------------------------------------------
    // GET /api/live/pools/{poolId}/miners
    // Heavy: live hashrate from memory + pendingShares from DB
    // ----------------------------------------------------------------
    [HttpGet("pools/{poolId}/miners")]
    public async Task<IActionResult> GetPoolMiners(
        string poolId,
        [FromQuery] int windowSec = DefaultWindowSec,
        [FromQuery] int limit = DefaultLimit)
    {
        windowSec = Math.Clamp(windowSec, MinWindowSec, MaxWindowSec);
        limit = Math.Clamp(limit, 1, MaxLimit);

        var poolCfg = GetPool(poolId);
        var ct = HttpContext.RequestAborted;
        var unit = ResolveUnit(poolCfg.Template.Family);
        var poolInst = TryGetPoolInstance(poolCfg.Id);

        var (startedAt, _, _) = LiveRoundState.Snapshot(poolCfg.Id);
        var roundStart = startedAt;

        // Live hashrate + online state from in-memory ring buffer
        var minersNow = LiveHashrateState
            .EnumeratePoolAddresses(poolCfg.Id, windowSec)
            .Select(m =>
            {
                var sharesPerSec = m.diffSum / Math.Max(1d, windowSec);
                var hashrate = DiffToHashrate(poolCfg, m.diffSum, windowSec, poolInst);

                return new
                {
                    address = m.address,
                    hashrate,
                    sharesPerSec,
                    online = IsOnlineFromLast(m.lastSeenMax, windowSec),
                        lastShareAt = m.lastSeenMax > 0
                            ? DateTimeOffset.FromUnixTimeSeconds(m.lastSeenMax).UtcDateTime
                            : (DateTime?) null
                };
            })
            .OrderByDescending(x => x.hashrate)
            .Take(limit)
            .ToArray();

        // Enrich with pendingShares from DB (heavy but accurate)
        var items = new List<object>(minersNow.Length);

        foreach (var m in minersNow)
        {
            double pendingShares = 0;

            // Reuse the same miner stats logic used in classic API
            var stats = await cf.Run(con =>
                statsRepo.GetMinerStatsAsync(con, null, poolCfg.Id, m.address, ct));

            if (stats != null)
            {
                pendingShares = stats.PendingShares;

                // Keep parity with classic endpoint: adjust for Bitcoin share multiplier
                if (poolCfg.Template.Family == CoinFamily.Bitcoin)
                {
                    var bt = poolCfg.Template.As<BitcoinTemplate>();
                    if (bt != null && bt.ShareMultiplier > 0)
                        pendingShares *= bt.ShareMultiplier;
                }
            }

            items.Add(new
            {
                address = m.address,
                hashrate = m.hashrate,
                sharesPerSecond = m.sharesPerSec,
                pendingShares,
                online = m.online,
                lastShareAt = m.lastShareAt,
            });
        }

        return Ok(new
        {
            poolId = poolCfg.Id,
            unit,
            windowSec,
            round = new
            {
                startedAt = roundStart
            },
            items
        });
    }

    // ----------------------------------------------------------------
    // GET /api/live/pools/{poolId}/miners-all
    // Live: ALL current miners (address-level), paginated, with DB pendingShares
    // ----------------------------------------------------------------
    [HttpGet("pools/{poolId}/miners-all")]
    public async Task<IActionResult> GetPoolALLMiners(
        string poolId,
        [FromQuery] int? windowSec,
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = DefaultLimit)
    {
        // Clamp inputs
        var win = Math.Clamp(windowSec ?? DefaultWindowSec, MinWindowSec, MaxWindowSec);
        page = Math.Max(1, page);
        pageSize = Math.Clamp(pageSize, 1, MaxLimit);

        var poolCfg = GetPool(poolId);
        var ct = HttpContext.RequestAborted;
        var unit = ResolveUnit(poolCfg.Template.Family);
        var poolInst = TryGetPoolInstance(poolCfg.Id);

        // Round Snapshot (live)
        var (startedAt, _, _) = LiveRoundState.Snapshot(poolCfg.Id);
        var roundStart = startedAt;

        // All miners (exclude zombies)
        var all = LiveHashrateState
            .EnumeratePoolAddresses(poolCfg.Id, win)
            .Select(m =>
            {
                var sharesPerSec = m.diffSum / Math.Max(1d, win);
                var hashrate = DiffToHashrate(poolCfg, m.diffSum, win, poolInst);

                return new
                {
                    address = m.address,
                    hashrate,
                    sharesPerSec,
                    online = IsOnlineFromLast(m.lastSeenMax, win),
                        lastShareAt = m.lastSeenMax > 0
                            ? DateTimeOffset.FromUnixTimeSeconds(m.lastSeenMax).UtcDateTime
                            : (DateTime?) null

                };
            })
            .OrderByDescending(x => x.hashrate)
            .ToList();

        var totalItems = all.Count;
        var totalPages = (int) Math.Ceiling(totalItems / (double) pageSize);

        // Page slice
        var skip = (page - 1) * pageSize;
        var pageItems = all
            .Skip(skip)
            .Take(pageSize)
            .ToArray();

        // pendingShares from DB (Only for the current page (controlled cost))
        var result = new List<object>(pageItems.Length);

        foreach (var m in pageItems)
        {
            double pendingShares = 0;

            var stats = await cf.Run(con =>
                statsRepo.GetMinerStatsAsync(con, null, poolCfg.Id, m.address, ct));

            if (stats != null)
            {
                pendingShares = stats.PendingShares;

                if (poolCfg.Template.Family == CoinFamily.Bitcoin)
                {
                    var bt = poolCfg.Template.As<BitcoinTemplate>();
                    if (bt != null && bt.ShareMultiplier > 0)
                        pendingShares *= bt.ShareMultiplier;
                }
            }

            result.Add(new
            {
                address = m.address,
                hashrate = m.hashrate,
                sharesPerSecond = m.sharesPerSec,
                pendingShares,
                online = m.online,
                lastShareAt = m.lastShareAt
            });
        }

        Response.Headers["Cache-Control"] = "no-store";

        return Ok(new
        {
            poolId = poolCfg.Id,
            unit,
            windowSec = win,
            round = new
            {
                startedAt = roundStart
            },
            page,
            pageSize,
            totalItems,
            totalPages,
            items = result
        });
    }



    //***************************** SSE *****************************\\

    // ----------------------------------------------------------------
    // GET /api/live/pools/{poolId}/feed (SSE)
    // ----------------------------------------------------------------
    [HttpGet("pools/{poolId}/feed")]
    public async Task FeedAsync(
        string poolId,
        [FromQuery] int intervalSec = 2,
        [FromQuery] int? windowSec = null)
    {
        var poolCfg = GetPool(poolId);
        intervalSec = Math.Clamp(intervalSec, 1, 10);

        var win = Math.Clamp(windowSec ?? DefaultWindowSec, MinWindowSec, MaxWindowSec);

        Response.Headers["Cache-Control"] = "no-store";
        Response.ContentType = "text/event-stream";

        var ct = HttpContext.RequestAborted;
        var poolInst = TryGetPoolInstance(poolCfg.Id);

        while (!ct.IsCancellationRequested)
        {
            var unit = ResolveUnit(poolCfg.Template.Family);
            var diffSum = LiveHashrateState.ForPool(poolCfg.Id).SumWindow(win);
            var current = DiffToHashrate(poolCfg, diffSum, win, poolInst);

            var nowIso = DateTime.UtcNow.ToString("o");
            var payload =
                $"data: {{\"poolId\":\"{poolCfg.Id}\",\"asOf\":\"{nowIso}\",\"unit\":\"{unit}\",\"windowSec\":{win},\"currentHashrate\":{current}}}\n\n";

            await Response.WriteAsync(payload, ct);
            await Response.Body.FlushAsync(ct);
            await Task.Delay(TimeSpan.FromSeconds(intervalSec), ct);
        }
    }



    //************************** LITE COST **************************\\

    // ----------------------------------------------------------------
    // LITE STATS THAT ONLY ACCESS MEM, REDUCING DB READS
    // ----------------------------------------------------------------


    // ----------------------------------------------------------------
    // GET /api/live/miners/search-lite?q=&limit=100  (address-level)
    // ----------------------------------------------------------------
    [HttpGet("miners/search-lite")]
    public ActionResult<object> SearchMinersLite(
        [FromQuery] string q,
        [FromQuery] int limit = DefaultLimit,
        [FromQuery] int? windowSec = null)
    {
        q ??= string.Empty;

        var win = Math.Clamp(windowSec ?? DefaultWindowSec, MinWindowSec, MaxWindowSec);
        limit = Math.Clamp(limit, 1, MaxLimit);        

        var items = clusterConfig.Pools?.Where(p => p.Enabled).SelectMany(p =>
                LiveHashrateState.EnumeratePoolAddresses(p.Id, win)
                    .Where(m => m.address.Contains(q, StringComparison.OrdinalIgnoreCase))
                    .Select(m => new
                    {
                        address = m.address,
                        poolId = p.Id,
                        lastSeen = m.lastSeenMax > 0
                            ? DateTimeOffset.FromUnixTimeSeconds(m.lastSeenMax).UtcDateTime
                            : (DateTime?)null,
                        online = IsOnlineFromLast(m.lastSeenMax, win),
                        sharesPerSec = m.diffSum / Math.Max(1d, win),
                        windowSec = win
                    }))
            ?? Enumerable.Empty<object>();

        Response.Headers["Cache-Control"] = "no-store";
        return Ok(new { items = items.Take(limit) });
    }

    // ----------------------------------------------------------------
    // GET /api/live/pools/{poolId}/top-miners-lite  (address-level)
    // ----------------------------------------------------------------
    [HttpGet("pools/{poolId}/top-miners-lite")]
    public ActionResult<object> GetTopMinersNowAsyncLite(
        string poolId,
        [FromQuery] int windowSec = DefaultWindowSec,
        [FromQuery] int limit = DefaultLimit)
    {
        windowSec = Math.Clamp(windowSec, MinWindowSec, MaxWindowSec);
        limit = Math.Clamp(limit, 1, MaxLimit);

        var poolCfg = GetPool(poolId);
        var unit = ResolveUnit(poolCfg.Template.Family);

        var poolInst = TryGetPoolInstance(poolCfg.Id);

        var miners = LiveHashrateState.EnumeratePoolAddresses(poolCfg.Id, windowSec)
            .Select(m =>
            {
                var h = DiffToHashrate(poolCfg, m.diffSum, windowSec, poolInst);

                return new
                {
                    address = m.address,
                    hashrate = h,
                    online = IsOnlineFromLast(m.lastSeenMax, windowSec),
                        lastShareAt = m.lastSeenMax > 0
                            ? DateTimeOffset.FromUnixTimeSeconds(m.lastSeenMax).UtcDateTime
                            : (DateTime?) null

                };
            })
            .OrderByDescending(x => x.hashrate)
            .Take(limit)
            .ToArray();

        Response.Headers["Cache-Control"] = "no-store";
        return Ok(new { poolId = poolCfg.Id, unit, windowSec, items = miners });
    }

    // ----------------------------------------------------------------
    // GET /api/live/pools/{poolId}/miners-lite
    // ----------------------------------------------------------------
    [HttpGet("pools/{poolId}/miners-lite")]
    public IActionResult GetPoolMinersLite(
    string poolId,
    [FromQuery] int windowSec = DefaultWindowSec,
    [FromQuery] int limit = DefaultLimit)
    {
        windowSec = Math.Clamp(windowSec, MinWindowSec, MaxWindowSec);
        limit = Math.Clamp(limit, 1, MaxLimit);

        var poolCfg = GetPool(poolId);        
        var unit = ResolveUnit(poolCfg.Template.Family);
        var poolInst = TryGetPoolInstance(poolCfg.Id);

        var (startedAt, _, _) = LiveRoundState.Snapshot(poolCfg.Id);
        var roundStart = startedAt;

        var minersNow = LiveHashrateState
            .EnumeratePoolAddresses(poolCfg.Id, windowSec)
            .Select(m =>
            {
                var sharesPerSec = m.diffSum / Math.Max(1d, windowSec);
                var hashrate = DiffToHashrate(poolCfg, m.diffSum, windowSec, poolInst);

                return new
                {
                    address = m.address,
                    hashrate,
                    sharesPerSec,
                    online = IsOnlineFromLast(m.lastSeenMax, windowSec),
                        lastShareAt = m.lastSeenMax > 0
                            ? DateTimeOffset.FromUnixTimeSeconds(m.lastSeenMax).UtcDateTime
                            : (DateTime?) null

                };
            })
            .OrderByDescending(x => x.hashrate)
            .Take(limit)
            .ToArray();

        // pendingShares 
        // NOT SHOWED, NEED DB AND LITE ENDPOINTS HAVE NO DB USE

        var items = minersNow.Select(m => new
        {
            address = m.address,
            hashrate = m.hashrate,
            sharesPerSecond = m.sharesPerSec,
            online = m.online,
            lastShareAt = m.lastShareAt,
        });

        return Ok(new
        {
            poolId = poolCfg.Id,
            unit,
            windowSec,
            round = new
            {
                startedAt = roundStart
            },
            items
        });
    }

    // ----------------------------------------------------------------
    // GET /api/live/static-lite
    // ----------------------------------------------------------------
    [HttpGet("pools/static-lite")]
    public ActionResult<object> GetPoolsStaticLite()
    {
        var items = (clusterConfig.Pools ?? Array.Empty<PoolConfig>())
            .Where(p => p.Enabled)
            .Select(p => new
            {
                poolId = p.Id,
                coin = MapCoinMeta(p),
                pool = MapPoolStatic(p),
                unit = ResolveUnit(p.Template.Family)
            })
            .ToArray();

        Response.Headers["Cache-Control"] = "no-store";
        return Ok(new { items });
    }

    // ----------------------------------------------------------------
    // GET /api/live/status-lite
    // ----------------------------------------------------------------
    [HttpGet("status-lite")]
    public ActionResult<object> GetClusterStatusLite([FromQuery] int? windowSec)
    {
        var now = clock.Now;

        var win = Math.Clamp(windowSec ?? DefaultWindowSec, MinWindowSec, MaxWindowSec);

        // Only enabled pools
        var enabled = clusterConfig.Pools?.Where(p => p.Enabled) ?? Enumerable.Empty<PoolConfig>();

        var pools = enabled.Select(poolCfg =>
        {
            var unit = ResolveUnit(poolCfg.Template.Family);

            // Live hashrate via ring buffer (no DB)
            var ring = LiveHashrateState.ForPool(poolCfg.Id);
            var diffSum = ring.SumWindow(win);
            var poolInst = TryGetPoolInstance(poolCfg.Id);
            var hashrate = DiffToHashrate(poolCfg, diffSum, win, poolInst);

            // Prefer robust "online in window" (tolerant to reconnects/proxies)
            var (addressesOnline, _) = CountOnlineNow(poolCfg.Id, win);

            return new
            {
                poolId = poolCfg.Id,
                coin = poolCfg.Template.Symbol,
                algo = poolCfg.Template.Family.ToString(),
                unit,
                windowSec = win,
                currentHashrate = hashrate,
                minersOnline = addressesOnline
                // NOTE: intentionally no DB fields here (blockHeight/difficulty/networkHashrate)
            };
        }).ToArray();

        Response.Headers["Cache-Control"] = "no-store";

        // Avoid dynamic casts by projecting to concrete values in aggregation
        var totalMiners = pools.Sum(p => p.minersOnline);
        var totalHashrate = pools.Sum(p => p.currentHashrate);

        return Ok(new
        {
            asOf = now,
            pools,
            totalPools = pools.Length,
            totalMiners,
            totalHashrate
        });
    }

    // ----------------------------------------------------------------
    // GET /api/live/pools/{poolId}/online-lite?mode=window|live&windowSec=...
    // GET /api/live/pools/{poolId}/online-lite
    // ----------------------------------------------------------------
    [HttpGet("pools/{poolId}/online-lite")]
    public ActionResult<object> GetPoolOnlineWorkerLite(string poolId, [FromQuery] string mode, [FromQuery] int? windowSec)
    {
        // Resolve pool and guard against disabled pools
        var poolCfg = GetPool(poolId);
        if(!poolCfg.Enabled)
            return NotFound(new { error = "Pool disabled", poolId });


        var now = clock.Now;
        var useWindow = string.Equals(mode ?? "window", "window", StringComparison.OrdinalIgnoreCase);

        int win = Math.Clamp(windowSec ?? DefaultWindowSec, MinWindowSec, MaxWindowSec);

        int minersOnline;
        int workersOnline = 0;

        var poolInst = TryGetPoolInstance(poolCfg.Id);

        if (useWindow)
        {
            // Robust count within a time window (recommended for UI)
            var t = CountOnlineNow(poolCfg.Id, win);
            minersOnline = t.addressesOnline;
            workersOnline = t.workersOnline;
        }
        else
        {
            // Instant snapshot of connected miners (cheap but brittle)
            minersOnline = TryGetConnectedMiners(poolInst) ?? 0;
        }

        Response.Headers["Cache-Control"] = "no-store";
        return Ok(new
        {
            asOf = now,
            poolId = poolCfg.Id,
            mode = useWindow ? "window" : "live",
            windowSec = useWindow ? win : (int?)null,
            minersOnline,
            workersOnline = useWindow ? workersOnline : (int?)null
        });
    }

    // ----------------------------------------------------------------
    // GET /api/live/pools/online-lite?mode=window|live&windowSec=...
    // GET /api/live/pools/online-lite
    // ----------------------------------------------------------------
    [HttpGet("pools/online-lite")]
    public ActionResult<object> GetPoolsOnlineWorkersLite([FromQuery] string mode, [FromQuery] int? windowSec)
    {
        var now = clock.Now;
        var useWindow = string.Equals(mode ?? "window", "window", StringComparison.OrdinalIgnoreCase);
        int win = Math.Clamp(windowSec ?? DefaultWindowSec, MinWindowSec, MaxWindowSec);

        var enabled = clusterConfig.Pools?.Where(p => p.Enabled) ?? Enumerable.Empty<PoolConfig>();

        var items = enabled.Select(p =>
        {
            int miners;
            int workers = 0;

            if (useWindow)
            {
                var t = CountOnlineNow(p.Id, win);
                miners = t.addressesOnline;
                workers = t.workersOnline;
            }
            else
            {
                var inst = TryGetPoolInstance(p.Id);
                miners = TryGetConnectedMiners(inst) ?? 0;
            }

            return new
            {
                poolId = p.Id,
                unit = ResolveUnit(p.Template.Family),
                mode = useWindow ? "window" : "live",
                windowSec = useWindow ? win : (int?)null,
                minersOnline = miners,
                workersOnline = useWindow ? workers : (int?)null
            };
        }).ToArray();

        Response.Headers["Cache-Control"] = "no-store";
        return Ok(new
        {
            asOf = now,
            items,
            totalPools = items.Length,
            totalMiners = items.Sum(x => x.minersOnline)
        });
    }

    // ----------------------------------------------------------------
    // GET /api/live/pools/snapshot-lite
    // Super-cheap: all enabled pools, in-memory only, no DB.
    // ----------------------------------------------------------------
    [HttpGet("pools/snapshot-lite")]
    public ActionResult<object> GetAllPoolsSnapshotLite([FromQuery] int? windowSec)
    {
        var now = clock.Now;

        // Clamp window similarly to other lite endpoints
        var win = Math.Clamp(windowSec ?? DefaultWindowSec, MinWindowSec, MaxWindowSec);

        var enabled = clusterConfig.Pools?.Where(p => p.Enabled) ?? Enumerable.Empty<PoolConfig>();

        var items = new List<object>();
        double totalHashrate = 0;
        int totalMiners = 0;

        foreach(var poolCfg in enabled)
        {
            var unit = ResolveUnit(poolCfg.Template.Family);
            var poolInst = TryGetPoolInstance(poolCfg.Id);

            // Live hashrate via ring buffer
            var ring = LiveHashrateState.ForPool(poolCfg.Id);
            var diffSum = ring.SumWindow(win);
            var sharesPerSec = diffSum / Math.Max(1d, win);
            var currentHashrate = DiffToHashrate(poolCfg, diffSum, win, poolInst);

            // Online miners via window-based heuristic
            var t = CountOnlineNow(poolCfg.Id, win);
            var minersOnline = t.addressesOnline;

            // Live round info (no DB difficulty)
            var (startedAt, roundHeight, actualShares) = LiveRoundState.Snapshot(poolCfg.Id);

            items.Add(new
            {
                poolId = poolCfg.Id,
                unit,
                windowSec = win,

                currentHashrate,
                sharesPerSec,
                minersOnline,

                round = new
                {
                    height = roundHeight,
                    startedAt,
                    actualShares
                }
            });

            totalHashrate += currentHashrate;
            totalMiners += minersOnline;
        }

        Response.Headers["Cache-Control"] = "no-store";

        return Ok(new
        {
            asOf = now,
            windowSec = win,
            pools = items,
            totalPools = items.Count,
            totalMiners,
            totalHashrate
        });
    }

    // ----------------------------------------------------------------
    // GET /api/live/pools/{poolId}/miners/{address}/round-lite
    // ----------------------------------------------------------------
    [HttpGet("pools/{poolId}/miners/{address}/round-lite")]
    public ActionResult<object> GetMinerRoundLite(
        string poolId, string address,
        [FromQuery] int? windowSec)
    {
        var poolCfg = GetPool(poolId);
        if(string.IsNullOrWhiteSpace(address))
            throw new ApiException("Invalid or missing miner address", HttpStatusCode.BadRequest);

        address = address.Trim();

        var now = clock.Now;
        var win = Math.Clamp(windowSec ?? DefaultWindowSec, MinWindowSec, MaxWindowSec);
        var poolInst = TryGetPoolInstance(poolCfg.Id);

        // address live window
        var (diffSum, lastMax) = LiveHashrateState.GetAddressWindow(poolCfg.Id, address, win);
        var sharesPerSec = diffSum / Math.Max(1d, win);
        var currentHashrate = DiffToHashrate(poolCfg, diffSum, win, poolInst);
        var online = IsOnlineFromLast(lastMax, win);

        // Round global
        var (roundStartedAt, roundHeight, roundActualShares) = LiveRoundState.Snapshot(poolCfg.Id);

        Response.Headers["Cache-Control"] = "no-store";

        return Ok(new
        {
            asOf = now,
            poolId = poolCfg.Id,
            address,
            unit = ResolveUnit(poolCfg.Template.Family),
            windowSec = win,

            // live per-miner
            online,
            lastShareAt = lastMax > 0 ? DateTimeOffset.FromUnixTimeSeconds(lastMax).UtcDateTime : (DateTime?) null,
            currentHashrate,
            sharesPerSec,

            // global round (no DB)
            round = new
            {
                height = roundHeight,
                startedAt = roundStartedAt,
                actualShares = roundActualShares
            }
        });
    }

    // ----------------------------------------------------------------
    // GET /api/live/pools/{poolId}/snapshot-lite
    // Super-cheap: only uses in-memory live state, no DB.
    // ----------------------------------------------------------------
    [HttpGet("pools/{poolId}/snapshot-lite")]
    public ActionResult<object> GetPoolSnapshotLite(
        string poolId,
        [FromQuery] int? windowSec)
    {
        // Resolve pool (throws ApiException if not found)
        var poolCfg = GetPool(poolId);

        // Guard disabled pools
        if(!poolCfg.Enabled)
            return NotFound(new { error = "Pool disabled", poolId });

        var now = clock.Now;

        // Clamp window to a sane range
        var win = Math.Clamp(windowSec ?? DefaultWindowSec, MinWindowSec, MaxWindowSec);

        // Unit and pool instance
        var unit = ResolveUnit(poolCfg.Template.Family);
        var poolInst = TryGetPoolInstance(poolCfg.Id);

        // Live hashrate via ring buffer
        var ring = LiveHashrateState.ForPool(poolCfg.Id);
        var diffSum = ring.SumWindow(win);
        var sharesPerSec = diffSum / Math.Max(1d, win);
        var currentHashrate = DiffToHashrate(poolCfg, diffSum, win, poolInst);

        // Online miners via window-based heuristic
        var t = CountOnlineNow(poolCfg.Id, win);
        var minersOnline = t.addressesOnline;

        // Live round info (without DB difficulty)
        var (startedAt, roundHeight, actualShares) = LiveRoundState.Snapshot(poolCfg.Id);

        Response.Headers["Cache-Control"] = "no-store";

        return Ok(new
        {
            asOf = now,
            poolId = poolCfg.Id,
            unit,
            windowSec = win,

            currentHashrate,
            sharesPerSec,
            minersOnline,

            round = new
            {
                height = roundHeight,
                startedAt,
                actualShares
                // NOTE: No expectedShares / luckPercent here - would require DB difficulty
            }
        });
    }

    // ----------------------------------------------------------------
    // GET /api/live/pools/{poolId}/miners-all-lite
    // Live: ALL current miners (address-level), paginated, NO DB
    // ----------------------------------------------------------------
    [HttpGet("pools/{poolId}/miners-all-lite")]
    public IActionResult GetPoolALLMinersLite(
        string poolId,
        [FromQuery] int? windowSec,
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = DefaultLimit)
    {
        var win = Math.Clamp(windowSec ?? DefaultWindowSec, MinWindowSec, MaxWindowSec);
        page = Math.Max(1, page);
        pageSize = Math.Clamp(pageSize, 1, MaxLimit);

        var poolCfg = GetPool(poolId);
        var unit = ResolveUnit(poolCfg.Template.Family);
        var poolInst = TryGetPoolInstance(poolCfg.Id);

        var (startedAt, _, _) = LiveRoundState.Snapshot(poolCfg.Id);
        var roundStart = startedAt;

        // Enum all live miners from window
        var all = LiveHashrateState
            .EnumeratePoolAddresses(poolCfg.Id, win)
            .Select(m =>
            {
                var sharesPerSec = m.diffSum / Math.Max(1d, win);
                var hashrate = DiffToHashrate(poolCfg, m.diffSum, win, poolInst);

                return new
                {
                    address = m.address,
                    hashrate,
                    sharesPerSec,
                    online = IsOnlineFromLast(m.lastSeenMax, win),
                        lastShareAt = m.lastSeenMax > 0
                            ? DateTimeOffset.FromUnixTimeSeconds(m.lastSeenMax).UtcDateTime
                            : (DateTime?) null

                };
            })
            .OrderByDescending(x => x.hashrate)
            .ToList();

        var totalItems = all.Count;
        var totalPages = (int) Math.Ceiling(totalItems / (double) pageSize);

        var skip = (page - 1) * pageSize;
        var pageItems = all
            .Skip(skip)
            .Take(pageSize)
            .ToArray();

        Response.Headers["Cache-Control"] = "no-store";

        return Ok(new
        {
            poolId = poolCfg.Id,
            unit,
            windowSec = win,
            round = new
            {
                startedAt = roundStart
            },
            page,
            pageSize,
            totalItems,
            totalPages,
            items = pageItems
        });
    }

}
