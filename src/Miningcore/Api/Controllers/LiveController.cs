// miningcore/src/Miningcore/Api/Controllers/LiveController.cs
using System.Net;
using AutoMapper;
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
    private readonly IMapper mapper;
    private readonly IShareRepository shareRepo;
    private readonly MiningPoolRegistry poolRegistry;

    public LiveController(
        ClusterConfig clusterConfig,
        IConnectionFactory cf,
        IStatsRepository statsRepo,
        IMasterClock clock,
        IMapper mapper,
        MiningPoolRegistry poolRegistry,
        IShareRepository shareRepo)
    {
        this.clusterConfig = clusterConfig;
        this.cf = cf;
        this.statsRepo = statsRepo;
        this.clock = clock;
        this.mapper = mapper;
        this.poolRegistry = poolRegistry;
        this.shareRepo = shareRepo;
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
                    online = LiveHashrateState.IsAddressOnline(poolCfg.Id, m.address, windowSec),
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

    // ----------------------------------------------------------------
    // GET /api/live/pools/snapshot
    // ----------------------------------------------------------------
    [HttpGet("pools/snapshot")]
    public async Task<ActionResult<object>> GetAllPoolsSnapshotAsync([FromQuery] int? windowSec)
    {
        var ct = HttpContext.RequestAborted;
        var win = Math.Clamp(windowSec ?? LiveHashrateState.DefaultWindowSec, 10, 1800);
        var now = clock.Now;

        var enabled = clusterConfig.Pools?.Where(p => p.Enabled) ?? Enumerable.Empty<PoolConfig>();
        var items = new List<object>();

        foreach(var poolCfg in enabled)
            items.Add(await BuildPoolSnapshotObjectAsync(poolCfg, win, ct));

        Response.Headers["Cache-Control"] = "no-store";
        return Ok(new { asOf = now, windowSec = win, pools = items });
    }



    // ----------------------------------------------------------------
    // GET /api/live/pools/snapinfo
    // ----------------------------------------------------------------
    [HttpGet("pools/snapinfo")]
    public async Task<ActionResult<object>> GetAllPoolsSnapInfoAsync([FromQuery] int? windowSec)
    {
        var ct = HttpContext.RequestAborted;
        var win = Math.Clamp(windowSec ?? LiveHashrateState.DefaultWindowSec, 10, 1800);
        var now = clock.Now;

        var enabled = clusterConfig.Pools?.Where(p => p.Enabled) ?? Enumerable.Empty<PoolConfig>();
        var items = new List<object>();

        foreach(var poolCfg in enabled)
            items.Add(await BuildPoolSnapInfoObjectAsync(poolCfg, win, ct));

        Response.Headers["Cache-Control"] = "no-store";
        return Ok(new { asOf = now, windowSec = win, pools = items });
    }


    // ----------------------------------------------------------------
    // GET /api/live/pools/{poolId}/snapshot
    // ----------------------------------------------------------------
    [HttpGet("pools/{poolId}/snapshot")]
    public async Task<PoolSnapshotResponse> GetPoolSnapshotAsync(string poolId,
        [FromQuery] int windowSec = LiveHashrateState.DefaultWindowSec)
    {
        windowSec = Math.Clamp(windowSec, 10, 1800);

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
        [FromQuery] int windowSec = LiveHashrateState.DefaultWindowSec)
    {
        windowSec = Math.Clamp(windowSec, 10, 1800);

        var poolCfg = GetPool(poolId);

        if(string.IsNullOrWhiteSpace(address))
            throw new ApiException("Invalid or missing miner address", HttpStatusCode.BadRequest);

        address = address.Trim();

        var unit = ResolveUnit(poolCfg.Template.Family);

        var (diffSum, lastMax) = LiveHashrateState.GetAddressWindow(poolCfg.Id, address, windowSec);
        var sharesPerSec = diffSum / Math.Max(1d, windowSec);

        var poolInst = TryGetPoolInstance(poolCfg.Id);
        var current = DiffToHashrate(poolCfg, diffSum, windowSec, poolInst);

        var online = LiveHashrateState.IsAddressOnline(poolCfg.Id, address, windowSec);

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
    // GET /api/live/miners/search?q=&limit=20  (address-level)
    // ----------------------------------------------------------------
    [HttpGet("miners/search")]
    public ActionResult<object> SearchMiners([FromQuery] string q, [FromQuery] int limit = 20)
    {
        q ??= string.Empty;
        limit = Math.Clamp(limit, 1, 100);

        // naive: window = default
        var windowSec = LiveHashrateState.DefaultWindowSec;

        var items = clusterConfig.Pools?.Where(p => p.Enabled).SelectMany(p =>
                LiveHashrateState.EnumeratePoolAddresses(p.Id, windowSec)
                    .Where(m => m.address.Contains(q, StringComparison.OrdinalIgnoreCase))
                    .Select(m => new
                    {
                        address = m.address,
                        poolId = p.Id,
                        lastSeen = m.lastSeenMax > 0 ? DateTimeOffset.FromUnixTimeSeconds(m.lastSeenMax).UtcDateTime : (DateTime?) null,
                        online = LiveHashrateState.IsAddressOnline(p.Id, m.address, windowSec),
                        sharesPerSec = m.diffSum / Math.Max(1d, windowSec),
                        windowSec
                    }))
            ?? Enumerable.Empty<object>();

        Response.Headers["Cache-Control"] = "no-store";
        return Ok(new { items = items.Take(limit) });
    }

    // ----------------------------------------------------------------
    // GET /api/live/pools/{poolId}/top-miners  (address-level)
    // ----------------------------------------------------------------
    [HttpGet("pools/{poolId}/top-miners")]
    public ActionResult<object> GetTopMinersNowAsync(
        string poolId,
        [FromQuery] int windowSec = LiveHashrateState.DefaultWindowSec,
        [FromQuery] int limit = 50)
    {
        windowSec = Math.Clamp(windowSec, 10, 1800);

        var poolCfg = GetPool(poolId);
        limit = Math.Clamp(limit, 1, 200);
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
                    online = LiveHashrateState.IsAddressOnline(poolCfg.Id, m.address, windowSec),
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
    // GET /api/live/pools/{poolId}/miners
    // ----------------------------------------------------------------
    [HttpGet("pools/{poolId}/miners")]
    public IActionResult GetPoolMiners(
    string poolId,
    [FromQuery] int windowSec = LiveHashrateState.DefaultWindowSec,
    [FromQuery] int limit = 500)
    {
        windowSec = Math.Clamp(windowSec, 10, 1800);

        var poolCfg = GetPool(poolId);
        var ct = HttpContext.RequestAborted;
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
                    online = LiveHashrateState.IsAddressOnline(poolCfg.Id, m.address, windowSec),
                    lastShareAt = m.lastSeenMax > 0
                        ? DateTimeOffset.FromUnixTimeSeconds(m.lastSeenMax).UtcDateTime
                        : (DateTime?) null
                };
            })
            .OrderByDescending(x => x.hashrate)
            .Take(Math.Clamp(limit, 1, 5000))
            .ToArray();

        // pendingShares 
        //TODO 

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
    // GET /api/live/pools/{poolId}/feed (SSE)
    // ----------------------------------------------------------------
    [HttpGet("pools/{poolId}/feed")]
    public async Task FeedAsync(string poolId, [FromQuery] int intervalSec = 2)
    {
        var poolCfg = GetPool(poolId);
        intervalSec = Math.Clamp(intervalSec, 1, 10);

        Response.Headers["Cache-Control"] = "no-store";
        Response.ContentType = "text/event-stream";

        var ct = HttpContext.RequestAborted;
        var poolInst = TryGetPoolInstance(poolCfg.Id);

        while(!ct.IsCancellationRequested)
        {
            var unit = ResolveUnit(poolCfg.Template.Family);
            var diffSum = LiveHashrateState.ForPool(poolCfg.Id).SumWindow(LiveHashrateState.DefaultWindowSec);
            var current = DiffToHashrate(poolCfg, diffSum, LiveHashrateState.DefaultWindowSec, poolInst);

            var nowIso = DateTime.UtcNow.ToString("o");
            var payload =
                $"data: {{\"poolId\":\"{poolCfg.Id}\",\"asOf\":\"{nowIso}\",\"unit\":\"{unit}\",\"windowSec\":{LiveHashrateState.DefaultWindowSec},\"currentHashrate\":{current}}}\n\n";

            await Response.WriteAsync(payload, ct);
            await Response.Body.FlushAsync(ct);
            await Task.Delay(TimeSpan.FromSeconds(intervalSec), ct);
        }
    }

    // ----------------------------------------------------------------
    // GET /api/live/status
    // ----------------------------------------------------------------
    [HttpGet("status")]
    public async Task<ActionResult<object>> GetClusterStatusAsync()
    {
        var now = clock.Now;
        var pools = clusterConfig.Pools ?? Array.Empty<PoolConfig>();
        var ct = HttpContext.RequestAborted;

        var summaries = new List<object>();
        var windowSec = LiveHashrateState.DefaultWindowSec;

        foreach(var poolCfg in pools)
        {
            var persisted = await cf.Run(con => statsRepo.GetLastPoolStatsAsync(con, poolCfg.Id, ct));
            var unit = ResolveUnit(poolCfg.Template.Family);

            var diffSum = LiveHashrateState.ForPool(poolCfg.Id).SumWindow(windowSec);
            var poolInst = TryGetPoolInstance(poolCfg.Id);
            var current = DiffToHashrate(poolCfg, diffSum, windowSec, poolInst);

            summaries.Add(new
            {
                poolId = poolCfg.Id,
                coin = poolCfg.Template.Symbol,
                algo = poolCfg.Template.Family.ToString(),
                currentHashrate = current,
                minersOnline = persisted?.ConnectedMiners ?? 0,
                blockHeight = persisted?.BlockHeight ?? 0,
                difficulty = persisted?.NetworkDifficulty ?? 0,
                unit,
                windowSec
            });
        }

        Response.Headers["Cache-Control"] = "no-store";

        return Ok(new
        {
            asOf = now,
            pools = summaries,
            totalPools = summaries.Count,
            totalMiners = summaries.Sum(x => (int) ((dynamic) x).minersOnline),
            totalHashrate = summaries.Sum(x => (double) ((dynamic) x).currentHashrate)
        });
    }

    // ----------------------------------------------------------------
    // LITE STATS THAT ONLY ACCESS MEM, REDUCING DB READS
    // ----------------------------------------------------------------

    // ----------------------------------------------------------------
    // GET /api/live/static-lite
    // ----------------------------------------------------------------
    [HttpGet("pools/static-lite")]
    public ActionResult<object> GetPoolsStatic()
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

        var win = Math.Clamp(windowSec ?? LiveHashrateState.DefaultWindowSec, 10, 1800);

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
    public ActionResult<object> GetPoolOnlineWorker(string poolId, [FromQuery] string mode, [FromQuery] int? windowSec)
    {
        // Resolve pool and guard against disabled pools
        var poolCfg = GetPool(poolId);
        if (poolCfg == null || !poolCfg.Enabled)
            return NotFound(new { error = "Pool not found or disabled", poolId });

        var now = clock.Now;
        var useWindow = string.Equals(mode ?? "window", "window", StringComparison.OrdinalIgnoreCase);

        int win = Math.Clamp(windowSec ?? LiveHashrateState.DefaultWindowSec, 10, 1800);

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
    public ActionResult<object> GetPoolsOnlineWorkers([FromQuery] string mode, [FromQuery] int? windowSec)
    {
        var now = clock.Now;
        var useWindow = string.Equals(mode ?? "window", "window", StringComparison.OrdinalIgnoreCase);
        int win = Math.Clamp(windowSec ?? LiveHashrateState.DefaultWindowSec, 10, 1800);

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
        var win = Math.Clamp(windowSec ?? LiveHashrateState.DefaultWindowSec, 10, 1800);

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

}
