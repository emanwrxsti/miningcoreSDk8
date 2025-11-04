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
using Miningcore.Mining; // IMiningPool + MiningPoolRegistry
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
    private readonly MiningPoolRegistry poolRegistry;

    public LiveController(
    ClusterConfig clusterConfig,
    IConnectionFactory cf,
    IStatsRepository statsRepo,
    IMasterClock clock,
    IMapper mapper,
    MiningPoolRegistry poolRegistry)
    {
        this.clusterConfig = clusterConfig;
        this.cf = cf;
        this.statsRepo = statsRepo;
        this.clock = clock;
        this.mapper = mapper;
        this.poolRegistry = poolRegistry;
    }

    // ---------- Helpers ----------
    private static bool IsEquihash(CoinFamily family) =>
        family == CoinFamily.Equihash ||
        family.ToString().Contains("Equihash", StringComparison.OrdinalIgnoreCase);

    private const double Diff1Hash = 4294967296d; // 2^32

    private static string ResolveUnit(CoinFamily family) =>
        IsEquihash(family) ? "Sol/s" : "H/s";

    private PoolConfig GetPool(string poolId)
    {
        var pool = clusterConfig.Pools?.FirstOrDefault(x =>
            string.Equals(x.Id, poolId, StringComparison.OrdinalIgnoreCase));

        if(pool == null)
            throw new ApiException($"Pool '{poolId}' not found", HttpStatusCode.NotFound);

        return pool;
    }

    private IMiningPool TryGetPoolInstance(string poolId)
    {
        return poolRegistry.Get(poolId);
    }

    // ----------------------------------------------------------------
    // GET /api/live/pools/{poolId}/snapshot
    // ----------------------------------------------------------------
    [HttpGet("pools/{poolId}/snapshot")]
    public async Task<PoolSnapshotResponse> GetPoolSnapshotAsync(
        string poolId,
        [FromQuery] int windowSec = LiveHashrateState.DefaultWindowSec)
    {
        var poolCfg = GetPool(poolId);
        var ct = HttpContext.RequestAborted;

        var persisted = await cf.Run(con => statsRepo.GetLastPoolStatsAsync(con, poolCfg.Id, ct));
        var unit = ResolveUnit(poolCfg.Template.Family);

        var ring = LiveHashrateState.ForPool(poolCfg.Id);
        var diffSum = ring.SumWindow(windowSec);
        var sharesPerSec = diffSum / Math.Max(1d, windowSec);

        // Use exact pool conversion when available; fallback keeps endpoint alive during early startup
        var poolInst = TryGetPoolInstance(poolCfg.Id);
        double current;

        if(poolInst != null)
            current = poolInst.HashrateFromShares(diffSum, windowSec);
        else
        {
            if(IsEquihash(poolCfg.Template.Family))
                current = sharesPerSec;
            else
                current = sharesPerSec * Diff1Hash;
        }


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
                Height = (ulong) (persisted?.BlockHeight ?? 0),
                Difficulty = persisted?.NetworkDifficulty ?? 0,
                Hashrate = persisted?.NetworkHashrate ?? 0
            },
            Round = new PoolRoundInfo
            {
                Height = (ulong) (persisted?.BlockHeight ?? 0),
                StartedAt = null,
                ActualShares = 0,
                ExpectedShares = 0,
                LuckPercent = 0
            }
        };

        Response.Headers["Cache-Control"] = "no-store";
        return resp;
    }

    // ----------------------------------------------------------------
    // GET /api/live/pools/{poolId}/miners/{address}/snapshot
    // ----------------------------------------------------------------
    [HttpGet("pools/{poolId}/miners/{address}/snapshot")]
    public ActionResult<MinerSnapshotResponse> GetMinerSnapshotAsync(
        string poolId,
        string address,
        [FromQuery] int windowSec = LiveHashrateState.DefaultWindowSec)
    {
        var poolCfg = GetPool(poolId);

        if(string.IsNullOrWhiteSpace(address))
            throw new ApiException("Invalid or missing miner address", HttpStatusCode.BadRequest);

        address = address.Trim();

        var unit = ResolveUnit(poolCfg.Template.Family);

        var ring = LiveHashrateState.ForMiner(poolCfg.Id, address);
        var diffSum = ring.SumWindow(windowSec);
        var sharesPerSec = diffSum / Math.Max(1d, windowSec);

        var poolInst = TryGetPoolInstance(poolCfg.Id);
        double current;

        if(poolInst != null)
            current = poolInst.HashrateFromShares(diffSum, windowSec);
        else
        {
            if(IsEquihash(poolCfg.Template.Family))
                current = sharesPerSec;
            else
                current = sharesPerSec * Diff1Hash;
        }


        var online = LiveHashrateState.IsOnline(poolCfg.Id, address);
        var lastSec = LiveHashrateState.GetMinerLastSeenSec(poolCfg.Id, address);

        var resp = new MinerSnapshotResponse
        {
            PoolId = poolCfg.Id,
            Address = address,
            AsOf = clock.Now,
            WindowSec = windowSec,
            Unit = unit,
            Online = online,
            LastShareAt = lastSec.HasValue ? DateTimeOffset.FromUnixTimeSeconds(lastSec.Value).UtcDateTime : null,
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
    // GET /api/live/miners/search?q=&limit=20
    // ----------------------------------------------------------------
    [HttpGet("miners/search")]
    public ActionResult<object> SearchMiners([FromQuery] string q, [FromQuery] int limit = 20)
    {
        q ??= string.Empty;
        limit = Math.Clamp(limit, 1, 100);

        var items = LiveHashrateState
            .EnumerateAllMiners()
            .Where(kv => kv.address.Contains(q, StringComparison.OrdinalIgnoreCase))
            .OrderByDescending(kv => kv.lastSeen)
            .Take(limit)
            .Select(kv =>
            {
                var perSec = kv.ring.SumWindow(LiveHashrateState.DefaultWindowSec) /
                             Math.Max(1d, LiveHashrateState.DefaultWindowSec);

                // We expose neutral shares/s in search to keep it simple
                return new
                {
                    address = kv.address,
                    poolId = kv.poolId,
                    lastSeen = kv.lastSeen > 0
                        ? DateTimeOffset.FromUnixTimeSeconds(kv.lastSeen).UtcDateTime
                        : (DateTime?) null,
                    online = LiveHashrateState.IsOnline(kv.poolId, kv.address),
                    sharesPerSec = perSec,
                    windowSec = LiveHashrateState.DefaultWindowSec
                };
            });

        Response.Headers["Cache-Control"] = "no-store";
        return Ok(new { items });
    }

    // ----------------------------------------------------------------
    // GET /api/live/pools/{poolId}/top-miners
    // ----------------------------------------------------------------
    [HttpGet("pools/{poolId}/top-miners")]
    public ActionResult<object> GetTopMinersNowAsync(
        string poolId,
        [FromQuery] int windowSec = LiveHashrateState.DefaultWindowSec,
        [FromQuery] int limit = 50)
    {
        var poolCfg = GetPool(poolId);
        limit = Math.Clamp(limit, 1, 200);
        var unit = ResolveUnit(poolCfg.Template.Family);

        var poolInst = TryGetPoolInstance(poolCfg.Id);

        var miners = LiveHashrateState.EnumeratePoolMiners(poolCfg.Id)
            .Select(m =>
            {
                var diffSum = m.ring.SumWindow(windowSec);
                var perSec = diffSum / Math.Max(1d, windowSec);

                double h;

                if(poolInst != null)
                    h = poolInst.HashrateFromShares(diffSum, windowSec);
                else
                {
                    if(IsEquihash(poolCfg.Template.Family))
                        h = perSec;
                    else
                        h = perSec * Diff1Hash;
                }


                return new
                {
                    address = m.address,
                    hashrate = h,
                    online = LiveHashrateState.IsOnline(poolCfg.Id, m.address),
                    lastShareAt = m.lastSeen > 0
                        ? DateTimeOffset.FromUnixTimeSeconds(m.lastSeen).UtcDateTime
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
    // GET /api/live/pools/{poolId}/round
    // ----------------------------------------------------------------
    [HttpGet("pools/{poolId}/round")]
    public async Task<ActionResult<object>> GetRoundNowAsync(string poolId)
    {
        var poolCfg = GetPool(poolId);
        var ct = HttpContext.RequestAborted;

        var persisted = await cf.Run(con => statsRepo.GetLastPoolStatsAsync(con, poolCfg.Id, ct));
        var networkDiff = persisted?.NetworkDifficulty ?? 0d;

        var (startedAt, height, actualShares) = LiveRoundState.Snapshot(poolCfg.Id);

        var expectedShares = networkDiff; // Diff1
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
            var perSec = diffSum / Math.Max(1d, LiveHashrateState.DefaultWindowSec);

            double current;

            if(poolInst != null)
                current = poolInst.HashrateFromShares(diffSum, LiveHashrateState.DefaultWindowSec);
            else
            {
                if(IsEquihash(poolCfg.Template.Family))
                    current = perSec;
                else
                    current = perSec * Diff1Hash;
            }


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
            var perSec = diffSum / Math.Max(1d, windowSec);

            var poolInst = TryGetPoolInstance(poolCfg.Id);

            double current;

            if(poolInst != null)
                current = poolInst.HashrateFromShares(diffSum, windowSec);
            else
            {
                if(IsEquihash(poolCfg.Template.Family))
                    current = perSec;
                else
                    current = perSec * Diff1Hash;
            }


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
}
