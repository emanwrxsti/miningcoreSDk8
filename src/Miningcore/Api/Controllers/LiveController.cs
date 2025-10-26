// Api/Controllers/LiveController.cs
using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Http; // required for Response.WriteAsync
using AutoMapper;
using Miningcore.Api.Responses.Live;
using Miningcore.Configuration;
using Miningcore.Live;
using Miningcore.Persistence;
using Miningcore.Persistence.Repositories;
using Miningcore.Time;
using Miningcore.Extensions;


namespace Miningcore.Api.Controllers;

/// <summary>
/// Live endpoints built on top of in-memory rolling counters.
/// IMPORTANT: The ring window is fixed in LiveHashrateState; per-request window is honored via SumWindow(windowSec).
/// </summary>
[ApiController]
[Route("api/live")]
public class LiveController : ControllerBase
{
    private readonly ClusterConfig clusterConfig;
    private readonly IConnectionFactory cf;
    private readonly IStatsRepository statsRepo;
    private readonly IMasterClock clock;
    private readonly IMapper mapper;

    public LiveController(
        ClusterConfig clusterConfig,
        IConnectionFactory cf,
        IStatsRepository statsRepo,
        IMasterClock clock,
        IMapper mapper)
    {
        this.clusterConfig = clusterConfig;
        this.cf = cf;
        this.statsRepo = statsRepo;
        this.clock = clock;
        this.mapper = mapper;
    }

    /// <summary>
    /// Resolve pool config or throw a 404 API exception.
    /// </summary>
    private PoolConfig GetPool(string poolId)
    {
        var pool = clusterConfig.Pools?.FirstOrDefault(x =>
            string.Equals(x.Id, poolId, StringComparison.OrdinalIgnoreCase));

        if(pool == null)
            throw new ApiException($"Pool '{poolId}' not found", HttpStatusCode.NotFound);

        return pool;
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

        // Reference stats from persistence (for net diff, height, miners, etc.)
        var persisted = await cf.Run(con => statsRepo.GetLastPoolStatsAsync(con, poolCfg.Id, ct));

        // Preferred unit for this coin family
        var unit = LiveCommon.ResolveUnit(poolCfg.Template.Family);

        // Live counters: Sum over the requested window using the fixed ring
        var ring = LiveHashrateState.ForPool(poolCfg.Id);
        var shares = ring.SumWindow(windowSec);
        var sharesPerSec = shares / (double) Math.Max(1, windowSec);

        // Use network difficulty as an approximation for "share difficulty"
        var shareDiff = persisted?.NetworkDifficulty > 0 ? persisted.NetworkDifficulty : 1d;
        var currentHps = LiveCommon.HpsFromShares(sharesPerSec, shareDiff);

        var resp = new PoolSnapshotResponse
        {
            PoolId = poolCfg.Id,
            AsOf = clock.Now,
            WindowSec = windowSec,
            Unit = unit,
            CurrentHashrate = currentHps,
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
    public async Task<MinerSnapshotResponse> GetMinerSnapshotAsync(
        string poolId,
        string address,
        [FromQuery] int windowSec = LiveHashrateState.DefaultWindowSec)
    {
        var poolCfg = GetPool(poolId);
        var ct = HttpContext.RequestAborted;

        if(string.IsNullOrWhiteSpace(address))
            throw new ApiException("Invalid or missing miner address", HttpStatusCode.BadRequest);

        // Normalize to improve deduplication and UX
        address = LiveCommon.NormalizeMinerAddress(poolCfg.Template.Family, address);

        // Pull light persistent stats for difficulty hint
        var persisted = await cf.Run(con => statsRepo.GetLastPoolStatsAsync(con, poolCfg.Id, ct));
        var unit = LiveCommon.ResolveUnit(poolCfg.Template.Family);

        // Live counters for the miner
        var ring = LiveHashrateState.ForMiner(poolCfg.Id, address);
        var shares = ring.SumWindow(windowSec);
        var sharesPerSec = shares / (double) Math.Max(1, windowSec);

        var shareDiff = persisted?.NetworkDifficulty > 0 ? persisted.NetworkDifficulty : 1d;
        var currentHps = LiveCommon.HpsFromShares(sharesPerSec, shareDiff);

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
            CurrentHashrate = currentHps,
            SharesPerSec = sharesPerSec,
            DifficultyAssigned = 0,
            RejectPercentWindow = 0,
            StalePercentWindow = 0
        };

        Response.Headers["Cache-Control"] = "no-store";
        return resp;
    }

    // ----------------------------------------------------------------
    // GET /api/live/miners/search?q=&limit=20  (global search)
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
                var poolId = kv.poolId;
                var addr = kv.address;
                var online = LiveHashrateState.IsOnline(poolId, addr);
                var shares = kv.ring.SumWindow(LiveHashrateState.DefaultWindowSec);
                var sharesPerSec = shares / (double) LiveHashrateState.DefaultWindowSec;

                return new
                {
                    address = addr,
                    poolId,
                    lastSeen = kv.lastSeen > 0 ? DateTimeOffset.FromUnixTimeSeconds(kv.lastSeen).UtcDateTime : (DateTime?) null,
                    online,
                    currentHashrate = LiveCommon.HpsFromShares(sharesPerSec, 1d), // neutral diff; UI should treat as relative
                    windowSec = LiveHashrateState.DefaultWindowSec
                };
            });

        Response.Headers["Cache-Control"] = "no-store";
        return Ok(new { items });
    }

    // ----------------------------------------------------------------
    // GET /api/live/pools/{poolId}/top-miners?windowSec=600&limit=50
    // ----------------------------------------------------------------
    [HttpGet("pools/{poolId}/top-miners")]
    public async Task<ActionResult<object>> GetTopMinersNowAsync(
        string poolId,
        [FromQuery] int windowSec = LiveHashrateState.DefaultWindowSec,
        [FromQuery] int limit = 50)
    {
        var poolCfg = GetPool(poolId);
        var ct = HttpContext.RequestAborted;

        limit = Math.Clamp(limit, 1, 200);

        // Difficulty hint from persistence
        var persisted = await cf.Run(con => statsRepo.GetLastPoolStatsAsync(con, poolCfg.Id, ct));
        var diff = persisted?.NetworkDifficulty > 0 ? persisted.NetworkDifficulty : 1d;
        var unit = LiveCommon.ResolveUnit(poolCfg.Template.Family);

        var miners = LiveHashrateState.EnumeratePoolMiners(poolCfg.Id)
            .Select(m =>
            {
                var shares = m.ring.SumWindow(windowSec);
                var sps = Math.Max(0.0, shares / (double) windowSec);
                var hps = LiveCommon.HpsFromShares(sps, diff);
                return new
                {
                    address = m.address,
                    hashrate = hps,
                    online = LiveHashrateState.IsOnline(poolCfg.Id, m.address),
                    lastShareAt = m.lastSeen > 0 ? DateTimeOffset.FromUnixTimeSeconds(m.lastSeen).UtcDateTime : (DateTime?) null
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

        // LiveRoundState is assumed to exist in your codebase.
        // It should snapshot current round progress (actual shares, current height, startedAt).
        var (startedAt, height, actualShares) = LiveRoundState.Snapshot(poolCfg.Id);

        // Expected shares ~ network difficulty (approximation at Diff1)
        var expectedShares = networkDiff;
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
    // GET /api/live/pools/{poolId}/feed   (Server-Sent Events)
    // ----------------------------------------------------------------
    [HttpGet("pools/{poolId}/feed")]
    public async Task FeedAsync(string poolId, [FromQuery] int intervalSec = 2)
    {
        var poolCfg = GetPool(poolId);
        intervalSec = Math.Clamp(intervalSec, 1, 10);

        Response.Headers["Cache-Control"] = "no-store";
        Response.ContentType = "text/event-stream";

        var ct = HttpContext.RequestAborted;

        while(!ct.IsCancellationRequested)
        {
            // Compute using a stable window for SSE (keeps bandwidth predictable)
            var ring = LiveHashrateState.ForPool(poolCfg.Id);
            var shares = ring.SumWindow(LiveHashrateState.DefaultWindowSec);
            var sps = shares / (double) LiveHashrateState.DefaultWindowSec;

            var persisted = await cf.Run(con => statsRepo.GetLastPoolStatsAsync(con, poolCfg.Id, ct));
            var diff = persisted?.NetworkDifficulty > 0 ? persisted.NetworkDifficulty : 1d;
            var hps = LiveCommon.HpsFromShares(sps, diff);

            var nowIso = DateTime.UtcNow.ToString("o");
            var payload = $"data: {{\"poolId\":\"{poolCfg.Id}\",\"asOf\":\"{nowIso}\",\"windowSec\":{LiveHashrateState.DefaultWindowSec},\"currentHashrate\":{hps} }}\n\n";
            await Response.WriteAsync(payload, ct);
            await Response.Body.FlushAsync(ct);

            await Task.Delay(TimeSpan.FromSeconds(intervalSec), ct);
        }
    }

    // ----------------------------------------------------------------
    // GET /api/live/status  (cluster landing summary)
    // ----------------------------------------------------------------
    [HttpGet("status")]
    public async Task<ActionResult<object>> GetClusterStatusAsync()
    {
        var now = clock.Now;
        var pools = clusterConfig.Pools ?? Array.Empty<PoolConfig>();
        var ct = HttpContext.RequestAborted;

        var summaries = new System.Collections.Generic.List<object>();

        foreach(var pool in pools)
        {
            var persisted = await cf.Run(con => statsRepo.GetLastPoolStatsAsync(con, pool.Id, ct));
            var diff = persisted?.NetworkDifficulty > 0 ? persisted.NetworkDifficulty : 1d;
            var netHeight = persisted?.BlockHeight ?? 0;

            var ring = LiveHashrateState.ForPool(pool.Id);
            var shares = ring.SumWindow(LiveHashrateState.DefaultWindowSec);
            var sps = shares / (double) LiveHashrateState.DefaultWindowSec;
            var currentHps = LiveCommon.HpsFromShares(sps, diff);

            var minersOnline = persisted?.ConnectedMiners ?? 0;

            summaries.Add(new
            {
                poolId = pool.Id,
                coin = pool.Template.Symbol,
                algo = pool.Template.Family.ToString(),
                currentHashrate = currentHps,
                minersOnline,
                blockHeight = netHeight,
                difficulty = diff,
                unit = LiveCommon.ResolveUnit(pool.Template.Family),
                windowSec = LiveHashrateState.DefaultWindowSec
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
