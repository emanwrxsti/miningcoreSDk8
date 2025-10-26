// Api/Live/LiveHashrateState.cs
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace Miningcore.Live;

/// <summary>
/// Lock-free(ish) in-memory rolling counters for live hashrate.
/// Design:
/// - Time window = 600 s (10 minutes), split into 60 buckets of 10 s each.
/// - Each pool and (pool, miner) keeps a RollingRing accumulating "share events".
/// - Reads are O(1) for the full-window sum; O(k) for sub-window sums via SumWindow().
/// - Sharding spreads contention under high concurrency (e.g., many miners).
/// </summary>
public static class LiveHashrateState
{
    // --- Window configuration (match controllers' defaults)
    public const int DefaultWindowSec = 600;   // 10 minutes
    public const int BucketSec        = 10;    // 10-second buckets
    public const int Buckets          = DefaultWindowSec / BucketSec; // 60
    public const int OnlineGraceSec   = 120;   // mark online if last-seen within this

    /// <summary>
    /// Rolling ring buffer of "share counts" over the live window.
    /// Each bucket holds the number of shares observed during that 10 s slice.
    /// </summary>
    public sealed class RollingRing
    {
        private readonly int[] buckets = new int[Buckets];
        private int lastBucketIndex;              // index of the current bucket (0..59)
        private long lastBucketEpoch10s;          // epoch seconds / 10
        private long total;                       // running sum across all buckets

        public RollingRing()
        {
            var now10 = Now10();
            lastBucketEpoch10s = now10;
            lastBucketIndex = (int)(now10 % Buckets);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static long Now10() => DateTimeOffset.UtcNow.ToUnixTimeSeconds() / BucketSec;

        /// <summary>
        /// Advance the ring to "now", zeroing out any buckets that rolled over.
        /// Also adjusts the running total accordingly.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AdvanceIfNeeded(long now10)
        {
            var last = Volatile.Read(ref lastBucketEpoch10s);
            if (now10 == last) return;

            var steps = (int)Math.Min(Buckets, Math.Max(0, now10 - last));
            var idx = Volatile.Read(ref lastBucketIndex);

            for (var i = 0; i < steps; i++)
            {
                idx = (idx + 1) % Buckets;
                var old = Interlocked.Exchange(ref buckets[idx], 0);
                Interlocked.Add(ref total, -old);
            }

            Volatile.Write(ref lastBucketIndex, idx);
            Volatile.Write(ref lastBucketEpoch10s, now10);
        }

        /// <summary>
        /// Add n "share events" to the current bucket and update the running total.
        /// </summary>
        public void Add(int n)
        {
            var now10 = Now10();
            AdvanceIfNeeded(now10);

            var idx = Volatile.Read(ref lastBucketIndex);
            Interlocked.Add(ref buckets[idx], n);
            Interlocked.Add(ref total, n);
        }

        /// <summary>
        /// O(1) sum across the full configured window (600 s).
        /// </summary>
        public int Sum()
        {
            AdvanceIfNeeded(Now10());
            return (int)Volatile.Read(ref total);
        }

        /// <summary>
        /// O(k) sum across the last "windowSec" seconds, rounded down to bucket resolution.
        /// For example, with 10 s buckets: 125 s -> 12 buckets -> 120 s of data.
        /// </summary>
        public int SumWindow(int windowSec)
        {
            AdvanceIfNeeded(Now10());

            var k = Math.Clamp(windowSec / BucketSec, 1, Buckets);
            var sum = 0;
            var idx = Volatile.Read(ref lastBucketIndex);

            for (int i = 0; i < k; i++)
            {
                var bi = (idx - i + Buckets) % Buckets;
                sum += Volatile.Read(ref buckets[bi]);
            }

            return sum;
        }
    }

    // --- Sharding to reduce contention on dictionaries under high concurrency.
    private const int Shards = 64;

    private static readonly ConcurrentDictionary<string, RollingRing>[] PoolRings =
        Enumerable.Range(0, Shards).Select(_ =>
            new ConcurrentDictionary<string, RollingRing>(StringComparer.OrdinalIgnoreCase)).ToArray();

    private static readonly ConcurrentDictionary<(string poolId, string address), RollingRing>[] MinerRings =
        Enumerable.Range(0, Shards).Select(_ =>
            new ConcurrentDictionary<(string, string), RollingRing>()).ToArray();

    private static readonly ConcurrentDictionary<(string poolId, string address), long>[] MinerLastSeen =
        Enumerable.Range(0, Shards).Select(_ =>
            new ConcurrentDictionary<(string, string), long>()).ToArray();

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int ShardOf(string s) =>
        ((s.GetHashCode() & 0x7fffffff) % Shards);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int ShardOf((string a, string b) k)
    {
        // Stable-ish tuple hashing; ensure correct precedence of modulo and mask.
        var h = ((k.a.GetHashCode() * 397) ^ k.b.GetHashCode());
        return ((h & 0x7fffffff) % Shards);
    }

    /// <summary>
    /// Get or create the rolling ring for a pool.
    /// The ring window is fixed; interpretation (actual time window) is up to the caller via SumWindow().
    /// </summary>
    public static RollingRing ForPool(string poolId)
    {
        var shard = ShardOf(poolId);
        return PoolRings[shard].GetOrAdd(poolId, _ => new RollingRing());
    }

    /// <summary>
    /// Get or create the rolling ring for a (pool, miner).
    /// </summary>
    public static RollingRing ForMiner(string poolId, string address)
    {
        var key = (poolId, address);
        var shard = ShardOf(key);
        return MinerRings[shard].GetOrAdd(key, _ => new RollingRing());
    }

    /// <summary>
    /// Mark a miner as "seen now". Call this when a share arrives or a heartbeat is processed.
    /// </summary>
    public static void TouchMiner(string poolId, string address)
    {
        var key = (poolId, address);
        var shard = ShardOf(key);
        MinerLastSeen[shard][key] = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
    }

    /// <summary>
    /// A miner is considered online if it was seen within the last OnlineGraceSec (or window).
    /// </summary>
    public static bool IsOnline(string poolId, string address)
    {
        var key = (poolId, address);
        var shard = ShardOf(key);
        return MinerLastSeen[shard].TryGetValue(key, out var last) &&
               (DateTimeOffset.UtcNow.ToUnixTimeSeconds() - last) <= Math.Max(DefaultWindowSec, OnlineGraceSec);
    }

    /// <summary>
    /// Get last-seen epoch seconds for a miner, or null if never seen.
    /// </summary>
    public static long? GetMinerLastSeenSec(string poolId, string address)
    {
        var key = (poolId, address);
        var shard = ShardOf(key);
        return MinerLastSeen[shard].TryGetValue(key, out var last) ? last : (long?)null;
    }

    /// <summary>
    /// Enumerate all miners in a pool with their ring and last-seen.
    /// This is optimized for read-mostly and avoids allocations inside the hot path.
    /// </summary>
    public static IEnumerable<(string address, RollingRing ring, long lastSeen)> EnumeratePoolMiners(string poolId)
    {
        for (int i = 0; i < Shards; i++)
        {
            foreach (var kv in MinerRings[i])
            {
                if (kv.Key.poolId == poolId)
                {
                    MinerLastSeen[i].TryGetValue(kv.Key, out var last);
                    yield return (kv.Key.address, kv.Value, last);
                }
            }
        }
    }

    /// <summary>
    /// Enumerate all miners across all pools.
    /// Useful for global search endpoints.
    /// </summary>
    public static IEnumerable<(string poolId, string address, RollingRing ring, long lastSeen)> EnumerateAllMiners()
    {
        for (int i = 0; i < Shards; i++)
        {
            foreach (var kv in MinerRings[i])
            {
                MinerLastSeen[i].TryGetValue(kv.Key, out var last);
                yield return (kv.Key.poolId, kv.Key.address, kv.Value, last);
            }
        }
    }
}
