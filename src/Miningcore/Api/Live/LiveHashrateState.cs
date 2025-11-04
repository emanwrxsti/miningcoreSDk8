// miningcore/src/Miningcore/Api/Live/LiveHashrateState.cs
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Miningcore.Live;

/// Lock-free in-memory rolling counters (per second) for LIVE.
/// - Ring of 1024 seconds (power of 2), sum weighted by the difficulty of the share.
/// - Add(double) is O(1) with Interlocked.
/// - SumWindow(sec) iterates at most 'windowSec' seconds (<= 600 typical).
public static class LiveHashrateState
{
    public const int DefaultWindowSec = 600;    // 10 min
    private const int RingSize = 1024;          // power of 2
    private const int Mask = RingSize - 1;
    private const long SCALE = 1_000_000;       // fixed-point 6 decimals
    public const int OnlineGraceSec = 120;

    public sealed class RollingRing
    {
        private readonly long[] buckets = new long[RingSize]; // scaled values
        private readonly int[] secs = new int[RingSize];      // epochSec from bucket

        /// Increments the bucket of the second chain by 'amount' (difficulty-weighted).
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Add(double amount)
        {
            var nowSec = (int) DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            var idx = nowSec & Mask;

            if (Volatile.Read(ref secs[idx]) != nowSec)
            {
                Volatile.Write(ref secs[idx], nowSec);
                Interlocked.Exchange(ref buckets[idx], 0);
            }

            var inc = (long) Math.Round(amount * SCALE);
            Interlocked.Add(ref buckets[idx], inc);
        }

        /// Sum of last 'windowSec' seconds.
        public double SumWindow(int windowSec)
        {
            if (windowSec <= 0) windowSec = DefaultWindowSec;
            var nowSec = (int) DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            var fromSec = nowSec - windowSec + 1;

            long acc = 0;
            for (var t = fromSec; t <= nowSec; t++)
            {
                var idx = t & Mask;
                if (Volatile.Read(ref secs[idx]) == t)
                    acc += Volatile.Read(ref buckets[idx]);
            }

            return acc / (double) SCALE;
        }
    }

    // ---- Sharding to reduce containment
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
    private static int ShardOf(string s) => (s.GetHashCode() & 0x7fffffff) % Shards;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int ShardOf((string a, string b) k)
    {
        var h = ((k.a.GetHashCode() * 397) ^ k.b.GetHashCode());
        return (h & 0x7fffffff) % Shards;
    }

    public static RollingRing ForPool(string poolId)
    {
        var shard = ShardOf(poolId);
        return PoolRings[shard].GetOrAdd(poolId, _ => new RollingRing());
    }

    public static RollingRing ForMiner(string poolId, string address)
    {
        var key = (poolId, address);
        var shard = ShardOf(key);
        return MinerRings[shard].GetOrAdd(key, _ => new RollingRing());
    }

    public static void TouchMiner(string poolId, string address)
    {
        var key = (poolId, address);
        var shard = ShardOf(key);
        MinerLastSeen[shard][key] = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
    }

    public static bool IsOnline(string poolId, string address)
    {
        var key = (poolId, address);
        var shard = ShardOf(key);
        return MinerLastSeen[shard].TryGetValue(key, out var last) &&
               (DateTimeOffset.UtcNow.ToUnixTimeSeconds() - last) <= Math.Max(DefaultWindowSec, OnlineGraceSec);
    }

    public static long? GetMinerLastSeenSec(string poolId, string address)
    {
        var key = (poolId, address);
        var shard = ShardOf(key);
        return MinerLastSeen[shard].TryGetValue(key, out var last) ? last : (long?) null;
    }

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
