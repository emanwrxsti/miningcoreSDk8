namespace Miningcore.Api.Responses
{
    // Use double for all rate-like numeric metrics to preserve precision
    public partial class AggregatedPoolStats
    {
        // Pool hashrate can be very large; double avoids precision loss vs float
        public double PoolHashrate { get; set; }

        public int ConnectedMiners { get; set; }

        // Shares/second is a rate and benefits from double precision
        public double ValidSharesPerSecond { get; set; }

        public double NetworkHashrate { get; set; }
        public double NetworkDifficulty { get; set; }

        public DateTime Created { get; set; }
    }

    public class GetPoolStatsResponse
    {
        public AggregatedPoolStats[] Stats { get; set; }
    }
}
