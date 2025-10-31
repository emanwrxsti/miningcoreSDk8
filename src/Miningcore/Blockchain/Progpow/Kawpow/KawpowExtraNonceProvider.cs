using Miningcore.Mining;

namespace Miningcore.Blockchain.Progpow.kawpow
{
    /// <summary>
    /// ExtraNonce provider tuned for KawPoW profiles.
    /// </summary>
    public class KawpowExtraNonceProvider : ExtraNonceProviderBase
    {
        public KawpowExtraNonceProvider(string poolId, byte? clusterInstanceId)
            : base(poolId, KawpowConstants.ExtranoncePlaceHolderLength, clusterInstanceId) { }
    }
}
