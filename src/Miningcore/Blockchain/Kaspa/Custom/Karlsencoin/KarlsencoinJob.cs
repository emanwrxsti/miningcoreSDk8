using System;
using System.Numerics;
using Miningcore.Crypto;
using Miningcore.Crypto.Hashing.Algorithms;
using Miningcore.Extensions;
using Miningcore.Stratum;
using Miningcore.Util;
using NBitcoin;

namespace Miningcore.Blockchain.Kaspa.Custom.Karlsencoin;

public class KarlsencoinJob : KaspaJob
{
    public KarlsencoinJob(IHashAlgorithm customBlockHeaderHasher, IHashAlgorithm customCoinbaseHasher, IHashAlgorithm customShareHasher) : base(customBlockHeaderHasher, customCoinbaseHasher, customShareHasher)
    {
    }

    protected override void SerializeCoinbase(ReadOnlySpan<byte> prePowHash, long timestamp, ulong nonce, Span<byte> result)
    {
        using var stream = new MemoryStream();
        stream.Write(prePowHash);
        stream.Write(BitConverter.GetBytes((ulong)timestamp));
        stream.Write(new byte[32]); // 32 zero bytes padding
        stream.Write(BitConverter.GetBytes(nonce));

        var streamBytes = (Span<byte>)stream.ToArray();
        if (shareHasher is not FishHashKarlsen)
            coinbaseHasher.Digest(streamBytes, result);
        else
            streamBytes.CopyTo(result);
    }


    protected override Share ProcessShareInternal(StratumConnection worker, string nonce)
    {
        var context = worker.ContextAs<KaspaWorkerContext>();
        BlockTemplate.Header.Nonce = Convert.ToUInt64(nonce, 16);

        Span<byte> coinbaseBuf = stackalloc byte[(shareHasher is not FishHashKarlsen) ? 32 : KarlsencoinConstants.CoinbaseSize];
        SerializeCoinbase(prePowHashBytes, BlockTemplate.Header.Timestamp, BlockTemplate.Header.Nonce, coinbaseBuf);

        Span<byte> shareHash32 = stackalloc byte[32];

        if (shareHasher is not FishHashKarlsen)
        {
            Span<byte> mixed32 = stackalloc byte[32];
            ComputeCoinbase(prePowHashBytes, coinbaseBuf, mixed32);
            shareHasher.Digest(mixed32, shareHash32);
        }
        else
        {
            shareHasher.Digest(coinbaseBuf, shareHash32);
        }

        var targetShare = new Target(new BigInteger(shareHash32.ToNewReverseArray(), true, true));
        var shareValue = targetShare.ToUInt256();

        var shareDiff = (double)new BigRational(KarlsencoinConstants.Diff1b, targetShare.ToBigInteger()) * shareMultiplier;
        var stratumDifficulty = context.Difficulty;
        var ratio = shareDiff / stratumDifficulty;

        bool isBlockCandidate = shareValue <= blockTargetValue;

        if (!isBlockCandidate && ratio < 0.99)
        {
            if (context.VarDiff?.LastUpdate != null && context.PreviousDifficulty.HasValue)
            {
                ratio = shareDiff / context.PreviousDifficulty.Value;
                if (ratio < 0.99)
                    throw new StratumException(StratumError.LowDifficultyShare, $"low difficulty share ({shareDiff})");
                stratumDifficulty = context.PreviousDifficulty.Value;
            }
            else
                throw new StratumException(StratumError.LowDifficultyShare, $"low difficulty share ({shareDiff})");
        }

        var result = new Share
        {
            BlockHeight = (long)BlockTemplate.Header.DaaScore,
            NetworkDifficulty = Difficulty,
            Difficulty = context.Difficulty / shareMultiplier
        };

        if (isBlockCandidate)
        {
            Span<byte> hdrHash = stackalloc byte[32];
            SerializeHeader(BlockTemplate.Header, hdrHash, false);
            result.IsBlockCandidate = true;
            result.BlockHash = hdrHash.ToHexString();
        }

        return result;
    }

}