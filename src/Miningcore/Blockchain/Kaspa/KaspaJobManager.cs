using System;
using static System.Array;
using System.Globalization;
using System.Net.Http;
using System.Numerics;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using Autofac;
using Grpc.Core;
using Grpc.Net.Client;
using Miningcore.Blockchain.Kaspa.Configuration;
using Miningcore.Blockchain.Kaspa.Custom.Astrix;
using Miningcore.Blockchain.Kaspa.Custom.Karlsencoin;
using Miningcore.Blockchain.Kaspa.Custom.KaspaHeavy;
using Miningcore.Blockchain.Kaspa.Custom.Pyrin;
using Miningcore.Blockchain.Kaspa.Custom.Spectre;
using NLog;
using Miningcore.Configuration;
using Miningcore.Crypto;
using Miningcore.Crypto.Hashing.Algorithms;
using Miningcore.Extensions;
using Miningcore.Messaging;
using Miningcore.Mining;
using Miningcore.Notifications.Messages;
using Miningcore.Stratum;
using Miningcore.Time;
using Newtonsoft.Json;
using Contract = Miningcore.Contracts.Contract;
using static Miningcore.Util.ActionUtils;
using kaspaWalletd = Miningcore.Blockchain.Kaspa.KaspaWalletd;
using kaspad = Miningcore.Blockchain.Kaspa.Kaspad;

namespace Miningcore.Blockchain.Kaspa;

public class KaspaJobManager : JobManagerBase<KaspaJob>
{
    public KaspaJobManager(
        IComponentContext ctx,
        IMessageBus messageBus,
        IMasterClock clock,
        IExtraNonceProvider extraNonceProvider) :
        base(ctx, messageBus)
    {
        Contract.RequiresNonNull(clock);
        Contract.RequiresNonNull(extraNonceProvider);

        this.clock = clock;
        this.extraNonceProvider = extraNonceProvider;
    }

    private DaemonEndpointConfig[] daemonEndpoints;
    private DaemonEndpointConfig[] walletDaemonEndpoints;
    private KaspaCoinTemplate coin;
    private kaspad.KaspadRPC.KaspadRPCClient rpc;
    private kaspaWalletd.KaspaWalletdRPC.KaspaWalletdRPCClient walletRpc;
    private string network;
    private readonly IExtraNonceProvider extraNonceProvider;
    private readonly IMasterClock clock;
    private KaspaPoolConfigExtra extraPoolConfig;
    private KaspaPaymentProcessingConfigExtra extraPoolPaymentProcessingConfig;
    protected int maxActiveJobs;
    protected string extraData;
    protected IHashAlgorithm customBlockHeaderHasher;
    protected IHashAlgorithm customCoinbaseHasher;
    protected IHashAlgorithm customShareHasher;

    protected IObservable<kaspad.RpcBlock> KaspaSubscribeNewBlockTemplate(CancellationToken ct, object payload = null,
        JsonSerializerSettings payloadJsonSerializerSettings = null)
    {
        return Observable.Defer(() => Observable.Create<kaspad.RpcBlock>(obs =>
        {
            var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);

            Task.Run(async () =>
            {
                using (cts)
                {
                retry_subscription:
                    // stream to communicate with Kaspad
                    var streamNotifyNewBlockTemplate = rpc.MessageStream(null, null, cts.Token);

                    // request to subscribe NotifyNewBlockTemplate
                    var requestNotifyNewBlockTemplate = new kaspad.KaspadMessage();
                    requestNotifyNewBlockTemplate.NotifyNewBlockTemplateRequest = new kaspad.NotifyNewBlockTemplateRequestMessage();

                    // request to get BlockTemplate
                    var requestBlockTemplate = new kaspad.KaspadMessage();
                    requestBlockTemplate.GetBlockTemplateRequest = new kaspad.GetBlockTemplateRequestMessage
                    {
                        PayAddress = poolConfig.Address,
                        ExtraData = extraData,
                    };

                    logger.Debug(() => $"Sending NotifyNewBlockTemplateRequest");

                    try
                    {
                        await streamNotifyNewBlockTemplate.RequestStream.WriteAsync(requestNotifyNewBlockTemplate, cts.Token);
                    }
                    catch (Exception ex)
                    {
                        logger.Error(() => $"{ex.GetType().Name} '{ex.Message}' while subscribing to kaspad \"NewBlockTemplate\" notifications");

                        if (!cts.IsCancellationRequested)
                        {
                            await streamNotifyNewBlockTemplate.RequestStream.CompleteAsync();
                            logger.Error(() => $"Reconnecting in 10s");
                            await Task.Delay(TimeSpan.FromSeconds(10), cts.Token);
                            goto retry_subscription;
                        }
                        else
                            goto end_gameover;
                    }

                    while (!cts.IsCancellationRequested)
                    {
                        logger.Debug(() => $"Successful `NewBlockTemplate` subscription");

                    retry_blocktemplate:
                        logger.Debug(() => $"New job received :D");

                        try
                        {
                            await streamNotifyNewBlockTemplate.RequestStream.WriteAsync(requestBlockTemplate, cts.Token);
                            await foreach (var responseBlockTemplate in streamNotifyNewBlockTemplate.ResponseStream.ReadAllAsync(cts.Token))
                            {
                                logger.Debug(() => $"DaaScore (BlockHeight): {responseBlockTemplate.GetBlockTemplateResponse.Block.Header.DaaScore}");

                                obs.OnNext(responseBlockTemplate.GetBlockTemplateResponse.Block);

                                if (!string.IsNullOrEmpty(responseBlockTemplate.GetBlockTemplateResponse.Error?.Message))
                                    logger.Warn(() => responseBlockTemplate.GetBlockTemplateResponse.Error?.Message);
                            }
                        }
                        catch (NullReferenceException)
                        {
                            logger.Info(() => $"Waiting for `NewBlockTemplate` data...");
                            goto retry_blocktemplate;
                        }

                        catch (Exception ex)
                        {
                            logger.Error(() => $"{ex.GetType().Name} '{ex.Message}' while streaming kaspad \"NewBlockTemplate\" notifications");

                            if (!cts.IsCancellationRequested)
                            {
                                await streamNotifyNewBlockTemplate.RequestStream.CompleteAsync();
                                logger.Error(() => $"Reconnecting in 10s");
                                await Task.Delay(TimeSpan.FromSeconds(10), cts.Token);
                                goto retry_subscription;
                            }
                            else
                                goto end_gameover;
                        }
                    }

                end_gameover:
                    await streamNotifyNewBlockTemplate.RequestStream.CompleteAsync();
                    logger.Debug(() => $"No more data received. Bye!");
                }
            }, cts.Token);

            return Disposable.Create(() => { cts.Cancel(); });
        }));
    }

    private void SetupJobUpdates(CancellationToken ct)
    {
        var blockFound = blockFoundSubject.Synchronize();

        var triggers = new List<IObservable<(string Via, kaspad.RpcBlock Data)>>
        {
            blockFound.Select(_ => (JobRefreshBy.BlockFound, (kaspad.RpcBlock) null))
        };

        // stream of NewBlockTemplate
        var getWorkKaspad = KaspaSubscribeNewBlockTemplate(ct)
            .Publish()
            .RefCount();

        triggers.Add(getWorkKaspad
            .Select(blockTemplate => (JobRefreshBy.BlockTemplateStream, blockTemplate))
            .Publish()
            .RefCount());

        // initial job 
        triggers.Add(Observable.Interval(TimeSpan.FromMilliseconds(1000))
            .Select(_ => (JobRefreshBy.Initial, (kaspad.RpcBlock)null))
            .TakeWhile(_ => !hasInitialBlockTemplate));

        Jobs = triggers.Merge()
            .Select(x => Observable.FromAsync(() => UpdateJob(ct, x.Via, x.Data)))
            .Concat()
            .Where(x => x)
            .Do(x =>
            {
                if (x)
                    hasInitialBlockTemplate = true;
            })
            .Select(x => GetJobParamsForStratum())
            .Publish()
            .RefCount();
    }

    private KaspaJob CreateJob(ulong blockHeight)
    {
        switch (coin.Symbol)
        {
            case "AIX":
                if (customBlockHeaderHasher is not Blake2b)
                    customBlockHeaderHasher = new Blake2b(Encoding.UTF8.GetBytes(KaspaConstants.CoinbaseBlockHash));

                if (customCoinbaseHasher is not CShake256)
                    customCoinbaseHasher = new CShake256(null, Encoding.UTF8.GetBytes(KaspaConstants.CoinbaseProofOfWorkHash));

                if (customShareHasher is not CShake256)
                    customShareHasher = new CShake256(null, Encoding.UTF8.GetBytes(KaspaConstants.CoinbaseHeavyHash));

                return new AstrixJob(customBlockHeaderHasher, customCoinbaseHasher, customShareHasher);

            case "CAS":
            case "HTN":
                if (customBlockHeaderHasher is not Blake3)
                {
                    string coinbaseBlockHash = KaspaConstants.CoinbaseBlockHash;
                    byte[] hashBytes = Encoding.UTF8.GetBytes(coinbaseBlockHash.PadRight(32, '\0')).Take(32).ToArray();
                    customBlockHeaderHasher = new Blake3(hashBytes);
                }

                if (customCoinbaseHasher is not Blake3)
                    customCoinbaseHasher = new Blake3();

                if (customShareHasher is not Blake3)
                    customShareHasher = new Blake3();

                return new PyrinJob(customBlockHeaderHasher, customCoinbaseHasher, customShareHasher);

            case "KAS":
                // KASPA vanilla → HeavyHash on header
                var heavy = new HeavyHash();
                return new KaspaHeavyJob(heavy, heavy);

            case "KLS":
                var karlsenNetwork = network.ToLower();

                if (customBlockHeaderHasher is not Blake2b)
                    customBlockHeaderHasher = new Blake2b(Encoding.UTF8.GetBytes(KaspaConstants.CoinbaseBlockHash));

                if (customCoinbaseHasher is not Blake3)
                    customCoinbaseHasher = new Blake3();

                if ((karlsenNetwork == "testnet" && blockHeight >= KarlsencoinConstants.FishHashPlusForkHeightTestnet) || (karlsenNetwork == "mainnet" && blockHeight >= KarlsencoinConstants.FishHashPlusForkHeightMainnet))
                {
                    logger.Debug(() => $"fishHashPlusHardFork activated");

                    if (customShareHasher is not FishHashKarlsen)
                        customShareHasher = new FishHashKarlsen(FishHash.FishHashKernelPlus);
                    else if (customShareHasher is FishHashKarlsen fishHashKarlsenAlgo)
                    {
                        if (fishHashKarlsenAlgo.fishHashKernel != FishHash.FishHashKernelPlus)
                            customShareHasher = new FishHashKarlsen(FishHash.FishHashKernelPlus);
                    }

                }
                else if (karlsenNetwork == "testnet" && blockHeight >= KarlsencoinConstants.FishHashForkHeightTestnet)
                {
                    logger.Debug(() => $"fishHashHardFork activated");

                    if (customShareHasher is not FishHashKarlsen)
                        customShareHasher = new FishHashKarlsen();
                }
                else
                    if (customShareHasher is not CShake256)
                    customShareHasher = new CShake256(null, Encoding.UTF8.GetBytes(KaspaConstants.CoinbaseHeavyHash));

                return new KarlsencoinJob(customBlockHeaderHasher, customCoinbaseHasher, customShareHasher);

            case "CSS":
            case "NTL":
            case "NXL":
            case "PUG":
                if (customBlockHeaderHasher is not Blake2b)
                    customBlockHeaderHasher = new Blake2b(Encoding.UTF8.GetBytes(KaspaConstants.CoinbaseBlockHash));

                if (customCoinbaseHasher is not Blake3)
                    customCoinbaseHasher = new Blake3();

                if (customShareHasher is not CShake256)
                    customShareHasher = new CShake256(null, Encoding.UTF8.GetBytes(KaspaConstants.CoinbaseHeavyHash));

                return new KarlsencoinJob(customBlockHeaderHasher, customCoinbaseHasher, customShareHasher);

            case "PYI":
                if (blockHeight >= PyrinConstants.Blake3ForkHeight)
                {
                    logger.Debug(() => $"blake3HardFork activated");

                    if (customBlockHeaderHasher is not Blake3)
                    {
                        string coinbaseBlockHash = KaspaConstants.CoinbaseBlockHash;
                        byte[] hashBytes = Encoding.UTF8.GetBytes(coinbaseBlockHash.PadRight(32, '\0')).Take(32).ToArray();
                        customBlockHeaderHasher = new Blake3(hashBytes);
                    }

                    if (customCoinbaseHasher is not Blake3)
                        customCoinbaseHasher = new Blake3();

                    if (customShareHasher is not Blake3)
                        customShareHasher = new Blake3();
                }
                else
                {
                    if (customBlockHeaderHasher is not Blake2b)
                        customBlockHeaderHasher = new Blake2b(Encoding.UTF8.GetBytes(KaspaConstants.CoinbaseBlockHash));

                    if (customCoinbaseHasher is not CShake256)
                        customCoinbaseHasher = new CShake256(null, Encoding.UTF8.GetBytes(KaspaConstants.CoinbaseProofOfWorkHash));

                    if (customShareHasher is not CShake256)
                        customShareHasher = new CShake256(null, Encoding.UTF8.GetBytes(KaspaConstants.CoinbaseHeavyHash));
                }

                return new PyrinJob(customBlockHeaderHasher, customCoinbaseHasher, customShareHasher);

            case "SPR":
                if (customBlockHeaderHasher is not Blake2b)
                    customBlockHeaderHasher = new Blake2b(Encoding.UTF8.GetBytes(KaspaConstants.CoinbaseBlockHash));

                if (customCoinbaseHasher is not CShake256)
                    customCoinbaseHasher = new CShake256(null, Encoding.UTF8.GetBytes(KaspaConstants.CoinbaseProofOfWorkHash));

                if (customShareHasher is not CShake256)
                    customShareHasher = new CShake256(null, Encoding.UTF8.GetBytes(KaspaConstants.CoinbaseHeavyHash));

                return new SpectreJob(customBlockHeaderHasher, customCoinbaseHasher, customShareHasher);
        }

        if (customBlockHeaderHasher is not Blake2b)
            customBlockHeaderHasher = new Blake2b(Encoding.UTF8.GetBytes(KaspaConstants.CoinbaseBlockHash));

        if (customCoinbaseHasher is not CShake256)
            customCoinbaseHasher = new CShake256(null, Encoding.UTF8.GetBytes(KaspaConstants.CoinbaseProofOfWorkHash));

        if (customShareHasher is not CShake256)
            customShareHasher = new CShake256(null, Encoding.UTF8.GetBytes(KaspaConstants.CoinbaseHeavyHash));

        return new KaspaJob(customBlockHeaderHasher, customCoinbaseHasher, customShareHasher);
    }

    private async Task<bool> UpdateJob(CancellationToken ct, string via = null, kaspad.RpcBlock blockTemplate = null)
    {
        var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);

        return await Task.Run(() =>
        {
            using (cts)
            {
                try
                {
                    if (blockTemplate == null)
                        return false;

                    var job = currentJob;

                    var isNew = (job == null || job.BlockTemplate?.Header.DaaScore < blockTemplate.Header.DaaScore);

                    if (isNew)
                        messageBus.NotifyChainHeight(poolConfig.Id, blockTemplate.Header.DaaScore, poolConfig.Template);

                    if (isNew)
                    {
                        job = CreateJob(blockTemplate.Header.DaaScore);

                        job.Init(blockTemplate, NextJobId("D"), ShareMultiplier);

                        logger.Debug(() => $"blockTargetValue: {job.blockTargetValue}");
                        logger.Debug(() => $"Difficulty: {job.Difficulty}");

                        if (via != null)
                            logger.Info(() => $"Detected new block {job.BlockTemplate.Header.DaaScore} [{via}]");
                        else
                            logger.Info(() => $"Detected new block {job.BlockTemplate.Header.DaaScore}");

                        // update stats
                        if (job.BlockTemplate.Header.DaaScore > BlockchainStats.BlockHeight)
                        {
                            BlockchainStats.LastNetworkBlockTime = clock.Now;
                            BlockchainStats.BlockHeight = job.BlockTemplate.Header.DaaScore;
                            BlockchainStats.NetworkDifficulty = job.Difficulty;
                        }

                        currentJob = job;
                    }
                    else
                    {
                        if (via != null)
                            logger.Debug(() => $"Template update {job.BlockTemplate.Header.DaaScore}");
                        else
                            logger.Debug(() => $"Template update {job.BlockTemplate.Header.DaaScore}");
                    }

                    return isNew;
                }

                catch (OperationCanceledException)
                {
                    // ignore
                }

                catch (Exception ex)
                {
                    logger.Error(() => $"{ex.GetType().Name} '{ex.Message}' while updating new job");
                }

                return false;
            }
        }, cts.Token);
    }

    private async Task UpdateNetworkStatsAsync(CancellationToken ct)
    {
        try
        {
            var stream = rpc.MessageStream(null, null, ct);

            var request = new kaspad.KaspadMessage();
            request.EstimateNetworkHashesPerSecondRequest = new kaspad.EstimateNetworkHashesPerSecondRequestMessage
            {
                WindowSize = 1000,
            };
            await stream.RequestStream.WriteAsync(request);
            await foreach (var infoHashrate in stream.ResponseStream.ReadAllAsync(ct))
            {
                if (string.IsNullOrEmpty(infoHashrate.EstimateNetworkHashesPerSecondResponse.Error?.Message))
                    BlockchainStats.NetworkHashrate = (double)infoHashrate.EstimateNetworkHashesPerSecondResponse.NetworkHashesPerSecond;

                break;
            }

            request = new kaspad.KaspadMessage();
            request.GetConnectedPeerInfoRequest = new kaspad.GetConnectedPeerInfoRequestMessage();
            await stream.RequestStream.WriteAsync(request);
            await foreach (var info in stream.ResponseStream.ReadAllAsync(ct))
            {
                if (string.IsNullOrEmpty(info.GetConnectedPeerInfoResponse.Error?.Message))
                    BlockchainStats.ConnectedPeers = info.GetConnectedPeerInfoResponse.Infos.Count;

                break;
            }
            await stream.RequestStream.CompleteAsync();
        }

        catch (Exception ex)
        {
            logger.Error(() => $"{ex.GetType().Name} '{ex.Message}' while updating network stats");
        }
    }

    private async Task ShowDaemonSyncProgressAsync(CancellationToken ct)
    {
        var stream = rpc.MessageStream(null, null, ct);

        var request = new kaspad.KaspadMessage();
        request.GetInfoRequest = new kaspad.GetInfoRequestMessage();
        await Guard(() => stream.RequestStream.WriteAsync(request),
            ex => logger.Debug(ex));
        await foreach (var info in stream.ResponseStream.ReadAllAsync(ct))
        {
            if (!string.IsNullOrEmpty(info.GetInfoResponse.Error?.Message))
                logger.Debug(info.GetInfoResponse.Error?.Message);

            if (info.GetInfoResponse.IsSynced != true && info.GetInfoResponse.IsUtxoIndexed != true)
                logger.Info(() => $"Daemon is downloading headers ...");

            break;
        }
        await stream.RequestStream.CompleteAsync();
    }

    private async Task<bool> SubmitBlockAsync(CancellationToken ct, kaspad.RpcBlock block, object payload = null,
        JsonSerializerSettings payloadJsonSerializerSettings = null)
    {
        Contract.RequiresNonNull(block);

        bool succeed = false;

        try
        {
            var stream = rpc.MessageStream(null, null, ct);

            var request = new kaspad.KaspadMessage();
            request.SubmitBlockRequest = new kaspad.SubmitBlockRequestMessage
            {
                Block = block,
                AllowNonDAABlocks = false,
            };
            await stream.RequestStream.WriteAsync(request);
            await foreach (var response in stream.ResponseStream.ReadAllAsync(ct))
            {
                if (!string.IsNullOrEmpty(response.SubmitBlockResponse.Error?.Message))
                {
                    logger.Warn(() => $"Block submission failed: {response.SubmitBlockResponse.Error?.Message} [{response.SubmitBlockResponse?.RejectReason.ToString()}]");
                    messageBus.SendMessage(new AdminNotification("Block submission failed", $"Pool {poolConfig.Id}: {response.SubmitBlockResponse.Error?.Message} [{response.SubmitBlockResponse?.RejectReason.ToString()}]"));
                }
                else
                    succeed = true;

                break;
            }
            await stream.RequestStream.CompleteAsync();
        }

        catch (Exception ex)
        {
            logger.Error(() => $"{ex.GetType().Name} '{ex.Message}' while submitting block");
            messageBus.SendMessage(new AdminNotification("Block submission failed", $"Pool {poolConfig.Id} failed to submit block"));
        }

        return succeed;
    }

    private object[] GetJobParamsForStratum()
    {
        var job = currentJob;
        return job?.GetJobParams();
    }

    public override KaspaJob GetJobForStratum()
    {
        var job = currentJob;
        return job;
    }

    #region API-Surface

    public IObservable<object[]> Jobs { get; private set; }
    public BlockchainStats BlockchainStats { get; } = new();
    public string Network => network;

    public KaspaCoinTemplate Coin => coin;

    public object[] GetSubscriberData(StratumConnection worker)
    {
        Contract.RequiresNonNull(worker);

        var context = worker.ContextAs<KaspaWorkerContext>();
        var extraNonce1Size = GetExtraNonce1Size();

        // assign unique ExtraNonce1 to worker (miner)
        context.ExtraNonce1 = extraNonceProvider.Next();

        // setup response data
        var responseData = new object[]
        {
            context.ExtraNonce1,
            KaspaConstants.ExtranoncePlaceHolderLength - extraNonce1Size,
        };

        return responseData;
    }

    public int GetExtraNonce1Size()
    {
        return extraPoolConfig?.ExtraNonce1Size ?? 2;
    }

    public virtual async ValueTask<Share> SubmitShareAsync(StratumConnection worker, object submission, CancellationToken ct)
    {
        Contract.RequiresNonNull(worker);
        Contract.RequiresNonNull(submission);

        if (submission is not object[] submitParams)
            throw new StratumException(StratumError.Other, "invalid params");

        var context = worker.ContextAs<KaspaWorkerContext>();

        var jobId = submitParams[1] as string;
        var nonce = submitParams[2] as string;

        KaspaJob job;

        lock (context)
        {
            job = context.GetJob(jobId);

            if (job == null)
            {
                // hack for ASICs sending wrong jobIds (IceRiver/Bitmain)
                if (ValidateIsGodMiner(context.UserAgent) || ValidateIsIceRiverMiner(context.UserAgent))
                    job = context.validJobs.ToArray().FirstOrDefault(x => Int64.Parse(x.JobId) < Int64.Parse(jobId));
            }

            if (job == null)
                logger.Warn(() => $"[{context.Miner}] => jobId: {jobId} - Last known job: {context.validJobs.ToArray().FirstOrDefault()?.JobId}");
        }

        if (job == null)
            throw new StratumException(StratumError.JobNotFound, "job not found");

        // validate & process
        var share = job.ProcessShare(worker, nonce);

        // enrich share with common data
        share.PoolId = poolConfig.Id;
        share.IpAddress = worker.RemoteEndpoint.Address.ToString();
        share.Miner = context.Miner;
        share.Worker = context.Worker;
        share.UserAgent = context.UserAgent;
        share.Source = clusterConfig.ClusterName;
        share.Created = clock.Now;

        // block candidate → submit
        if (share.IsBlockCandidate)
        {
            logger.Info(() => $"Submitting block {share.BlockHeight} [{share.BlockHash}]");

            var acceptResponse = await SubmitBlockAsync(ct, job.BlockTemplate);

            share.IsBlockCandidate = acceptResponse;

            if (share.IsBlockCandidate)
            {
                logger.Info(() => $"Daemon accepted block {share.BlockHeight} [{share.BlockHash}] submitted by {context.Miner}");

                OnBlockFound();

                share.TransactionConfirmationData = nonce;
            }

            else
            {
                share.TransactionConfirmationData = null;
            }
        }

        return share;
    }

    public bool ValidateIsLargeJob(string userAgent)
    {
        if (string.IsNullOrEmpty(userAgent))
            return false;

        if (ValidateIsBzMiner(userAgent))
            return true;

        if (ValidateIsIceRiverMiner(userAgent))
            return true;

        if (ValidateIsGoldShell(userAgent))
            return true;

        return false;
    }

    public bool ValidateIsBzMiner(string userAgent)
    {
        if (string.IsNullOrEmpty(userAgent))
            return false;

        MatchCollection matchesUserAgentBzMiner = KaspaConstants.RegexUserAgentBzMiner.Matches(userAgent);
        return (matchesUserAgentBzMiner.Count > 0);
    }

    public bool ValidateIsGodMiner(string userAgent)
    {
        if (string.IsNullOrEmpty(userAgent))
            return false;

        MatchCollection matchesUserAgentGodMiner = KaspaConstants.RegexUserAgentGodMiner.Matches(userAgent);
        return (matchesUserAgentGodMiner.Count > 0);
    }

    public bool ValidateIsIceRiverMiner(string userAgent)
    {
        if (string.IsNullOrEmpty(userAgent))
            return false;

        MatchCollection matchesUserAgentIceRiverMiner = KaspaConstants.RegexUserAgentIceRiverMiner.Matches(userAgent);
        return (matchesUserAgentIceRiverMiner.Count > 0);
    }

    public bool ValidateIsGoldShell(string userAgent)
    {
        if (string.IsNullOrEmpty(userAgent))
            return false;

        MatchCollection matchesUserAgentGoldShell = KaspaConstants.RegexUserAgentGoldShell.Matches(userAgent);
        return (matchesUserAgentGoldShell.Count > 0);
    }

    public bool ValidateIsTNNMiner(string userAgent)
    {
        if (string.IsNullOrEmpty(userAgent))
            return false;

        MatchCollection matchesUserAgentTNNMiner = KaspaConstants.RegexUserAgentTNNMiner.Matches(userAgent);
        return (matchesUserAgentTNNMiner.Count > 0);
    }

    public double ShareMultiplier => coin.ShareMultiplier;

    #endregion // API-Surface

    #region Overrides

    protected override async Task PostStartInitAsync(CancellationToken ct)
    {
        // validar endereço do pool
        if (string.IsNullOrEmpty(poolConfig.Address))
            throw new PoolStartupException($"Pool address is not configured", poolConfig.Id);

        var stream = rpc.MessageStream(null, null, ct);

        var request = new kaspad.KaspadMessage();
        request.GetCurrentNetworkRequest = new kaspad.GetCurrentNetworkRequestMessage();
        await Guard(() => stream.RequestStream.WriteAsync(request),
            ex => throw new PoolStartupException($"Error writing a request in the communication stream '{ex.GetType().Name}' : {ex}", poolConfig.Id));
        await foreach (var currentNetwork in stream.ResponseStream.ReadAllAsync(ct))
        {
            if (!string.IsNullOrEmpty(currentNetwork.GetCurrentNetworkResponse.Error?.Message))
                throw new PoolStartupException($"Daemon reports: {currentNetwork.GetCurrentNetworkResponse.Error?.Message}", poolConfig.Id);

            network = currentNetwork.GetCurrentNetworkResponse.CurrentNetwork;
            break;
        }

        var (kaspaAddressUtility, errorKaspaAddressUtility) = KaspaUtils.ValidateAddress(poolConfig.Address, network, coin);
        if (errorKaspaAddressUtility != null)
            throw new PoolStartupException($"Pool address: {poolConfig.Address} is invalid for network [{network}]: {errorKaspaAddressUtility}", poolConfig.Id);
        else
            logger.Info(() => $"Pool address: {poolConfig.Address} => {KaspaConstants.KaspaAddressType[kaspaAddressUtility.KaspaAddress.Version()]}");

        // update stats
        BlockchainStats.NetworkType = network;
        BlockchainStats.RewardType = "POW";

        request = new kaspad.KaspadMessage();
        request.GetInfoRequest = new kaspad.GetInfoRequestMessage();
        await Guard(() => stream.RequestStream.WriteAsync(request),
            ex => throw new PoolStartupException($"Error writing a request in the communication stream '{ex.GetType().Name}' : {ex}", poolConfig.Id));
        await foreach (var info in stream.ResponseStream.ReadAllAsync(ct))
        {
            if (!string.IsNullOrEmpty(info.GetInfoResponse.Error?.Message))
                throw new PoolStartupException($"Daemon reports: {info.GetInfoResponse.Error?.Message}", poolConfig.Id);

            if (info.GetInfoResponse.IsUtxoIndexed != true)
                throw new PoolStartupException("UTXO index is disabled", poolConfig.Id);

            extraData = (string)info.GetInfoResponse.ServerVersion + (!string.IsNullOrEmpty(extraData) ? "." + extraData : "");
            break;
        }
        await stream.RequestStream.CompleteAsync();

        // Payment-processing setup
        if (clusterConfig.PaymentProcessing?.Enabled == true && poolConfig.PaymentProcessing?.Enabled == true)
        {
            var call = walletRpc.ShowAddressesAsync(new kaspaWalletd.ShowAddressesRequest(), null, null, ct);

            var walletAddresses = await Guard(() => call.ResponseAsync,
                ex => throw new PoolStartupException($"Error validating pool address '{ex.GetType().Name}' : {ex}", poolConfig.Id));
            call.Dispose();

            if (!walletAddresses.Address.Contains(poolConfig.Address))
                throw new PoolStartupException($"Pool address: {poolConfig.Address} is not controlled by pool wallet", poolConfig.Id);
        }

        await UpdateNetworkStatsAsync(ct);

        // periodic stats
        Observable.Interval(TimeSpan.FromMinutes(1))
            .Select(via => Observable.FromAsync(() =>
                Guard(() => UpdateNetworkStatsAsync(ct),
                    ex => logger.Error(ex))))
            .Concat()
            .Subscribe();

        SetupJobUpdates(ct);
    }

    public override void Configure(PoolConfig pc, ClusterConfig cc)
    {
        coin = pc.Template.As<KaspaCoinTemplate>();

        extraPoolConfig = pc.Extra.SafeExtensionDataAs<KaspaPoolConfigExtra>();
        extraPoolPaymentProcessingConfig = pc.PaymentProcessing.Extra.SafeExtensionDataAs<KaspaPaymentProcessingConfigExtra>();

        maxActiveJobs = extraPoolConfig?.MaxActiveJobs ?? 8;
        extraData = extraPoolConfig?.ExtraData ?? "Miningcore.developers[\"Cedric CRISPIN\"]";

        // endpoints daemon
        daemonEndpoints = pc.Daemons
            .Where(x => string.IsNullOrEmpty(x.Category))
            .ToArray();

        if (cc.PaymentProcessing?.Enabled == true && pc.PaymentProcessing?.Enabled == true)
        {
            // wallet daemon endpoints
            walletDaemonEndpoints = pc.Daemons
                .Where(x => x.Category?.ToLower() == KaspaConstants.WalletDaemonCategory)
                .ToArray();

            if (walletDaemonEndpoints.Length == 0)
                throw new PoolStartupException("Wallet-RPC daemon is not configured (Daemon configuration for kaspa-pools require an additional entry of category 'wallet' pointing to the wallet daemon)", pc.Id);
        }

        base.Configure(pc, cc);
    }

    protected override void ConfigureDaemons()
    {
        logger.Debug(() => $"ProtobufDaemonRpcServiceName: {extraPoolConfig?.ProtobufDaemonRpcServiceName ?? KaspaConstants.ProtobufDaemonRpcServiceName}");

        rpc = KaspaClientFactory.CreateKaspadRPCClient(daemonEndpoints, extraPoolConfig?.ProtobufDaemonRpcServiceName ?? KaspaConstants.ProtobufDaemonRpcServiceName);

        // Payment-processing setup
        if (clusterConfig.PaymentProcessing?.Enabled == true && poolConfig.PaymentProcessing?.Enabled == true)
        {
            logger.Debug(() => $"ProtobufWalletRpcServiceName: {extraPoolConfig?.ProtobufWalletRpcServiceName ?? KaspaConstants.ProtobufWalletRpcServiceName}");

            walletRpc = KaspaClientFactory.CreateKaspaWalletdRPCClient(walletDaemonEndpoints, extraPoolConfig?.ProtobufWalletRpcServiceName ?? KaspaConstants.ProtobufWalletRpcServiceName);
        }
    }

    protected override async Task<bool> AreDaemonsHealthyAsync(CancellationToken ct)
    {
        // Payment-processing
        if (clusterConfig.PaymentProcessing?.Enabled == true && poolConfig.PaymentProcessing?.Enabled == true)
        {
            var call = walletRpc.ShowAddressesAsync(new kaspaWalletd.ShowAddressesRequest(), null, null, ct);

            var walletAddresses = await Guard(() => call.ResponseAsync,
                ex => logger.Debug(ex));
            call.Dispose();

            if (walletAddresses == null)
                return false;
        }

        var stream = rpc.MessageStream(null, null, ct);

        var request = new kaspad.KaspadMessage();
        request.GetInfoRequest = new kaspad.GetInfoRequestMessage();
        await Guard(() => stream.RequestStream.WriteAsync(request),
            ex => logger.Debug(ex));
        bool areDaemonsHealthy = false;
        await foreach (var info in stream.ResponseStream.ReadAllAsync(ct))
        {
            if (!string.IsNullOrEmpty(info.GetInfoResponse.Error?.Message))
            {
                logger.Debug(info.GetInfoResponse.Error?.Message);
                return false;
            }

            if (info.GetInfoResponse.IsUtxoIndexed != true)
                throw new PoolStartupException("UTXO index is disabled", poolConfig.Id);

            if (info.GetInfoResponse.ServerVersion != null)
                BlockchainStats.NodeVersion = (string)info.GetInfoResponse.ServerVersion;

            areDaemonsHealthy = true;
            break;
        }
        await stream.RequestStream.CompleteAsync();

        return areDaemonsHealthy;
    }

    protected override async Task<bool> AreDaemonsConnectedAsync(CancellationToken ct)
    {
        // Payment-processing
        if (clusterConfig.PaymentProcessing?.Enabled == true && poolConfig.PaymentProcessing?.Enabled == true)
        {
            var call = walletRpc.ShowAddressesAsync(new kaspaWalletd.ShowAddressesRequest(), null, null, ct);

            var walletAddresses = await Guard(() => call.ResponseAsync,
                ex => logger.Debug(ex));
            call.Dispose();

            if (walletAddresses == null)
                return false;
        }

        var stream = rpc.MessageStream(null, null, ct);

        var request = new kaspad.KaspadMessage();
        request.GetConnectedPeerInfoRequest = new kaspad.GetConnectedPeerInfoRequestMessage();
        await Guard(() => stream.RequestStream.WriteAsync(request),
            ex => logger.Debug(ex));
        int totalPeers = 0;
        await foreach (var info in stream.ResponseStream.ReadAllAsync(ct))
        {
            if (!string.IsNullOrEmpty(info.GetConnectedPeerInfoResponse.Error?.Message))
            {
                logger.Debug(info.GetConnectedPeerInfoResponse.Error?.Message);
                return false;
            }
            else
                totalPeers = info.GetConnectedPeerInfoResponse.Infos.Count;

            break;
        }
        await stream.RequestStream.CompleteAsync();

        return totalPeers > 0;
    }

    protected override async Task EnsureDaemonsSynchedAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(5));

        var syncPendingNotificationShown = false;

        do
        {
            var isSynched = false;

            var stream = rpc.MessageStream(null, null, ct);

            var request = new kaspad.KaspadMessage();
            request.GetInfoRequest = new kaspad.GetInfoRequestMessage();
            await Guard(() => stream.RequestStream.WriteAsync(request),
                ex => logger.Debug(ex));
            await foreach (var info in stream.ResponseStream.ReadAllAsync(ct))
            {
                if (!string.IsNullOrEmpty(info.GetInfoResponse.Error?.Message))
                    logger.Debug(info.GetInfoResponse.Error?.Message);

                isSynched = (info.GetInfoResponse.IsSynced == true && info.GetInfoResponse.IsUtxoIndexed == true);
                break;
            }
            await stream.RequestStream.CompleteAsync();

            if (isSynched)
            {
                logger.Info(() => "Daemon is synced with blockchain");
                break;
            }

            if (!syncPendingNotificationShown)
            {
                logger.Info(() => "Daemon is still syncing with network. Manager will be started once synced.");
                syncPendingNotificationShown = true;
            }

            await ShowDaemonSyncProgressAsync(ct);
        } while (await timer.WaitForNextTickAsync(ct));
    }

    #endregion // Overrides
}
