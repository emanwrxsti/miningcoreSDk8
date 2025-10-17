using System;
using System.Diagnostics;
using NBitcoin;
using NBitcoin.DataEncoders;

namespace Miningcore.Blockchain.Bitcoin
{
    public static class BitcoinUtils
    {
        /// <summary>
        /// Converts any valid address from the <paramref name="expectedNetwork"/> into an IDestination.
        /// Supports Base58 (P2PKH/P2SH) and Bech32/Bech32m (P2WPKH, P2WSH, Taproot v1, etc).
        /// </summary>
        public static IDestination AddressToDestination(string address, Network expectedNetwork)
        {
            if (string.IsNullOrWhiteSpace(address))
                throw new ArgumentException("address is null/empty", nameof(address));
            if (expectedNetwork == null)
                throw new ArgumentNullException(nameof(expectedNetwork));

            // Usa o parser oficial do NBitcoin (lida com base58 + bech32/bech32m)
            var addr = BitcoinAddress.Create(address, expectedNetwork);

            // Extrai o destino a partir do ScriptPubKey
            var dest = addr.ScriptPubKey.GetDestination(expectedNetwork);
            if (dest == null)
                throw new FormatException($"Unable to derive destination from address '{address}' for network '{expectedNetwork.Name}'.");

            return dest;
        }

        /// <summary>
        /// Converts a Bech32 (or Bech32m) address to IDestination.
        /// If <paramref name="bechPrefix"/> is provided (for altchains with custom HRP),
        /// attempts to decode with that HRP when the default parser fails.
        /// </summary>
        public static IDestination BechSegwitAddressToDestination(string address, Network expectedNetwork, string bechPrefix = null)
        {
            if (string.IsNullOrWhiteSpace(address))
                throw new ArgumentException("address is null/empty", nameof(address));
            if (expectedNetwork == null)
                throw new ArgumentNullException(nameof(expectedNetwork));

            // 1) First try via single parser (covers v0/v1, p2wpkh/p2wsh/taproot, etc.)
            try
            {
                return AddressToDestination(address, expectedNetwork);
            }
            catch
            {
                // 2) Fallback: HRP custom (some chains change the HRP and NBitcoin's Network may not match)
                if (string.IsNullOrWhiteSpace(bechPrefix))
                    throw; // no custom HRP, nothing we can do

                var encoder = Encoders.Bech32(bechPrefix);
                var prog = encoder.Decode(address, out var witVersion);

                // v0 + 20 bytes -> P2WPKH
                if (witVersion == 0 && prog?.Length == 20)
                    return new WitKeyId(prog);

                // v0 + 32 bytes -> P2WSH
                if (witVersion == 0 && prog?.Length == 32)
                    return new WitScriptId(prog);

                // v1 + 32 bytes -> Taproot (If your NBitcoin version has Taproot in IDestination,
                // AddressToDestination already handled it. With custom HRP, we don't force a specific type.)

                // If you REALLY need to support Taproot with custom HRP, consider aligning the Network/HRP.

                throw new FormatException($"Unsupported bech32 witness (v={witVersion}, len={prog?.Length}) for '{address}' (hrp='{bechPrefix}').");
            }
        }

        /// <summary>
        /// Converts a Bitcoin Cash address to IDestination using the altcoin's Network.
        /// </summary>
        public static IDestination BCashAddressToDestination(string address, Network expectedNetwork)
        {
            if (string.IsNullOrWhiteSpace(address))
                throw new ArgumentException("address is null/empty", nameof(address));
            if (expectedNetwork == null)
                throw new ArgumentNullException(nameof(expectedNetwork));

            var bcash = NBitcoin.Altcoins.BCash.Instance.GetNetwork(expectedNetwork.ChainName);
            if (bcash == null)
                throw new ArgumentException($"Unable to resolve BCash network for chain '{expectedNetwork.ChainName}'.");

            var anyAddr = NBitcoin.Altcoins.BCash.Instance.Networks.Main // type is resolved via Parse<T>, but we can use Create
                .Parse<BitcoinAddress>(address, bcash);

            var dest = anyAddr.ScriptPubKey.GetDestination(bcash);
            if (dest == null)
                throw new FormatException($"Unable to derive destination from BCash address '{address}'.");

            return dest;
        }

        /// <summary>
        /// Converts a Litecoin address to IDestination using the altcoin's Network.
        /// Supports base58 and bech32 (ltc...).
        /// </summary>
        public static IDestination LitecoinAddressToDestination(string address, Network expectedNetwork)
        {
            if (string.IsNullOrWhiteSpace(address))
                throw new ArgumentException("address is null/empty", nameof(address));
            if (expectedNetwork == null)
                throw new ArgumentNullException(nameof(expectedNetwork));

            var ltc = NBitcoin.Altcoins.Litecoin.Instance.GetNetwork(expectedNetwork.ChainName);
            if (ltc == null)
                throw new ArgumentException($"Unable to resolve Litecoin network for chain '{expectedNetwork.ChainName}'.");

            var addr = BitcoinAddress.Create(address, ltc);
            var dest = addr.ScriptPubKey.GetDestination(ltc);
            if (dest == null)
                throw new FormatException($"Unable to derive destination from Litecoin address '{address}'.");

            // optional sanity check
            Debug.Assert(addr.ToString() == address);
            return dest;
        }
    }
}
