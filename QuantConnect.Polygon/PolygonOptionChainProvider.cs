/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using QuantConnect.Configuration;
using QuantConnect.Interfaces;
using QuantConnect.Logging;

namespace QuantConnect.Lean.DataSource.Polygon
{
    /// <summary>
    /// Polygon.io implementation of <see cref="IOptionChainProvider"/>
    /// </summary>
    /// <remarks>
    /// Reference: https://polygon.io/docs/options/get_v3_reference_options_contracts
    /// </remarks>
    public class PolygonOptionChainProvider : IOptionChainProvider
    {
        private PolygonRestApiClient _restApiClient;
        private PolygonSymbolMapper _symbolMapper;
        private PolygonFlatFileClient? _flatFileClient;

        private bool _unsupportedSecurityTypeLogSent;
        private bool _flatFileChainLogSent;

        /// <summary>
        /// Initializes a new instance of the <see cref="PolygonOptionChainProvider"/> class
        /// using the API key from configuration.
        /// </summary>
        public PolygonOptionChainProvider()
        {
            var apiKey = Config.Get("polygon-api-key");
            if (string.IsNullOrWhiteSpace(apiKey))
            {
                throw new ArgumentException("PolygonOptionChainProvider requires 'polygon-api-key' to be configured.");
            }
            _restApiClient = new PolygonRestApiClient(apiKey);
            _symbolMapper = new PolygonSymbolMapper();
            _flatFileClient = new PolygonFlatFileClient();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PolygonOptionChainProvider"/> class
        /// </summary>
        /// <param name="restApiClient">The Polygon REST API client</param>
        /// <param name="symbolMapper">The Polygon symbol mapper</param>
        public PolygonOptionChainProvider(PolygonRestApiClient restApiClient, PolygonSymbolMapper symbolMapper)
        {
            _restApiClient = restApiClient;
            _symbolMapper = symbolMapper;
            _flatFileClient = new PolygonFlatFileClient();
        }

        /// <summary>
        /// Gets the list of option contracts for a given underlying symbol.
        /// Uses S3 flat files when configured, falls back to REST API.
        /// </summary>
        /// <param name="symbol">The option or the underlying symbol to get the option chain for.
        /// Providing the option allows targeting an option ticker different than the default e.g. SPXW</param>
        /// <param name="date">The date for which to request the option chain (only used in backtesting)</param>
        /// <returns>The list of option contracts</returns>
        public IEnumerable<Symbol> GetOptionContractList(Symbol symbol, DateTime date)
        {
            // Only equity and index options are supported
            if (symbol.SecurityType == SecurityType.Future || symbol.SecurityType == SecurityType.FutureOption)
            {
                if (!_unsupportedSecurityTypeLogSent)
                {
                    Log.Trace($"PolygonOptionChainProvider.GetOptionContractList(): Unsupported security type {symbol.SecurityType}");
                    _unsupportedSecurityTypeLogSent = true;
                }
                yield break;
            }

            var underlying = symbol.SecurityType.IsOption() ? symbol.Underlying : symbol;

            // Try flat files first
            if (_flatFileClient != null && _flatFileClient.IsConfigured)
            {
                var flatFileSymbols = GetOptionContractListFromFlatFiles(underlying, date);
                if (flatFileSymbols != null)
                {
                    foreach (var s in flatFileSymbols)
                    {
                        yield return s;
                    }
                    yield break;
                }
            }

            // Fall back to REST API
            var optionsSecurityType = underlying.SecurityType == SecurityType.Index ? SecurityType.IndexOption : SecurityType.Option;

            var resource = "v3/reference/options/contracts";
            var parameters = new Dictionary<string, string>
            {
                ["underlying_ticker"] = underlying.Value,
                ["as_of"] = date.ToStringInvariant("yyyy-MM-dd"),
                ["expired"] = "false",
                ["limit"] = "1000"
            };

            foreach (var contract in _restApiClient.DownloadAndParseData<OptionChainResponse>(resource, parameters)
                                                  .SelectMany(response => response.Results))
            {
                // Unsupported option style (e.g. bermudan) or right (e.g. "other" in rare cases according to the endpoint's docs)
                if (!Enum.TryParse<OptionStyle>(contract.Style, ignoreCase: true, out var optionStyle) ||
                    !Enum.TryParse<OptionRight>(contract.Right, ignoreCase: true, out var optionRight))
                {
                    continue;
                }

                var contractSymbol = _symbolMapper.GetLeanSymbol(contract.Ticker, optionsSecurityType, underlying.ID.Market, optionStyle,
                    contract.ExpirationDate, contract.StrikePrice, optionRight, underlying);
                yield return contractSymbol;
            }
        }

        /// <summary>
        /// Derives the option chain from a day_aggs flat file.
        /// Any option ticker present in the file for the given underlying is a valid contract.
        /// Returns null if the flat file is unavailable (caller should fall back to REST).
        /// </summary>
        private List<Symbol>? GetOptionContractListFromFlatFiles(Symbol underlying, DateTime date)
        {
            var s3Key = PolygonFlatFileClient.GetDayAggsKey(date);
            using var stream = _flatFileClient!.GetFlatFile(s3Key);
            if (stream == null)
            {
                return null;
            }

            if (!_flatFileChainLogSent)
            {
                Log.Trace($"PolygonOptionChainProvider: Using flat files for option chain discovery");
                _flatFileChainLogSent = true;
            }

            var prefix = $"O:{underlying.Value}";
            var symbols = new List<Symbol>();
            using var reader = new StreamReader(stream);

            // Skip header
            reader.ReadLine();

            string? line;
            while ((line = reader.ReadLine()) != null)
            {
                // ticker is the first column
                var commaIdx = line.IndexOf(',');
                if (commaIdx <= 0) continue;

                var ticker = line.Substring(0, commaIdx);

                if (!ticker.StartsWith(prefix)) continue;

                // Ensure "SPY" doesn't match "SPYG" — next char must be a digit
                if (ticker.Length > prefix.Length && !char.IsDigit(ticker[prefix.Length])) continue;

                try
                {
                    var contractSymbol = _symbolMapper.GetLeanSymbol(ticker);
                    symbols.Add(contractSymbol);
                }
                catch (Exception ex)
                {
                    Log.Debug($"PolygonOptionChainProvider: Could not parse ticker {ticker}: {ex.Message}");
                }
            }

            Log.Debug($"PolygonOptionChainProvider: Found {symbols.Count} contracts for {underlying.Value} on {date:yyyy-MM-dd} from flat file");
            return symbols;
        }
    }
}
