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

using NodaTime;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Util;
using QuantConnect.Logging;
using QuantConnect.Securities;
using QuantConnect.Configuration;
using System.Collections.Concurrent;
using System.IO.Compression;
using System.Text;

namespace QuantConnect.Lean.DataSource.Polygon
{
    /// <summary>
    /// Data downloader class for pulling data from Polygon.io
    /// </summary>
    public class PolygonDataDownloader : IDataDownloader, IDisposable
    {
        /// <inheritdoc cref="PolygonDataProvider"/>
        private readonly PolygonDataProvider _historyProvider;

        /// <inheritdoc cref="MarketHoursDatabase" />
        private readonly MarketHoursDatabase _marketHoursDatabase;

        private readonly PolygonFlatFileClient _flatFileClient;
        private readonly PolygonSymbolMapper _symbolMapper;

        /// <summary>
        /// Tracks which date+underlying combos have had their minute data bulk-written to Lean format.
        /// Key format: "{underlying}_{yyyyMMdd}"
        /// </summary>
        private readonly ConcurrentDictionary<string, bool> _minuteDataWritten = new();

        /// <summary>
        /// Initializes a new instance of the <see cref="PolygonDataDownloader"/>
        /// </summary>
        /// <param name="apiKey">The Polygon.io API key</param>
        /// <param name="licenseType">The license type string retrieved from configuration (e.g., "Individual" or "Business").</param>
        public PolygonDataDownloader(string apiKey, string licenseType)
        {
            _historyProvider = new PolygonDataProvider(apiKey, false, licenseTypeFromConfig: licenseType);
            _marketHoursDatabase = MarketHoursDatabase.FromDataFolder();
            _flatFileClient = new PolygonFlatFileClient();
            _symbolMapper = new PolygonSymbolMapper();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PolygonDataDownloader"/>
        /// getting the Polygon.io API key from the configuration
        /// </summary>
        public PolygonDataDownloader()
            : this(Config.Get("polygon-api-key"), Config.Get("polygon-license-type"))
        {
        }

        /// <summary>
        /// Get historical data enumerable for a single symbol, type and resolution given this start and end time (in UTC).
        /// </summary>
        /// <param name="parameters">Parameters for the historical data request</param>
        /// <returns>Enumerable of base data for this symbol</returns>
        public IEnumerable<BaseData>? Get(DataDownloaderGetParameters parameters)
        {
            var symbol = parameters.Symbol;
            var resolution = parameters.Resolution;
            var startUtc = parameters.StartUtc;
            var endUtc = parameters.EndUtc;
            var tickType = parameters.TickType;

            // Polygon does not provide OpenInterest data for options. Return empty (not null) so
            // CanonicalDataDownloaderDecorator does not fall back to LiveOptionChainProvider and
            // make hundreds of API calls across the warmup date range.
            if (tickType == TickType.OpenInterest)
            {
                return Enumerable.Empty<BaseData>();
            }

            var dataType = LeanData.GetDataType(resolution, tickType);
            var exchangeHours = _marketHoursDatabase.GetExchangeHours(symbol.ID.Market, symbol, symbol.SecurityType);
            var dataTimeZone = _marketHoursDatabase.GetDataTimeZone(symbol.ID.Market, symbol, symbol.SecurityType);

            if (symbol.IsCanonical())
            {
                // Fast path: use flat files for canonical options with Trade tick type
                if (tickType == TickType.Trade)
                {
                    if (resolution == Resolution.Daily)
                    {
                        return GetCanonicalOptionHistoryFromFlatFiles(symbol, startUtc, endUtc);
                    }
                    if (resolution == Resolution.Minute)
                    {
                        return GetCanonicalOptionMinuteFromFlatFiles(symbol, startUtc, endUtc);
                    }
                }

                return GetCanonicalOptionHistory(symbol, startUtc, endUtc, dataType, resolution, exchangeHours, dataTimeZone, tickType);
            }
            else
            {
                // Fast path: bulk-write all contracts from flat file, then return requested contract's data
                if (tickType == TickType.Trade
                    && resolution == Resolution.Minute
                    && (symbol.SecurityType == SecurityType.Option || symbol.SecurityType == SecurityType.IndexOption))
                {
                    return GetOptionMinuteDataViaFlatFile(symbol, startUtc, endUtc, dataTimeZone);
                }

                // Fast path: equity tick data from S3 flat files (avoids REST API timestamp ordering bug)
                if (resolution == Resolution.Tick
                    && symbol.SecurityType == SecurityType.Equity
                    && (tickType == TickType.Quote || tickType == TickType.Trade))
                {
                    return GetEquityTickDataFromFlatFile(symbol, startUtc, endUtc, exchangeHours, tickType);
                }

                // Fast path: equity daily/minute trade data from S3 flat files
                if (tickType == TickType.Trade
                    && symbol.SecurityType == SecurityType.Equity
                    && (resolution == Resolution.Daily || resolution == Resolution.Minute))
                {
                    return GetEquityAggDataFromFlatFile(symbol, startUtc, endUtc, resolution);
                }

                var historyRequest = new HistoryRequest(startUtc, endUtc, dataType, symbol, resolution, exchangeHours, dataTimeZone, resolution,
                    true, false, DataNormalizationMode.Raw, tickType);

                var historyData = _historyProvider.GetHistory(historyRequest);

                if (historyData == null)
                {
                    return null;
                }

                return historyData;
            }
        }

        /// <summary>
        /// Returns daily data for all contracts from flat files (used for universe generation).
        /// </summary>
        private IEnumerable<BaseData> GetCanonicalOptionHistoryFromFlatFiles(Symbol symbol, DateTime startUtc, DateTime endUtc)
        {
            var underlying = symbol.Underlying?.Value ?? symbol.ID.Symbol;

            Log.Debug($"PolygonDataDownloader: Using flat files for canonical {underlying} daily data");

            foreach (var date in Time.EachDay(startUtc.Date, endUtc.Date))
            {
                var s3Key = PolygonFlatFileClient.GetDayAggsKey(date);

                using var stream = _flatFileClient.GetFlatFile(s3Key);
                if (stream == null)
                {
                    continue;
                }

                foreach (var bar in PolygonFlatFileParser.ParseAggs(stream, underlying, _symbolMapper, TimeSpan.FromDays(1)))
                {
                    yield return bar;
                }
            }
        }

        /// <summary>
        /// Returns minute data for all contracts from flat files (canonical symbol path).
        /// The DownloaderDataProvider/LeanDataWriter will write the data to the appropriate zip files.
        /// </summary>
        private IEnumerable<BaseData> GetCanonicalOptionMinuteFromFlatFiles(Symbol symbol, DateTime startUtc, DateTime endUtc)
        {
            var underlying = symbol.Underlying?.Value ?? symbol.ID.Symbol;
            var dataTimeZone = _marketHoursDatabase.GetDataTimeZone(symbol.ID.Market, symbol, symbol.SecurityType);

            Log.Debug($"PolygonDataDownloader: Using flat files for canonical {underlying} minute data");

            foreach (var date in Time.EachDay(startUtc.Date, endUtc.Date))
            {
                var s3Key = PolygonFlatFileClient.GetMinuteAggsKey(date);

                using var stream = _flatFileClient.GetFlatFile(s3Key);
                if (stream == null)
                {
                    continue;
                }

                foreach (var bar in PolygonFlatFileParser.ParseAggs(stream, underlying, _symbolMapper, TimeSpan.FromMinutes(1)))
                {
                    // Flat file timestamps are UTC; convert to data time zone so LEAN writes
                    // ms-from-midnight in local time rather than UTC.
                    yield return new TradeBar(
                        bar.Time.ConvertFromUtc(dataTimeZone),
                        bar.Symbol, bar.Open, bar.High, bar.Low, bar.Close,
                        bar.Volume, bar.Period);
                }
            }
        }

        /// <summary>
        /// Downloads a minute flat file, bulk-writes ALL contracts to Lean format zip files in the data directory,
        /// then returns data for the specifically requested contract.
        /// Subsequent calls for other contracts on the same date will find files already on disk.
        /// </summary>
        private IEnumerable<BaseData>? GetOptionMinuteDataViaFlatFile(Symbol symbol, DateTime startUtc, DateTime endUtc,
            DateTimeZone dataTimeZone)
        {
            var underlying = symbol.Underlying?.Value ?? symbol.ID.Symbol;
            var results = new List<TradeBar>();

            foreach (var date in Time.EachDay(startUtc.Date, endUtc.Date))
            {
                var dateKey = $"{underlying}_{date:yyyyMMdd}";

                if (_minuteDataWritten.ContainsKey(dateKey))
                {
                    // Already bulk-written this date. The caller (DownloaderDataProvider) should find
                    // the file on disk. But in case we're called directly, return empty and let the
                    // pipeline read from disk.
                    continue;
                }

                var s3Key = PolygonFlatFileClient.GetMinuteAggsKey(date);

                using var stream = _flatFileClient.GetFlatFile(s3Key);
                if (stream == null)
                {
                    continue;
                }

                var grouped = PolygonFlatFileParser.ParseAggsGrouped(stream, underlying, _symbolMapper, TimeSpan.FromMinutes(1));

                if (grouped.Count == 0)
                {
                    _minuteDataWritten.TryAdd(dateKey, true);
                    continue;
                }

                // Bulk-write ALL contracts to a single Lean zip file.
                // UTC→dataTimeZone conversion is done inside BulkWriteMinuteZip.
                BulkWriteMinuteZip(grouped, date, dataTimeZone);

                _minuteDataWritten.TryAdd(dateKey, true);

                // Extract the requested contract's data to return to the caller (in local time).
                if (grouped.TryGetValue(symbol, out var requestedBars))
                {
                    foreach (var bar in requestedBars)
                    {
                        results.Add(new TradeBar(
                            bar.Time.ConvertFromUtc(dataTimeZone),
                            bar.Symbol, bar.Open, bar.High, bar.Low, bar.Close,
                            bar.Volume, bar.Period));
                    }
                }

                Log.Debug($"PolygonDataDownloader: Bulk-wrote {grouped.Count} contracts for {underlying} on {date:yyyy-MM-dd}");
            }

            return results.Count > 0 ? results : null;
        }

        /// <summary>
        /// Downloads equity tick data (trades or quotes) from S3 flat files.
        /// Uses sip_timestamp which is pre-sorted in the file, avoiding the REST API ordering bug.
        /// </summary>
        private IEnumerable<BaseData> GetEquityTickDataFromFlatFile(Symbol symbol, DateTime startUtc, DateTime endUtc,
            SecurityExchangeHours exchangeHours, TickType tickType)
        {
            var ticker = symbol.Value;

            Log.Debug($"PolygonDataDownloader: Using flat files for {ticker} tick {tickType} data");

            foreach (var date in Time.EachDay(startUtc.Date, endUtc.Date))
            {
                var s3Key = tickType == TickType.Quote
                    ? PolygonFlatFileClient.GetStockQuotesKey(date)
                    : PolygonFlatFileClient.GetStockTradesKey(date);

                using var stream = _flatFileClient.GetFlatFile(s3Key);
                if (stream == null)
                {
                    continue;
                }

                var ticks = tickType == TickType.Quote
                    ? PolygonFlatFileParser.ParseStockQuotes(stream, symbol, ticker, exchangeHours.TimeZone)
                    : PolygonFlatFileParser.ParseStockTrades(stream, symbol, ticker, exchangeHours.TimeZone);

                foreach (var tick in ticks)
                {
                    yield return tick;
                }
            }
        }

        /// <summary>
        /// Downloads equity daily or minute trade data from S3 flat files.
        /// </summary>
        private IEnumerable<BaseData> GetEquityAggDataFromFlatFile(Symbol symbol, DateTime startUtc, DateTime endUtc,
            Resolution resolution)
        {
            var ticker = symbol.Value;
            var period = resolution == Resolution.Daily ? TimeSpan.FromDays(1) : TimeSpan.FromMinutes(1);

            Log.Debug($"PolygonDataDownloader: Using flat files for {ticker} {resolution} trade data");

            foreach (var date in Time.EachDay(startUtc.Date, endUtc.Date))
            {
                var s3Key = resolution == Resolution.Daily
                    ? PolygonFlatFileClient.GetStockDayAggsKey(date)
                    : PolygonFlatFileClient.GetStockMinuteAggsKey(date);

                using var stream = _flatFileClient.GetFlatFile(s3Key);
                if (stream == null)
                {
                    continue;
                }

                foreach (var bar in PolygonFlatFileParser.ParseAggs(stream, ticker, _symbolMapper, period, optionsOnly: false))
                {
                    yield return bar;
                }
            }
        }

        /// <summary>
        /// Writes all contracts' minute data into a single Lean-format zip file for the given date.
        /// Bar times from ParseAggsGrouped are in UTC; this method converts them to dataTimeZone
        /// before writing so LEAN can read them as milliseconds-from-midnight in local time.
        /// </summary>
        private static void BulkWriteMinuteZip(Dictionary<Symbol, List<TradeBar>> grouped, DateTime date, DateTimeZone dataTimeZone)
        {
            // All option contracts for the same underlying+date go into one zip file.
            // Use any contract symbol to derive the zip path (they all produce the same path).
            var anySymbol = grouped.Keys.First();
            var zipPath = LeanData.GenerateZipFilePath(Globals.DataFolder, anySymbol, date, Resolution.Minute, TickType.Trade);

            var dir = Path.GetDirectoryName(zipPath)!;
            Directory.CreateDirectory(dir);

            using var fs = new FileStream(zipPath, FileMode.Create, FileAccess.Write, FileShare.None);
            using var archive = new ZipArchive(fs, ZipArchiveMode.Create, leaveOpen: false);

            foreach (var (contractSymbol, bars) in grouped)
            {
                var entryName = LeanData.GenerateZipEntryName(contractSymbol, date, Resolution.Minute, TickType.Trade);

                var csv = new StringBuilder();
                foreach (var bar in bars)
                {
                    // Convert UTC bar time to local data time zone for LEAN's ms-from-midnight format.
                    var localBar = new TradeBar(
                        bar.Time.ConvertFromUtc(dataTimeZone),
                        bar.Symbol, bar.Open, bar.High, bar.Low, bar.Close,
                        bar.Volume, bar.Period);
                    csv.AppendLine(LeanData.GenerateLine(localBar, contractSymbol.ID.SecurityType, Resolution.Minute));
                }

                var zipEntry = archive.CreateEntry(entryName, CompressionLevel.Optimal);
                using var entryStream = zipEntry.Open();
                var bytes = Encoding.UTF8.GetBytes(csv.ToString().TrimEnd());
                entryStream.Write(bytes, 0, bytes.Length);
            }
        }

        private IEnumerable<BaseData>? GetCanonicalOptionHistory(Symbol symbol, DateTime startUtc, DateTime endUtc, Type dataType,
            Resolution resolution, SecurityExchangeHours exchangeHours, DateTimeZone dataTimeZone, TickType tickType)
        {
            var blockingOptionCollection = new BlockingCollection<BaseData>();
            var symbols = GetOptions(symbol, startUtc, endUtc);

            // Symbol can have a lot of Option parameters
            Task.Run(() => Parallel.ForEach(symbols, targetSymbol =>
            {
                var historyRequest = new HistoryRequest(startUtc, endUtc, dataType, targetSymbol, resolution, exchangeHours, dataTimeZone,
                    resolution, true, false, DataNormalizationMode.Raw, tickType);

                var history = _historyProvider.GetHistory(historyRequest);

                // If history is null, it indicates an incorrect or missing request for historical data,
                // so we skip processing for this symbol and move to the next one.
                if (history == null)
                {
                    return;
                }

                foreach (var data in history)
                {
                    blockingOptionCollection.Add(data);
                }
            })).ContinueWith(task =>
            {
                blockingOptionCollection.CompleteAdding();
                if (task.IsFaulted && task.Exception != null)
                {
                    var aggregateException = task.Exception.Flatten();
                    var errorMessages = string.Join("; ", aggregateException.InnerExceptions.Select(e => e.Message));
                    Log.Error($"{nameof(PolygonDataDownloader)}.{nameof(GetCanonicalOptionHistory)}: Task failed with error(s): {errorMessages}");
                }
            });

            var options = blockingOptionCollection.GetConsumingEnumerable();

            // Validate if the collection contains at least one successful response from history.
            if (!options.Any())
            {
                return null;
            }

            return options;
        }

        protected virtual IEnumerable<Symbol> GetOptions(Symbol symbol, DateTime startUtc, DateTime endUtc)
        {
            HashSet<Symbol> seenOptions = new();
            foreach (var date in Time.EachDay(startUtc.Date, endUtc.Date))
            {
                foreach (var option in _historyProvider.GetOptionChain(symbol, date))
                {
                    if (seenOptions.Add(option))
                    {
                        yield return option;
                    }
                }
            }
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose()
        {
            _historyProvider.DisposeSafely();
            _flatFileClient.Dispose();
        }
    }
}
