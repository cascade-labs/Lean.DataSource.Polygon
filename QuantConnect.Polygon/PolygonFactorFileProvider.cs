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

using System.Collections.Concurrent;
using System.Globalization;
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Data.Auxiliary;
using QuantConnect.Data.Market;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Securities;
using QuantConnect.Util;

namespace QuantConnect.Lean.DataSource.Polygon
{
    /// <summary>
    /// Factor file provider that fetches corporate actions from Polygon API
    /// and generates LEAN-compatible factor files on demand
    /// </summary>
    public class PolygonFactorFileProvider : IFactorFileProvider
    {
        private IMapFileProvider? _mapFileProvider;
        private IDataProvider? _dataProvider;
        private PolygonRestApiClient? _restClient;
        private LocalDiskFactorFileProvider? _localProvider;
        private readonly ConcurrentDictionary<Symbol, object> _generationLocks;
        private readonly object _initLock = new();
        private bool _initialized;

        /// <summary>
        /// Default start date for fetching corporate actions
        /// </summary>
        private static readonly DateTime DefaultStartDate = new DateTime(2000, 1, 1);

        public PolygonFactorFileProvider()
        {
            _generationLocks = new ConcurrentDictionary<Symbol, object>();
        }

        /// <summary>
        /// Initializes the provider with map file provider and data provider
        /// </summary>
        public void Initialize(IMapFileProvider mapFileProvider, IDataProvider dataProvider)
        {
            lock (_initLock)
            {
                if (_initialized) return;

                _mapFileProvider = mapFileProvider;
                _dataProvider = dataProvider;

                // Initialize the local provider for reading existing factor files
                _localProvider = new LocalDiskFactorFileProvider();
                _localProvider.Initialize(mapFileProvider, dataProvider);

                // Initialize REST client
                var apiKey = Config.Get("polygon-api-key");
                if (string.IsNullOrEmpty(apiKey))
                {
                    throw new InvalidOperationException("PolygonFactorFileProvider requires 'polygon-api-key' to be configured");
                }
                _restClient = new PolygonRestApiClient(apiKey);

                _initialized = true;
                Log.Trace("PolygonFactorFileProvider: Initialized");
            }
        }

        /// <summary>
        /// Gets a factor file for the specified symbol.
        /// If a factor file exists on disk and is up-to-date, uses that. Otherwise fetches from Polygon API.
        /// </summary>
        public IFactorProvider? Get(Symbol symbol)
        {
            if (!_initialized)
            {
                throw new InvalidOperationException("PolygonFactorFileProvider has not been initialized");
            }

            // Only support equities
            if (symbol.SecurityType != SecurityType.Equity)
            {
                return null;
            }

            var factorSymbol = symbol.GetFactorFileSymbol();
            var factorFilePath = GetFactorFilePath(symbol);

            if (File.Exists(factorFilePath))
            {
                var lastEntryDate = GetLastEntryDate(factorFilePath);
                var today = DateTime.UtcNow.Date;

                // If file is up-to-date (sentinel is today or yesterday), use it without API calls
                if (lastEntryDate >= today.AddDays(-1))
                {
                    Log.Debug($"PolygonFactorFileProvider: Using cached factor file for {symbol.Value} (valid through {lastEntryDate:yyyyMMdd})");
                    return _localProvider!.Get(symbol);
                }

                // File exists but sentinel is old - do incremental refresh
                return RefreshFactorFile(symbol, lastEntryDate);
            }

            // File doesn't exist - need to generate from scratch
            var symbolLock = _generationLocks.GetOrAdd(factorSymbol, _ => new object());
            lock (symbolLock)
            {
                // Double-check: another thread may have generated while we waited
                if (File.Exists(factorFilePath))
                {
                    return _localProvider!.Get(symbol);
                }

                // Generate from Polygon API
                Log.Trace($"PolygonFactorFileProvider: Generating factor file for {symbol.Value} from Polygon API");
                return GenerateFactorFile(symbol);
            }
        }

        /// <summary>
        /// Reads the last entry date (sentinel date) from a factor file.
        /// This is the date in the last line of the file, which indicates when the file was last verified.
        /// </summary>
        private static DateTime GetLastEntryDate(string factorFilePath)
        {
            var lastLine = File.ReadLines(factorFilePath).LastOrDefault();
            if (string.IsNullOrEmpty(lastLine))
            {
                return DateTime.MinValue;
            }

            var parts = lastLine.Split(',');
            if (parts.Length > 0 && DateTime.TryParseExact(parts[0], "yyyyMMdd",
                CultureInfo.InvariantCulture, DateTimeStyles.None, out var date))
            {
                return date;
            }

            return DateTime.MinValue;
        }

        /// <summary>
        /// Performs an incremental refresh of a factor file by checking for new corporate actions
        /// since the last entry date. Only queries the delta period instead of the full history.
        /// </summary>
        private IFactorProvider? RefreshFactorFile(Symbol symbol, DateTime lastEntryDate)
        {
            var ticker = symbol.Value;
            var startDate = lastEntryDate.AddDays(1);
            var endDate = DateTime.UtcNow.Date;

            var factorSymbol = symbol.GetFactorFileSymbol();
            var symbolLock = _generationLocks.GetOrAdd(factorSymbol, _ => new object());

            lock (symbolLock)
            {
                // Double-check after acquiring lock - another thread may have refreshed
                var currentLastDate = GetLastEntryDate(GetFactorFilePath(symbol));
                if (currentLastDate >= endDate.AddDays(-1))
                {
                    return _localProvider!.Get(symbol);
                }

                Log.Debug($"PolygonFactorFileProvider: Checking for new corporate actions for {ticker} from {startDate:yyyyMMdd} to {endDate:yyyyMMdd}");

                try
                {
                    // Only query the delta period (not full history)
                    var newSplits = FetchSplits(ticker, startDate, endDate);
                    var newDividends = FetchDividends(ticker, startDate, endDate);

                    if (newSplits.Count == 0 && newDividends.Count == 0)
                    {
                        // No new corporate actions - just update sentinel date
                        UpdateFactorFileSentinelDate(symbol, endDate);
                        return _localProvider!.Get(symbol);
                    }

                    // New corporate actions found - regenerate full file
                    Log.Trace($"PolygonFactorFileProvider: Found new corporate actions for {ticker}, regenerating factor file");
                    return GenerateFactorFile(symbol);
                }
                catch (Exception ex)
                {
                    Log.Error($"PolygonFactorFileProvider: Error refreshing factor file for {ticker}: {ex.Message}");
                    // Return existing file on error
                    return _localProvider!.Get(symbol);
                }
            }
        }

        /// <summary>
        /// Updates only the sentinel date in an existing factor file without regenerating.
        /// This marks the file as verified through the new date.
        /// </summary>
        private void UpdateFactorFileSentinelDate(Symbol symbol, DateTime newDate)
        {
            var filePath = GetFactorFilePath(symbol);
            var lines = File.ReadAllLines(filePath).ToList();

            if (lines.Count > 0)
            {
                // Parse last line, update date, keep factors
                var lastLine = lines[lines.Count - 1];
                var parts = lastLine.Split(',');
                if (parts.Length >= 4)
                {
                    lines[lines.Count - 1] = $"{newDate:yyyyMMdd},{parts[1]},{parts[2]},{parts[3]}";
                    File.WriteAllLines(filePath, lines);
                    Log.Debug($"PolygonFactorFileProvider: Updated sentinel to {newDate:yyyyMMdd} for {symbol.Value}");
                }
            }
        }

        /// <summary>
        /// Generates a factor file by fetching corporate actions and daily data from Polygon
        /// </summary>
        private CorporateFactorProvider GenerateFactorFile(Symbol symbol)
        {
            var ticker = symbol.Value;
            var startDate = DefaultStartDate;
            var endDate = DateTime.UtcNow.Date;

            try
            {
                // Fetch splits from Polygon
                var splits = FetchSplits(ticker, startDate, endDate);
                Log.Debug($"PolygonFactorFileProvider: Found {splits.Count} splits for {ticker}");

                // Fetch dividends from Polygon
                var dividends = FetchDividends(ticker, startDate, endDate);
                Log.Debug($"PolygonFactorFileProvider: Found {dividends.Count} dividends for {ticker}");

                // If no corporate actions, return a minimal factor file
                if (splits.Count == 0 && dividends.Count == 0)
                {
                    var minimalFactorFile = CreateMinimalFactorFile(symbol);
                    WriteFactorFile(symbol, minimalFactorFile);
                    return minimalFactorFile;
                }

                // Fetch daily price data for reference prices
                var dailyData = FetchDailyData(ticker, startDate, endDate);
                Log.Debug($"PolygonFactorFileProvider: Found {dailyData.Count} daily bars for {ticker}");

                if (dailyData.Count == 0)
                {
                    Log.Error($"PolygonFactorFileProvider: No daily data available for {ticker}, cannot generate factor file with corporate actions");
                    var minimalFactorFile = CreateMinimalFactorFile(symbol);
                    WriteFactorFile(symbol, minimalFactorFile);
                    return minimalFactorFile;
                }

                // Convert to LEAN types
                var corporateActions = ConvertToCorporateActions(symbol, splits, dividends, dailyData);

                // Get exchange hours for factor calculation
                var exchangeHours = MarketHoursDatabase.FromDataFolder()
                    .GetExchangeHours(symbol.ID.Market, symbol, symbol.SecurityType);

                // Generate factor file using CorporateFactorProvider.Apply
                var factorFile = GenerateFactorFileFromCorporateActions(symbol, corporateActions, dailyData, exchangeHours);

                // Write to disk
                WriteFactorFile(symbol, factorFile);

                return factorFile;
            }
            catch (Exception ex)
            {
                Log.Error($"PolygonFactorFileProvider: Error generating factor file for {ticker}: {ex.Message}");
                return CreateMinimalFactorFile(symbol);
            }
        }

        /// <summary>
        /// Fetches and deduplicates splits from Polygon API
        /// </summary>
        private List<PolygonSplit> FetchSplits(string ticker, DateTime startDate, DateTime endDate)
        {
            var resource = "v3/reference/splits";
            var parameters = new Dictionary<string, string>
            {
                ["ticker"] = ticker,
                ["execution_date.gte"] = startDate.ToString("yyyy-MM-dd"),
                ["execution_date.lte"] = endDate.ToString("yyyy-MM-dd"),
                ["order"] = "asc",
                ["limit"] = "1000"
            };

            var allSplits = new List<PolygonSplit>();
            try
            {
                foreach (var response in _restClient!.DownloadAndParseData<SplitResponse>(resource, parameters))
                {
                    allSplits.AddRange(response.Results);
                }
            }
            catch (Exception ex)
            {
                Log.Error($"PolygonFactorFileProvider: Error fetching splits for {ticker}: {ex.Message}");
            }

            // Deduplicate by execution date
            var uniqueSplits = allSplits
                .Where(s => s.GetExecutionDate() != DateTime.MinValue)
                .GroupBy(s => s.GetExecutionDate())
                .Select(g => g.First())
                .OrderBy(s => s.GetExecutionDate())
                .ToList();

            return uniqueSplits;
        }

        /// <summary>
        /// Fetches and deduplicates dividends from Polygon API
        /// </summary>
        private List<PolygonDividend> FetchDividends(string ticker, DateTime startDate, DateTime endDate)
        {
            var resource = "v3/reference/dividends";
            var parameters = new Dictionary<string, string>
            {
                ["ticker"] = ticker,
                ["ex_dividend_date.gte"] = startDate.ToString("yyyy-MM-dd"),
                ["ex_dividend_date.lte"] = endDate.ToString("yyyy-MM-dd"),
                ["order"] = "asc",
                ["limit"] = "1000"
            };

            var allDividends = new List<PolygonDividend>();
            try
            {
                foreach (var response in _restClient!.DownloadAndParseData<DividendResponse>(resource, parameters))
                {
                    allDividends.AddRange(response.Results);
                }
            }
            catch (Exception ex)
            {
                Log.Error($"PolygonFactorFileProvider: Error fetching dividends for {ticker}: {ex.Message}");
            }

            // Deduplicate by ex-dividend date and filter to cash dividends only
            var uniqueDividends = allDividends
                .Where(d => d.GetExDividendDate() != DateTime.MinValue)
                .Where(d => d.DividendType == "CD" || d.DividendType == "SC") // Cash or Special Cash dividends
                .GroupBy(d => d.GetExDividendDate())
                .Select(g => g.First())
                .OrderBy(d => d.GetExDividendDate())
                .ToList();

            return uniqueDividends;
        }

        /// <summary>
        /// Fetches daily price data from Polygon API
        /// </summary>
        private Dictionary<DateTime, decimal> FetchDailyData(string ticker, DateTime startDate, DateTime endDate)
        {
            var resource = $"v2/aggs/ticker/{ticker}/range/1/day/{startDate:yyyy-MM-dd}/{endDate:yyyy-MM-dd}";
            var parameters = new Dictionary<string, string>
            {
                ["adjusted"] = "false", // We need raw prices for reference
                ["sort"] = "desc"
            };

            var closeLookup = new Dictionary<DateTime, decimal>();
            try
            {
                foreach (var response in _restClient!.DownloadAndParseData<AggregatesResponse>(resource, parameters))
                {
                    foreach (var bar in response.Results)
                    {
                        var date = Time.UnixMillisecondTimeStampToDateTime(bar.Timestamp).Date;
                        if (bar.Close > 0 && !closeLookup.ContainsKey(date))
                        {
                            closeLookup[date] = bar.Close;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error($"PolygonFactorFileProvider: Error fetching daily data for {ticker}: {ex.Message}");
            }

            return closeLookup;
        }

        /// <summary>
        /// Converts Polygon responses to LEAN Dividend and Split objects
        /// </summary>
        private List<BaseData> ConvertToCorporateActions(
            Symbol symbol,
            List<PolygonSplit> splits,
            List<PolygonDividend> dividends,
            Dictionary<DateTime, decimal> closeLookup)
        {
            var corporateActions = new List<BaseData>();

            // Convert splits
            foreach (var split in splits)
            {
                var splitDate = split.GetExecutionDate();
                // Find reference price (previous day's close)
                var referencePrice = FindReferencePrice(splitDate, closeLookup);

                if (referencePrice > 0 && split.SplitFactor != 0)
                {
                    // Polygon's SplitFactor = SplitFrom/SplitTo = old_shares/new_shares
                    // LEAN's Split.SplitFactor uses the same convention (old_shares/new_shares)
                    // e.g. 4:1 forward split = 0.25, 10:1 reverse split = 10
                    corporateActions.Add(new Split(
                        symbol,
                        splitDate,
                        referencePrice,
                        split.SplitFactor,
                        SplitType.SplitOccurred
                    ));
                }
            }

            // Convert dividends
            foreach (var dividend in dividends)
            {
                var exDate = dividend.GetExDividendDate();
                // Find reference price (previous day's close)
                var referencePrice = FindReferencePrice(exDate, closeLookup);

                if (referencePrice > 0 && dividend.CashAmount > 0)
                {
                    corporateActions.Add(new Dividend(
                        symbol,
                        exDate,
                        dividend.CashAmount,
                        referencePrice
                    ));
                }
            }

            return corporateActions.OrderBy(c => c.Time).ToList();
        }

        /// <summary>
        /// Finds the reference price (close from the trading day before the event)
        /// </summary>
        private decimal FindReferencePrice(DateTime eventDate, Dictionary<DateTime, decimal> closeLookup)
        {
            // Look for close prices in the days before the event
            for (int i = 1; i <= 5; i++)
            {
                var lookupDate = eventDate.AddDays(-i).Date;
                if (closeLookup.TryGetValue(lookupDate, out var close))
                {
                    return close;
                }
            }

            return 0m;
        }

        /// <summary>
        /// Generates factor file from corporate actions using CorporateFactorProvider.Apply
        /// </summary>
        private CorporateFactorProvider GenerateFactorFileFromCorporateActions(
            Symbol symbol,
            List<BaseData> corporateActions,
            Dictionary<DateTime, decimal> dailyData,
            SecurityExchangeHours exchangeHours)
        {
            // Create initial factor file with sentinel row using today's date
            // This marks when the factor file was verified, enabling incremental updates
            var initialRows = new List<CorporateFactorRow>
            {
                new CorporateFactorRow(DateTime.UtcNow.Date, 1m, 1m, 0m)
            };

            // Get earliest data date for the first factor file row
            var earliestDate = dailyData.Count > 0
                ? dailyData.Keys.Min().Date
                : DefaultStartDate;

            // Add sentinel row for earliest date
            initialRows.Add(new CorporateFactorRow(earliestDate, 1m, 1m, 0m));

            var factorFile = new CorporateFactorProvider(symbol.Value, initialRows);

            // Apply corporate actions if any
            if (corporateActions.Count > 0)
            {
                factorFile = factorFile.Apply(corporateActions, exchangeHours);
            }

            return factorFile;
        }

        /// <summary>
        /// Creates a minimal factor file with no adjustments (all factors = 1)
        /// </summary>
        private CorporateFactorProvider CreateMinimalFactorFile(Symbol symbol)
        {
            // Use today's date as sentinel to mark when the file was verified
            var rows = new List<CorporateFactorRow>
            {
                new CorporateFactorRow(DateTime.UtcNow.Date, 1m, 1m, 0m),
                new CorporateFactorRow(DefaultStartDate, 1m, 1m, 0m)
            };

            return new CorporateFactorProvider(symbol.Value, rows);
        }

        /// <summary>
        /// Writes the factor file to the standard LEAN data directory
        /// </summary>
        private void WriteFactorFile(Symbol symbol, CorporateFactorProvider factorFile)
        {
            var filePath = GetFactorFilePath(symbol);
            var directory = Path.GetDirectoryName(filePath);

            if (!Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory!);
            }

            File.WriteAllLines(filePath, factorFile.GetFileFormat());
            Log.Trace($"PolygonFactorFileProvider: Wrote factor file to {filePath}");
        }

        /// <summary>
        /// Gets the path where factor file should be stored
        /// </summary>
        private static string GetFactorFilePath(Symbol symbol)
        {
            return LeanData.GenerateRelativeFactorFilePath(symbol);
        }
    }
}
