/*
 * CASCADELABS.IO
 * Cascade Labs LLC
 */

using System.Collections.Concurrent;
using Newtonsoft.Json;
using QuantConnect.Configuration;
using QuantConnect.Logging;

namespace QuantConnect.Lean.DataSource.Polygon
{
    /// <summary>
    /// Fetches, caches, and serves Polygon financial statement data per ticker.
    /// Thread-safe with per-ticker download-once semantics.
    /// </summary>
    public class PolygonFinancialDataService
    {
        private readonly PolygonRestApiClient _restClient;
        private readonly bool _liveMode;

        /// <summary>
        /// In-memory cache of financial results per ticker (sorted by filing date ascending)
        /// </summary>
        private readonly ConcurrentDictionary<string, List<PolygonFinancialResult>> _cache = new();

        /// <summary>
        /// Per-ticker locks for download-once semantics
        /// </summary>
        private readonly ConcurrentDictionary<string, object> _downloadLocks = new();

        /// <summary>
        /// Tracks which tickers have been loaded (to distinguish "loaded but empty" from "not yet loaded")
        /// </summary>
        private readonly ConcurrentDictionary<string, DateTime> _loadedAt = new();

        /// <summary>
        /// How long to cache financial data in live mode before refreshing (default 24 hours)
        /// </summary>
        private readonly int _cacheHours;

        /// <summary>
        /// Directory where per-ticker JSON cache files are stored
        /// </summary>
        private readonly string _cacheDirectory;

        public PolygonFinancialDataService(PolygonRestApiClient restClient, bool liveMode)
        {
            _restClient = restClient;
            _liveMode = liveMode;
            _cacheHours = Config.GetInt("polygon-financials-cache-hours", 24);
            _cacheDirectory = Path.Combine(Globals.DataFolder, "equity", "usa", "fundamental", "fine", "polygon");
        }

        /// <summary>
        /// Gets a financial value for a ticker at a given date and property.
        /// Uses point-in-time semantics: only filings with filing_date &lt;= date are considered.
        /// </summary>
        public double GetFinancialValue(string ticker, DateTime date, string propertyName)
        {
            if (propertyName == "CompanyProfile_MarketCap")
            {
                // MarketCap not available from financials API; would need snapshot API
                return double.NaN;
            }

            var parsed = PolygonFinancialPropertyMap.ParsePropertyName(propertyName);
            if (!parsed.HasValue)
            {
                return double.NaN;
            }

            var info = parsed.Value;
            EnsureLoaded(ticker);

            if (!_cache.TryGetValue(ticker, out var filings) || filings.Count == 0)
            {
                return double.NaN;
            }

            return info.Period switch
            {
                "ThreeMonths" => GetQuarterlyValue(filings, date, info),
                "TwelveMonths" => GetTTMValue(filings, date, info),
                _ => double.NaN // OneMonth, TwoMonths, SixMonths, NineMonths not supported
            };
        }

        /// <summary>
        /// Returns true if we have any financial data for this ticker
        /// </summary>
        public bool HasFinancialData(string ticker)
        {
            EnsureLoaded(ticker);
            return _cache.TryGetValue(ticker, out var filings) && filings.Count > 0;
        }

        /// <summary>
        /// Gets the most recent quarterly filing value using point-in-time lookup
        /// </summary>
        private double GetQuarterlyValue(List<PolygonFinancialResult> filings, DateTime date, PolygonFinancialPropertyMap.PropertyInfo info)
        {
            var filing = FindMostRecentQuarterlyFiling(filings, date);
            if (filing == null)
            {
                return double.NaN;
            }

            if (info.Statement == PolygonFinancialPropertyMap.StatementType.Computed)
            {
                return PolygonFinancialPropertyMap.ComputeFreeCashFlow(filing);
            }

            return PolygonFinancialPropertyMap.GetFieldValue(filing, info.PolygonField, info.Statement);
        }

        /// <summary>
        /// Gets the trailing twelve months value.
        /// For flow items (income/cash flow): sum of last 4 quarterly filings.
        /// For stock items (balance sheet): most recent quarterly filing value.
        /// </summary>
        private double GetTTMValue(List<PolygonFinancialResult> filings, DateTime date, PolygonFinancialPropertyMap.PropertyInfo info)
        {
            if (PolygonFinancialPropertyMap.IsStockItem(info.Statement))
            {
                // Balance sheet items: just use most recent value
                return GetQuarterlyValue(filings, date, info);
            }

            // Flow items: sum last 4 quarterly filings
            var quarterlyFilings = GetLast4QuarterlyFilings(filings, date);
            if (quarterlyFilings.Count < 4)
            {
                return double.NaN;
            }

            double sum = 0;
            foreach (var filing in quarterlyFilings)
            {
                double val;
                if (info.Statement == PolygonFinancialPropertyMap.StatementType.Computed)
                {
                    val = PolygonFinancialPropertyMap.ComputeFreeCashFlow(filing);
                }
                else
                {
                    val = PolygonFinancialPropertyMap.GetFieldValue(filing, info.PolygonField, info.Statement);
                }

                if (double.IsNaN(val))
                {
                    return double.NaN;
                }

                sum += val;
            }

            return sum;
        }

        /// <summary>
        /// Finds the most recent quarterly filing where filing_date &lt;= date (point-in-time)
        /// </summary>
        private static PolygonFinancialResult? FindMostRecentQuarterlyFiling(List<PolygonFinancialResult> filings, DateTime date)
        {
            // Filings are sorted by filing_date ascending, so iterate backwards
            for (int i = filings.Count - 1; i >= 0; i--)
            {
                var filing = filings[i];
                if (filing.Timeframe == "quarterly" && filing.GetFilingDate() <= date)
                {
                    return filing;
                }
            }

            return null;
        }

        /// <summary>
        /// Gets the last 4 quarterly filings available at a given date (point-in-time)
        /// </summary>
        private static List<PolygonFinancialResult> GetLast4QuarterlyFilings(List<PolygonFinancialResult> filings, DateTime date)
        {
            var result = new List<PolygonFinancialResult>();

            for (int i = filings.Count - 1; i >= 0 && result.Count < 4; i--)
            {
                var filing = filings[i];
                if (filing.Timeframe == "quarterly" && filing.GetFilingDate() <= date)
                {
                    result.Add(filing);
                }
            }

            // Reverse so they're in chronological order (oldest first)
            result.Reverse();
            return result;
        }

        /// <summary>
        /// Ensures financial data for a ticker is loaded into the in-memory cache.
        /// Uses per-ticker locks for thread safety and download-once semantics.
        /// </summary>
        private void EnsureLoaded(string ticker)
        {
            ticker = ticker.ToUpperInvariant();

            // Fast path: already loaded and not stale
            if (_loadedAt.TryGetValue(ticker, out var loadedTime))
            {
                if (!_liveMode || (DateTime.UtcNow - loadedTime).TotalHours < _cacheHours)
                {
                    return;
                }
            }

            var tickerLock = _downloadLocks.GetOrAdd(ticker, _ => new object());
            lock (tickerLock)
            {
                // Double-check after acquiring lock
                if (_loadedAt.TryGetValue(ticker, out loadedTime))
                {
                    if (!_liveMode || (DateTime.UtcNow - loadedTime).TotalHours < _cacheHours)
                    {
                        return;
                    }
                }

                var filings = LoadFromDisk(ticker);
                if (filings != null)
                {
                    _cache[ticker] = filings;
                    _loadedAt[ticker] = DateTime.UtcNow;
                    return;
                }

                // Download from API
                filings = DownloadFromApi(ticker);
                if (filings != null)
                {
                    _cache[ticker] = filings;
                    _loadedAt[ticker] = DateTime.UtcNow;
                    SaveToDisk(ticker, filings);
                }
            }
        }

        /// <summary>
        /// Loads cached financial data from disk. Returns null if no valid cache exists or cache is stale.
        /// </summary>
        private List<PolygonFinancialResult>? LoadFromDisk(string ticker)
        {
            var filePath = GetCacheFilePath(ticker);
            if (!File.Exists(filePath))
            {
                return null;
            }

            // In live mode, check file age
            if (_liveMode)
            {
                var fileAge = DateTime.UtcNow - File.GetLastWriteTimeUtc(filePath);
                if (fileAge.TotalHours > _cacheHours)
                {
                    return null; // Stale cache, will re-download
                }
            }

            try
            {
                var json = File.ReadAllText(filePath);
                var filings = JsonConvert.DeserializeObject<List<PolygonFinancialResult>>(json);
                if (filings != null)
                {
                    Log.Debug($"PolygonFinancialDataService: Loaded {filings.Count} filings for {ticker} from cache");
                    return filings;
                }
            }
            catch (Exception ex)
            {
                Log.Error($"PolygonFinancialDataService: Corrupt cache for {ticker}, deleting: {ex.Message}");
                try { File.Delete(filePath); } catch { }
            }

            return null;
        }

        /// <summary>
        /// Saves financial data to disk cache
        /// </summary>
        private void SaveToDisk(string ticker, List<PolygonFinancialResult> filings)
        {
            try
            {
                if (!Directory.Exists(_cacheDirectory))
                {
                    Directory.CreateDirectory(_cacheDirectory);
                }

                var filePath = GetCacheFilePath(ticker);
                var json = JsonConvert.SerializeObject(filings, Formatting.Indented);
                File.WriteAllText(filePath, json);
                Log.Debug($"PolygonFinancialDataService: Cached {filings.Count} filings for {ticker} to disk");
            }
            catch (Exception ex)
            {
                Log.Error($"PolygonFinancialDataService: Failed to save cache for {ticker}: {ex.Message}");
            }
        }

        /// <summary>
        /// Downloads financial data from the Polygon API. Returns empty list on failure (not null),
        /// so we don't cache the failure and can retry later.
        /// </summary>
        private List<PolygonFinancialResult>? DownloadFromApi(string ticker)
        {
            var resource = "vX/reference/financials";
            var parameters = new Dictionary<string, string>
            {
                ["ticker"] = ticker,
                ["timeframe"] = "quarterly",
                ["order"] = "asc",
                ["sort"] = "filing_date",
                ["limit"] = "100"
            };

            var allFilings = new List<PolygonFinancialResult>();
            try
            {
                Log.Trace($"PolygonFinancialDataService: Downloading financials for {ticker}");
                foreach (var response in _restClient.DownloadAndParseData<FinancialsResponse>(resource, parameters))
                {
                    allFilings.AddRange(response.Results);
                }

                // Filter out invalid filings and sort by filing date
                allFilings = allFilings
                    .Where(f => f.GetFilingDate() != DateTime.MinValue)
                    .OrderBy(f => f.GetFilingDate())
                    .ToList();

                Log.Trace($"PolygonFinancialDataService: Downloaded {allFilings.Count} quarterly filings for {ticker}");
                return allFilings;
            }
            catch (Exception ex)
            {
                Log.Error($"PolygonFinancialDataService: Failed to download financials for {ticker}: {ex.Message}");
                // Return null on failure so we don't cache an empty result
                return null;
            }
        }

        /// <summary>
        /// Gets the file path for a ticker's cached financial data
        /// </summary>
        private string GetCacheFilePath(string ticker)
        {
            return Path.Combine(_cacheDirectory, $"{ticker.ToLowerInvariant()}.json");
        }
    }
}
