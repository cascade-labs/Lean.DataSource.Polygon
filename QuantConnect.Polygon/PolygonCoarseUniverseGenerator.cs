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
using QuantConnect.Data.Auxiliary;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Util;

namespace QuantConnect.Lean.DataSource.Polygon
{
    /// <summary>
    /// Generates LEAN-compatible coarse universe files from Polygon API data.
    /// Output format: equity/usa/fundamental/coarse/{date}.csv
    /// </summary>
    public class PolygonCoarseUniverseGenerator : IDisposable
    {
        private readonly PolygonRestApiClient _restClient;
        private readonly IFactorFileProvider _factorFileProvider;
        private readonly string _outputDirectory;
        private readonly int _maxConcurrentTickers;
        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="PolygonCoarseUniverseGenerator"/> class
        /// </summary>
        /// <param name="restClient">The Polygon REST API client</param>
        /// <param name="factorFileProvider">Factor file provider for price/split factors</param>
        /// <param name="outputDirectory">Directory to write coarse universe files</param>
        public PolygonCoarseUniverseGenerator(
            PolygonRestApiClient restClient,
            IFactorFileProvider factorFileProvider,
            string outputDirectory)
        {
            _restClient = restClient;
            _factorFileProvider = factorFileProvider;
            _outputDirectory = outputDirectory;
            _maxConcurrentTickers = Config.GetInt("polygon-coarse-max-concurrent", 10);

            Log.Trace($"PolygonCoarseUniverseGenerator: Initialized with output directory: {outputDirectory}");
        }

        /// <summary>
        /// Generates coarse universe files for the specified date range
        /// </summary>
        /// <param name="startDate">Start date (inclusive)</param>
        /// <param name="endDate">End date (inclusive)</param>
        public void Generate(DateTime startDate, DateTime endDate)
        {
            Log.Trace($"PolygonCoarseUniverseGenerator: Starting generation for {startDate:yyyy-MM-dd} to {endDate:yyyy-MM-dd}");

            // Ensure output directory exists
            if (!Directory.Exists(_outputDirectory))
            {
                Directory.CreateDirectory(_outputDirectory);
            }

            // Process each date
            for (var date = startDate; date <= endDate; date = date.AddDays(1))
            {
                // Skip weekends
                if (date.DayOfWeek == DayOfWeek.Saturday || date.DayOfWeek == DayOfWeek.Sunday)
                {
                    continue;
                }

                GenerateForDate(date);
            }

            Log.Trace("PolygonCoarseUniverseGenerator: Generation complete");
        }

        /// <summary>
        /// Generates a coarse universe file for a single date using the stock snapshot API
        /// </summary>
        /// <param name="date">The date to generate for</param>
        public void GenerateForDate(DateTime date)
        {
            Log.Debug($"PolygonCoarseUniverseGenerator: Processing {date:yyyy-MM-dd}");

            // Ensure output directory exists
            if (!Directory.Exists(_outputDirectory))
            {
                Directory.CreateDirectory(_outputDirectory);
            }

            // Get all active stock tickers
            var tickers = GetActiveStockTickers();
            Log.Trace($"PolygonCoarseUniverseGenerator: Found {tickers.Count} active stock tickers");

            // Get snapshot for all tickers (single API call)
            var snapshots = GetStockSnapshots();
            Log.Trace($"PolygonCoarseUniverseGenerator: Got {snapshots.Count} stock snapshots");

            // Create ticker lookup for quick access
            var tickerSet = new HashSet<string>(tickers.Select(t => t.Ticker), StringComparer.OrdinalIgnoreCase);

            // Process snapshots in parallel
            var coarseRows = new ConcurrentBag<string>();
            var processedCount = 0;
            var errorCount = 0;

            var parallelOptions = new ParallelOptions
            {
                MaxDegreeOfParallelism = _maxConcurrentTickers
            };

            Parallel.ForEach(snapshots.Where(s => tickerSet.Contains(s.Ticker)), parallelOptions, snapshot =>
            {
                try
                {
                    var row = ProcessSnapshot(snapshot, date);
                    if (row != null)
                    {
                        coarseRows.Add(row);
                    }

                    var count = Interlocked.Increment(ref processedCount);
                    if (count % 1000 == 0)
                    {
                        Log.Debug($"PolygonCoarseUniverseGenerator: Processed {count}/{snapshots.Count} snapshots for {date:yyyy-MM-dd}");
                    }
                }
                catch (Exception ex)
                {
                    Interlocked.Increment(ref errorCount);
                    Log.Debug($"PolygonCoarseUniverseGenerator: Error processing {snapshot.Ticker}: {ex.Message}");
                }
            });

            if (coarseRows.Count > 0)
            {
                WriteCoarseFile(date, coarseRows.OrderBy(r => r).ToList());
                Log.Trace($"PolygonCoarseUniverseGenerator: Wrote {coarseRows.Count} entries for {date:yyyy-MM-dd} ({errorCount} errors)");
            }
            else
            {
                Log.Debug($"PolygonCoarseUniverseGenerator: No data for {date:yyyy-MM-dd}");
            }
        }

        /// <summary>
        /// Gets all active stock tickers from Polygon's reference API
        /// </summary>
        private List<TickerDetails> GetActiveStockTickers()
        {
            var resource = "v3/reference/tickers";
            var parameters = new Dictionary<string, string>
            {
                ["type"] = "CS",  // Common stock
                ["market"] = "stocks",
                ["active"] = "true",
                ["limit"] = "1000"
            };

            var tickers = new List<TickerDetails>();

            try
            {
                foreach (var response in _restClient.DownloadAndParseData<TickersResponse>(resource, parameters))
                {
                    tickers.AddRange(response.Results);
                }
            }
            catch (Exception ex)
            {
                Log.Error($"PolygonCoarseUniverseGenerator: Error fetching tickers: {ex.Message}");
            }

            return tickers;
        }

        /// <summary>
        /// Gets stock snapshots for all tickers from Polygon's snapshot API (single API call)
        /// </summary>
        private List<StockSnapshot> GetStockSnapshots()
        {
            var resource = "v2/snapshot/locale/us/markets/stocks/tickers";

            try
            {
                foreach (var response in _restClient.DownloadAndParseData<StockSnapshotResponse>(resource))
                {
                    return response.Tickers ?? new List<StockSnapshot>();
                }
            }
            catch (Exception ex)
            {
                Log.Error($"PolygonCoarseUniverseGenerator: Error fetching snapshots: {ex.Message}");
            }

            return new List<StockSnapshot>();
        }

        /// <summary>
        /// Processes a stock snapshot and returns the coarse row if data is valid
        /// </summary>
        private string? ProcessSnapshot(StockSnapshot snapshot, DateTime date)
        {
            // Use prevDay for historical generation, or day for current day
            var bar = snapshot.PrevDay ?? snapshot.Day;

            // Skip if no data or invalid
            if (bar == null || bar.Close <= 0 || bar.Volume <= 0)
            {
                return null;
            }

            // Generate SecurityIdentifier for this ticker
            var sid = SecurityIdentifier.GenerateEquity(date, snapshot.Ticker, Market.USA);
            var symbol = new Symbol(sid, snapshot.Ticker);

            // Get price and split factors from factor file provider
            var priceFactor = 1m;
            var splitFactor = 1m;

            try
            {
                var factorFile = _factorFileProvider.Get(symbol);
                if (factorFile is CorporateFactorProvider corporateFactorProvider)
                {
                    var factors = corporateFactorProvider.GetScalingFactors(date);
                    priceFactor = factors?.PriceFactor.Normalize() ?? 1m;
                    splitFactor = factors?.SplitFactor.Normalize() ?? 1m;
                }
            }
            catch (Exception ex)
            {
                Log.Debug($"PolygonCoarseUniverseGenerator: Could not get factors for {snapshot.Ticker}: {ex.Message}");
            }

            // Calculate dollar volume
            var dollarVolume = Math.Truncate((double)(bar.Close * bar.Volume));

            // Polygon doesn't provide fundamental data through the basic API
            var hasFundamentalData = false;

            // Format: sid,symbol,close,volume,dollar_volume,has_fundamental_data,price_factor,split_factor
            return string.Join(",",
                sid.ToString(),
                snapshot.Ticker,
                bar.Close.Normalize().ToString(CultureInfo.InvariantCulture),
                decimal.ToInt64(bar.Volume).ToString(CultureInfo.InvariantCulture),
                dollarVolume.ToString(CultureInfo.InvariantCulture),
                hasFundamentalData.ToString(),
                priceFactor.ToString(CultureInfo.InvariantCulture),
                splitFactor.ToString(CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// Writes the coarse universe file for a date
        /// </summary>
        private void WriteCoarseFile(DateTime date, List<string> rows)
        {
            var filename = $"{date.ToString(DateFormat.EightCharacter, CultureInfo.InvariantCulture)}.csv";
            var filePath = Path.Combine(_outputDirectory, filename);

            File.WriteAllLines(filePath, rows);
        }

        /// <summary>
        /// Disposes of the generator resources
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Disposes of the generator resources
        /// </summary>
        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    // Don't dispose the REST client - it's owned by the caller
                }
                _disposed = true;
            }
        }
    }
}
