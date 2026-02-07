/*
 * CASCADELABS.IO
 * Cascade Labs LLC
 */

using System.Collections.Concurrent;
using System.Globalization;
using QuantConnect.Configuration;
using QuantConnect.Data.Auxiliary;
using QuantConnect.Interfaces;
using QuantConnect.Logging;

namespace QuantConnect.Lean.DataSource.Polygon
{
    /// <summary>
    /// Map file provider that fetches ticker events from Polygon API
    /// and generates LEAN-compatible map files on demand for delistings and ticker changes
    /// </summary>
    public class PolygonMapFileProvider : IMapFileProvider
    {
        private IDataProvider? _dataProvider;
        private PolygonRestApiClient? _restClient;
        private LocalDiskMapFileProvider? _localProvider;
        private readonly ConcurrentDictionary<AuxiliaryDataKey, MapFileResolver> _cache;
        private readonly ConcurrentDictionary<string, object> _generationLocks;
        private readonly object _initLock = new();
        private bool _initialized;

        /// <summary>
        /// Default start date for ticker history (first trading date)
        /// </summary>
        private static readonly DateTime DefaultStartDate = new DateTime(2000, 1, 1);

        /// <summary>
        /// Far-future date used as the last row for active (non-delisted) symbols
        /// </summary>
        private static readonly DateTime FarFutureDate = new DateTime(2050, 12, 31);

        public PolygonMapFileProvider()
        {
            _cache = new ConcurrentDictionary<AuxiliaryDataKey, MapFileResolver>();
            _generationLocks = new ConcurrentDictionary<string, object>();
        }

        /// <summary>
        /// Initializes the provider with a data provider
        /// </summary>
        public void Initialize(IDataProvider dataProvider)
        {
            lock (_initLock)
            {
                if (_initialized) return;

                _dataProvider = dataProvider;

                // Initialize the local provider for reading existing map files
                _localProvider = new LocalDiskMapFileProvider();
                _localProvider.Initialize(dataProvider);

                // Initialize REST client
                var apiKey = Config.Get("polygon-api-key");
                if (string.IsNullOrEmpty(apiKey))
                {
                    throw new InvalidOperationException("PolygonMapFileProvider requires 'polygon-api-key' to be configured");
                }
                _restClient = new PolygonRestApiClient(apiKey);

                _initialized = true;
                Log.Trace("PolygonMapFileProvider: Initialized");
            }
        }

        /// <summary>
        /// Gets a MapFileResolver for the specified auxiliary data key.
        /// This resolver can be used to look up map files for symbols in the specified market.
        /// </summary>
        public MapFileResolver Get(AuxiliaryDataKey auxiliaryDataKey)
        {
            if (!_initialized)
            {
                throw new InvalidOperationException("PolygonMapFileProvider has not been initialized");
            }

            // Only support equities
            if (auxiliaryDataKey.SecurityType != SecurityType.Equity)
            {
                return MapFileResolver.Empty;
            }

            return _cache.GetOrAdd(auxiliaryDataKey, key => CreateMapFileResolver(key));
        }

        /// <summary>
        /// Creates a MapFileResolver that will generate map files on demand from Polygon
        /// </summary>
        private MapFileResolver CreateMapFileResolver(AuxiliaryDataKey key)
        {
            // First, load existing map files from disk
            var localResolver = _localProvider!.Get(key);

            // Create a custom resolver that can generate map files on demand
            return new PolygonMapFileResolver(
                localResolver,
                this,
                key.Market,
                key.SecurityType);
        }

        /// <summary>
        /// Generates or retrieves a map file for the specified symbol from Polygon API
        /// </summary>
        internal MapFile GetOrGenerateMapFile(string ticker, string market, SecurityType securityType)
        {
            var mapFilePath = GetMapFilePath(ticker, market, securityType);

            if (File.Exists(mapFilePath))
            {
                var lastEntryDate = GetLastEntryDate(mapFilePath);
                var today = DateTime.UtcNow.Date;

                // If file is up-to-date (last entry is far-future or recent), use cached
                if (lastEntryDate >= today.AddDays(-1) || lastEntryDate >= FarFutureDate.AddYears(-1))
                {
                    Log.Debug($"PolygonMapFileProvider: Using cached map file for {ticker}");
                    var rows = MapFileRow.Read(mapFilePath, market, securityType, _dataProvider!);
                    return new MapFile(ticker, rows);
                }
            }

            // Generate from Polygon API
            var symbolLock = _generationLocks.GetOrAdd(ticker.ToUpperInvariant(), _ => new object());
            lock (symbolLock)
            {
                // Double-check after acquiring lock
                if (File.Exists(mapFilePath))
                {
                    var lastEntryDate = GetLastEntryDate(mapFilePath);
                    var today = DateTime.UtcNow.Date;
                    if (lastEntryDate >= today.AddDays(-1) || lastEntryDate >= FarFutureDate.AddYears(-1))
                    {
                        var rows = MapFileRow.Read(mapFilePath, market, securityType, _dataProvider!);
                        return new MapFile(ticker, rows);
                    }
                }

                Log.Trace($"PolygonMapFileProvider: Generating map file for {ticker} from Polygon API");
                return GenerateMapFile(ticker, market, securityType);
            }
        }

        /// <summary>
        /// Reads the last entry date from a map file
        /// </summary>
        private static DateTime GetLastEntryDate(string mapFilePath)
        {
            var lastLine = File.ReadLines(mapFilePath).LastOrDefault();
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
        /// Generates a map file by fetching ticker events from Polygon
        /// </summary>
        private MapFile GenerateMapFile(string ticker, string market, SecurityType securityType)
        {
            try
            {
                var events = FetchTickerEvents(ticker);
                var rows = BuildMapFileRows(ticker, events, market, securityType);

                var mapFile = new MapFile(ticker, rows);

                // Write to disk
                WriteMapFile(mapFile, market, securityType);

                return mapFile;
            }
            catch (Exception ex)
            {
                Log.Error($"PolygonMapFileProvider: Error generating map file for {ticker}: {ex.Message}");
                // Return a minimal map file on error
                return CreateMinimalMapFile(ticker, market, securityType);
            }
        }

        /// <summary>
        /// Fetches ticker events from Polygon API
        /// </summary>
        private List<TickerEvent> FetchTickerEvents(string ticker)
        {
            var resource = $"v3/reference/tickers/{ticker}/events";
            var parameters = new Dictionary<string, string>
            {
                ["types"] = "ticker_change,delisted",
                ["limit"] = "1000"
            };

            var events = new List<TickerEvent>();
            try
            {
                foreach (var response in _restClient!.DownloadAndParseData<TickerEventsResponse>(resource, parameters))
                {
                    if (response.Results?.Events != null)
                    {
                        events.AddRange(response.Results.Events);
                    }
                }
            }
            catch (Exception ex)
            {
                // Log but don't throw - we'll create a minimal map file
                Log.Debug($"PolygonMapFileProvider: Error fetching ticker events for {ticker}: {ex.Message}");
            }

            return events.OrderBy(e => e.GetDate()).ToList();
        }

        /// <summary>
        /// Builds map file rows from ticker events
        /// </summary>
        private List<MapFileRow> BuildMapFileRows(string ticker, List<TickerEvent> events, string market, SecurityType securityType)
        {
            var rows = new List<MapFileRow>();
            var primaryExchange = GetPrimaryExchange(market, securityType);
            var currentSymbol = ticker.ToUpperInvariant();

            // First row: First trading date with original symbol
            // For simplicity, use DefaultStartDate as we don't have exact IPO date
            rows.Add(new MapFileRow(DefaultStartDate, currentSymbol, primaryExchange));

            DateTime? delistingDate = null;

            // Process events chronologically
            foreach (var tickerEvent in events)
            {
                var eventDate = tickerEvent.GetDate();
                if (eventDate == DateTime.MinValue) continue;

                if (tickerEvent.Type == "ticker_change" && tickerEvent.TickerChange != null)
                {
                    // Symbol changed - add row for the day before the change with the old symbol
                    var oldSymbol = tickerEvent.TickerChange.Ticker.ToUpperInvariant();
                    var dayBeforeChange = eventDate.AddDays(-1);

                    // Add the old symbol row (the day before the change takes effect)
                    rows.Add(new MapFileRow(dayBeforeChange, oldSymbol, primaryExchange));

                    // Current symbol is now the new one (which is the ticker we're looking up)
                    currentSymbol = ticker.ToUpperInvariant();
                }
                else if (tickerEvent.Type == "delisted")
                {
                    // Delisting - this will be the last row
                    delistingDate = eventDate;
                }
            }

            // Last row: Either delisting date (final trading day) or far-future for active symbols
            var lastDate = delistingDate ?? FarFutureDate;
            rows.Add(new MapFileRow(lastDate, currentSymbol, primaryExchange));

            // Remove duplicates and sort
            return rows
                .GroupBy(r => r.Date)
                .Select(g => g.Last()) // Take the last entry for each date
                .OrderBy(r => r.Date)
                .ToList();
        }

        /// <summary>
        /// Gets the primary exchange code for the market
        /// </summary>
        private static Exchange GetPrimaryExchange(string market, SecurityType securityType)
        {
            // For US equities, default to NASDAQ
            if (market == Market.USA && securityType == SecurityType.Equity)
            {
                return Exchange.NASDAQ;
            }
            return Exchange.UNKNOWN;
        }

        /// <summary>
        /// Creates a minimal map file for a symbol (no ticker changes, not delisted)
        /// </summary>
        private MapFile CreateMinimalMapFile(string ticker, string market, SecurityType securityType)
        {
            var primaryExchange = GetPrimaryExchange(market, securityType);
            var rows = new List<MapFileRow>
            {
                new MapFileRow(DefaultStartDate, ticker.ToUpperInvariant(), primaryExchange),
                new MapFileRow(FarFutureDate, ticker.ToUpperInvariant(), primaryExchange)
            };

            return new MapFile(ticker, rows);
        }

        /// <summary>
        /// Writes the map file to the standard LEAN data directory
        /// </summary>
        private void WriteMapFile(MapFile mapFile, string market, SecurityType securityType)
        {
            var filePath = GetMapFilePath(mapFile.Permtick, market, securityType);
            var directory = Path.GetDirectoryName(filePath);

            if (!Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory!);
            }

            File.WriteAllLines(filePath, mapFile.ToCsvLines());
            Log.Trace($"PolygonMapFileProvider: Wrote map file to {filePath}");
        }

        /// <summary>
        /// Gets the path where map file should be stored
        /// </summary>
        private static string GetMapFilePath(string ticker, string market, SecurityType securityType)
        {
            return Path.Combine(
                Globals.DataFolder,
                MapFile.GetRelativeMapFilePath(market, securityType),
                ticker.ToLowerInvariant() + ".csv");
        }
    }

    /// <summary>
    /// Custom MapFileResolver that generates map files on demand from Polygon
    /// </summary>
    internal class PolygonMapFileResolver : MapFileResolver
    {
        private readonly MapFileResolver _localResolver;
        private readonly PolygonMapFileProvider _provider;
        private readonly string _market;
        private readonly SecurityType _securityType;
        private readonly ConcurrentDictionary<string, MapFile> _generatedMapFiles;

        public PolygonMapFileResolver(
            MapFileResolver localResolver,
            PolygonMapFileProvider provider,
            string market,
            SecurityType securityType)
            : base(localResolver) // Pass through local resolver's map files
        {
            _localResolver = localResolver;
            _provider = provider;
            _market = market;
            _securityType = securityType;
            _generatedMapFiles = new ConcurrentDictionary<string, MapFile>(StringComparer.InvariantCultureIgnoreCase);
        }

        /// <summary>
        /// Resolves the map file for the specified symbol, generating from Polygon if not found locally
        /// </summary>
        public override MapFile ResolveMapFile(string symbol, DateTime date)
        {
            // First try to resolve from local files
            var localMapFile = _localResolver.ResolveMapFile(symbol, date);

            // If we found a non-empty local map file, use it
            if (localMapFile != null && localMapFile.Any())
            {
                return localMapFile;
            }

            // Check if we've already generated this map file
            if (_generatedMapFiles.TryGetValue(symbol, out var cachedMapFile))
            {
                return cachedMapFile;
            }

            // Generate from Polygon
            var generatedMapFile = _provider.GetOrGenerateMapFile(symbol, _market, _securityType);
            _generatedMapFiles[symbol] = generatedMapFile;

            return generatedMapFile;
        }
    }
}
