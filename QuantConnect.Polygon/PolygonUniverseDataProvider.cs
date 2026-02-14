/*
 * CASCADELABS.IO
 * Cascade Labs LLC
 */

using QuantConnect.Configuration;
using QuantConnect.Data.Fundamental;
using QuantConnect.Data.UniverseSelection;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Util;

namespace QuantConnect.Lean.DataSource.Polygon
{
    /// <summary>
    /// Fundamental data provider that downloads coarse universe data from Polygon APIs on-demand.
    /// Implements the same download-once pattern as DownloaderDataProvider for price data.
    /// </summary>
    public class PolygonUniverseDataProvider : BaseFundamentalDataProvider
    {
        /// <summary>
        /// Synchronizer to ensure download-once behavior for coarse files
        /// </summary>
        private static readonly KeyStringSynchronizer DownloadSynchronizer = new();

        private DateTime _currentDate;
        private readonly Dictionary<SecurityIdentifier, CoarseFundamentalDataProvider.CoarseFundamentalSource> _cache = new();
        private readonly object _cacheLock = new();
        private PolygonCoarseUniverseGenerator? _generator;
        private PolygonRestApiClient? _restClient;
        private IFactorFileProvider? _factorFileProvider;
        private PolygonFinancialDataService? _financialService;
        private bool _liveMode;
        private bool _initialized;

        /// <summary>
        /// Initializes the provider with data provider and live mode setting
        /// </summary>
        /// <param name="dataProvider">The data provider instance to use</param>
        /// <param name="liveMode">True if running in live mode</param>
        public override void Initialize(IDataProvider dataProvider, bool liveMode)
        {
            base.Initialize(dataProvider, liveMode);

            if (_initialized)
            {
                return;
            }

            var apiKey = Config.Get("polygon-api-key");
            if (string.IsNullOrEmpty(apiKey))
            {
                throw new InvalidOperationException("PolygonUniverseDataProvider requires 'polygon-api-key' to be configured");
            }

            _liveMode = liveMode;
            _restClient = new PolygonRestApiClient(apiKey);
            _factorFileProvider = Composer.Instance.GetPart<IFactorFileProvider>();
            _financialService = new PolygonFinancialDataService(_restClient, liveMode);

            var outputDirectory = Path.Combine(Globals.DataFolder, "equity", "usa", "fundamental", "coarse");
            _generator = new PolygonCoarseUniverseGenerator(_restClient, _factorFileProvider, outputDirectory);

            _initialized = true;
            Log.Trace("PolygonUniverseDataProvider: Initialized");
        }

        /// <summary>
        /// Fetches the requested fundamental information for the requested time and security
        /// </summary>
        /// <typeparam name="T">The expected data type</typeparam>
        /// <param name="time">The time to request this data for</param>
        /// <param name="securityIdentifier">The security identifier</param>
        /// <param name="name">The name of the fundamental property</param>
        /// <returns>The fundamental information</returns>
        public override T Get<T>(DateTime time, SecurityIdentifier securityIdentifier, FundamentalProperty name)
        {
            var enumName = Enum.GetName(name);

            // Check if this is a financial statement property
            if (_financialService != null && enumName != null && PolygonFinancialPropertyMap.IsFinancialProperty(enumName))
            {
                if (enumName == nameof(CoarseFundamental.HasFundamentalData))
                {
                    var ticker = securityIdentifier.Symbol;
                    return (T)(object)_financialService.HasFinancialData(ticker);
                }

                var financialTicker = securityIdentifier.Symbol;
                var value = _financialService.GetFinancialValue(financialTicker, time.Date, enumName);
                return (T)(object)value;
            }

            lock (_cacheLock)
            {
                if (time.Date != _currentDate)
                {
                    _currentDate = time.Date;
                    LoadOrDownload(time.Date);
                }

                return GetProperty<T>(securityIdentifier, enumName);
            }
        }

        /// <summary>
        /// Loads coarse data from disk, downloading if necessary
        /// </summary>
        private void LoadOrDownload(DateTime date)
        {
            var path = GetCoarsePath(date);

            if (!File.Exists(path))
            {
                // Thread-safe download-once pattern
                DownloadSynchronizer.Execute($"polygon-coarse-{date:yyyyMMdd}", singleExecution: true, () =>
                {
                    // Double-check after acquiring lock
                    if (!File.Exists(path))
                    {
                        Log.Trace($"PolygonUniverseDataProvider: Downloading coarse universe data for {date:yyyy-MM-dd}");
                        _generator!.GenerateForDate(date);
                    }
                });
            }

            // Load CSV into cache
            _cache.Clear();

            if (!File.Exists(path))
            {
                Log.Debug($"PolygonUniverseDataProvider: No coarse file available for {date:yyyy-MM-dd}");
                return;
            }

            foreach (var line in File.ReadLines(path))
            {
                var coarse = CoarseFundamentalDataProvider.Read(line, date);
                if (coarse != null)
                {
                    _cache[coarse.Symbol.ID] = coarse;
                }
            }

            Log.Debug($"PolygonUniverseDataProvider: Loaded {_cache.Count} entries for {date:yyyy-MM-dd}");
        }

        /// <summary>
        /// Gets the path to the coarse universe file for a date
        /// </summary>
        private static string GetCoarsePath(DateTime date)
        {
            return Path.Combine(Globals.DataFolder, "equity", "usa", "fundamental", "coarse", $"{date:yyyyMMdd}.csv");
        }

        /// <summary>
        /// Gets a property value from the cached coarse data
        /// </summary>
        private T GetProperty<T>(SecurityIdentifier securityIdentifier, string? property)
        {
            if (!_cache.TryGetValue(securityIdentifier, out var coarse))
            {
                return GetDefault<T>();
            }

            return property switch
            {
                nameof(CoarseFundamental.Price) => (T)(object)coarse.Price,
                nameof(CoarseFundamental.Value) => (T)(object)coarse.Value,
                nameof(CoarseFundamental.Market) => (T)(object)coarse.Market,
                nameof(CoarseFundamental.Volume) => (T)(object)coarse.Volume,
                nameof(CoarseFundamental.PriceFactor) => (T)(object)coarse.PriceFactor,
                nameof(CoarseFundamental.SplitFactor) => (T)(object)coarse.SplitFactor,
                nameof(CoarseFundamental.DollarVolume) => (T)(object)coarse.DollarVolume,
                nameof(CoarseFundamental.HasFundamentalData) => (T)(object)(_financialService?.HasFinancialData(securityIdentifier.Symbol) ?? false),
                _ => GetDefault<T>()
            };
        }
    }
}
