using Amazon.S3;
using Amazon.S3.Model;
using QuantConnect.Logging;
using QuantConnect.Configuration;
using System.Collections.Concurrent;
using System.IO.Compression;

namespace QuantConnect.Lean.DataSource.Polygon
{
    /// <summary>
    /// S3 client for downloading Polygon flat files from files.massive.com.
    /// Downloads .csv.gz files to temp storage and returns decompressed streams.
    /// </summary>
    public class PolygonFlatFileClient : IDisposable
    {
        private readonly AmazonS3Client _client;
        private readonly string _bucket;
        private readonly string _tempDir;
        private readonly ConcurrentDictionary<string, Lazy<string>> _downloadCache = new();
        private bool _disposed;

        /// <summary>
        /// Initializes the Polygon flat file S3 client from config.
        /// Throws if S3 credentials are not configured.
        /// </summary>
        public PolygonFlatFileClient()
        {
            var endpoint = Config.Get("polygon-s3-endpoint", "");
            var accessKey = Config.Get("polygon-s3-access-key", "");
            var secretKey = Config.Get("polygon-s3-secret-key", "");
            _bucket = Config.Get("polygon-s3-bucket", "flatfiles");
            _tempDir = Path.Combine(Globals.DataFolder, "polygon-flatfiles");

            if (string.IsNullOrEmpty(endpoint) || string.IsNullOrEmpty(accessKey) ||
                string.IsNullOrEmpty(secretKey))
            {
                throw new InvalidOperationException(
                    "PolygonFlatFileClient: S3 credentials are required. " +
                    "Set 'polygon-s3-endpoint', 'polygon-s3-access-key', and 'polygon-s3-secret-key' in config.");
            }

            var region = Config.Get("polygon-s3-region", "us-east-1");
            var config = new AmazonS3Config
            {
                ServiceURL = $"https://{endpoint}",
                ForcePathStyle = true,
                SignatureVersion = "4",
                AuthenticationRegion = region
            };

            _client = new AmazonS3Client(accessKey, secretKey, config);
            Log.Trace($"PolygonFlatFileClient: Initialized with endpoint {endpoint}, bucket {_bucket}");
        }

        /// <summary>
        /// Gets the S3 key for a day_aggs flat file
        /// </summary>
        public static string GetDayAggsKey(DateTime date)
        {
            return $"us_options_opra/day_aggs_v1/{date:yyyy}/{date:MM}/{date:yyyy-MM-dd}.csv.gz";
        }

        /// <summary>
        /// Gets the S3 key for a minute_aggs flat file
        /// </summary>
        public static string GetMinuteAggsKey(DateTime date)
        {
            return $"us_options_opra/minute_aggs_v1/{date:yyyy}/{date:MM}/{date:yyyy-MM-dd}.csv.gz";
        }

        /// <summary>
        /// Gets the S3 key for a trades flat file
        /// </summary>
        public static string GetTradesKey(DateTime date)
        {
            return $"us_options_opra/trades_v1/{date:yyyy}/{date:MM}/{date:yyyy-MM-dd}.csv.gz";
        }

        /// <summary>
        /// Gets the S3 key for a stock day_aggs flat file
        /// </summary>
        public static string GetStockDayAggsKey(DateTime date)
        {
            return $"us_stocks_sip/day_aggs_v1/{date:yyyy}/{date:MM}/{date:yyyy-MM-dd}.csv.gz";
        }

        /// <summary>
        /// Gets the S3 key for a stock minute_aggs flat file
        /// </summary>
        public static string GetStockMinuteAggsKey(DateTime date)
        {
            return $"us_stocks_sip/minute_aggs_v1/{date:yyyy}/{date:MM}/{date:yyyy-MM-dd}.csv.gz";
        }

        /// <summary>
        /// Gets the S3 key for a stock quotes flat file
        /// </summary>
        public static string GetStockQuotesKey(DateTime date)
        {
            return $"us_stocks_sip/quotes_v1/{date:yyyy}/{date:MM}/{date:yyyy-MM-dd}.csv.gz";
        }

        /// <summary>
        /// Gets the S3 key for a stock trades flat file
        /// </summary>
        public static string GetStockTradesKey(DateTime date)
        {
            return $"us_stocks_sip/trades_v1/{date:yyyy}/{date:MM}/{date:yyyy-MM-dd}.csv.gz";
        }

        /// <summary>
        /// Downloads a flat file from S3 (or returns from temp cache) and returns a decompressed stream.
        /// Returns null if the file doesn't exist in S3.
        /// Thread-safe: concurrent calls for the same key will wait for the first download to complete.
        /// </summary>
        public Stream? GetFlatFile(string s3Key)
        {
            var localPath = Path.Combine(_tempDir, s3Key.Replace('/', Path.DirectorySeparatorChar));

            // Thread-safe: use Lazy to ensure only one download per key
            var lazyPath = _downloadCache.GetOrAdd(s3Key, key => new Lazy<string>(() =>
            {
                EnsureDownloaded(key, localPath);
                return localPath;
            }));

            try
            {
                var cachedPath = lazyPath.Value;
                if (!File.Exists(cachedPath))
                {
                    return null;
                }

                var fileStream = File.OpenRead(cachedPath);
                return new GZipStream(fileStream, CompressionMode.Decompress);
            }
            catch (Exception ex)
            {
                Log.Error($"PolygonFlatFileClient: Error reading flat file {s3Key}: {ex.Message}");
                return null;
            }
        }

        private void EnsureDownloaded(string s3Key, string localPath)
        {
            if (File.Exists(localPath))
            {
                Log.Trace($"PolygonFlatFileClient: Using temp cached file {localPath}");
                return;
            }

            Log.Debug($"PolygonFlatFileClient: Downloading {_bucket}/{s3Key}...");

            try
            {
                var request = new GetObjectRequest
                {
                    BucketName = _bucket,
                    Key = s3Key
                };

                using var response = _client.GetObjectAsync(request).GetAwaiter().GetResult();

                var directory = Path.GetDirectoryName(localPath)!;
                Directory.CreateDirectory(directory);

                using var fileStream = File.Create(localPath);
                response.ResponseStream.CopyTo(fileStream);

                var fileInfo = new FileInfo(localPath);
                Log.Debug($"PolygonFlatFileClient: Downloaded {fileInfo.Length:N0} bytes to {localPath}");
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                Log.Debug($"PolygonFlatFileClient: File not found: {_bucket}/{s3Key}");
            }
            catch (Exception ex)
            {
                Log.Error($"PolygonFlatFileClient: Error downloading {_bucket}/{s3Key}: {ex.Message}");
                throw;
            }
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                _client.Dispose();
                // Raw flat files are cached persistently in {DataFolder}/polygon-flatfiles/
                // for reuse across backtest runs — no cleanup on dispose
                _disposed = true;
            }
        }
    }
}
