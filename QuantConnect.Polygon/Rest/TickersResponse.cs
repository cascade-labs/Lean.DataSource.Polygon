/*
 * CASCADELABS.IO
 * Cascade Labs LLC
 */

using Newtonsoft.Json;

namespace QuantConnect.Lean.DataSource.Polygon
{
    /// <summary>
    /// Response from Polygon's /v3/reference/tickers API endpoint
    /// </summary>
    public class TickersResponse : BaseResultsResponse<TickerDetails>
    {
    }

    /// <summary>
    /// Details about a ticker from Polygon's reference data
    /// </summary>
    public class TickerDetails
    {
        /// <summary>
        /// The ticker symbol
        /// </summary>
        [JsonProperty("ticker")]
        public string Ticker { get; set; } = string.Empty;

        /// <summary>
        /// The name of the security
        /// </summary>
        [JsonProperty("name")]
        public string Name { get; set; } = string.Empty;

        /// <summary>
        /// The primary exchange where the security is listed
        /// </summary>
        [JsonProperty("primary_exchange")]
        public string PrimaryExchange { get; set; } = string.Empty;

        /// <summary>
        /// The type of security (CS = common stock, ETF = exchange traded fund, etc.)
        /// </summary>
        [JsonProperty("type")]
        public string Type { get; set; } = string.Empty;

        /// <summary>
        /// Whether the ticker is actively traded
        /// </summary>
        [JsonProperty("active")]
        public bool Active { get; set; }

        /// <summary>
        /// The market the security trades in (stocks, crypto, fx, otc)
        /// </summary>
        [JsonProperty("market")]
        public string Market { get; set; } = string.Empty;

        /// <summary>
        /// The locale of the security (us, global)
        /// </summary>
        [JsonProperty("locale")]
        public string Locale { get; set; } = string.Empty;

        /// <summary>
        /// The CIK number for this ticker (SEC identifier)
        /// </summary>
        [JsonProperty("cik")]
        public string? Cik { get; set; }

        /// <summary>
        /// The currency the price is quoted in
        /// </summary>
        [JsonProperty("currency_name")]
        public string? CurrencyName { get; set; }
    }
}
