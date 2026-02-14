/*
 * CASCADELABS.IO
 * Cascade Labs LLC
 */

using System.Globalization;
using Newtonsoft.Json;

namespace QuantConnect.Lean.DataSource.Polygon
{
    /// <summary>
    /// Response from Polygon's vX/reference/financials API endpoint
    /// </summary>
    public class FinancialsResponse : BaseResultsResponse<PolygonFinancialResult>
    {
    }

    /// <summary>
    /// Represents a single financial filing result from Polygon
    /// </summary>
    public class PolygonFinancialResult
    {
        /// <summary>
        /// The ticker symbol
        /// </summary>
        [JsonProperty("ticker")]
        public string Ticker { get; set; } = string.Empty;

        /// <summary>
        /// The fiscal year of the filing
        /// </summary>
        [JsonProperty("fiscal_year")]
        public string FiscalYear { get; set; } = string.Empty;

        /// <summary>
        /// The fiscal period (e.g. "Q1", "Q2", "Q3", "Q4", "FY")
        /// </summary>
        [JsonProperty("fiscal_period")]
        public string FiscalPeriod { get; set; } = string.Empty;

        /// <summary>
        /// The start date of the reporting period (YYYY-MM-DD)
        /// </summary>
        [JsonProperty("start_date")]
        public string StartDate { get; set; } = string.Empty;

        /// <summary>
        /// The end date of the reporting period (YYYY-MM-DD)
        /// </summary>
        [JsonProperty("end_date")]
        public string EndDate { get; set; } = string.Empty;

        /// <summary>
        /// The date when the filing was made available (YYYY-MM-DD)
        /// </summary>
        [JsonProperty("filing_date")]
        public string FilingDate { get; set; } = string.Empty;

        /// <summary>
        /// The timeframe of the filing (e.g. "quarterly", "annual")
        /// </summary>
        [JsonProperty("timeframe")]
        public string Timeframe { get; set; } = string.Empty;

        /// <summary>
        /// The financial statements data
        /// </summary>
        [JsonProperty("financials")]
        public PolygonFinancials Financials { get; set; } = new();

        /// <summary>
        /// Parses the filing date string to a DateTime
        /// </summary>
        public DateTime GetFilingDate()
        {
            if (DateTime.TryParseExact(FilingDate, "yyyy-MM-dd",
                CultureInfo.InvariantCulture, DateTimeStyles.None, out var date))
            {
                return date;
            }
            return DateTime.MinValue;
        }

        /// <summary>
        /// Parses the end date string to a DateTime
        /// </summary>
        public DateTime GetEndDate()
        {
            if (DateTime.TryParseExact(EndDate, "yyyy-MM-dd",
                CultureInfo.InvariantCulture, DateTimeStyles.None, out var date))
            {
                return date;
            }
            return DateTime.MinValue;
        }
    }

    /// <summary>
    /// Contains the three financial statement sections
    /// </summary>
    public class PolygonFinancials
    {
        /// <summary>
        /// Income statement line items
        /// </summary>
        [JsonProperty("income_statement")]
        public Dictionary<string, PolygonFinancialValue> IncomeStatement { get; set; } = new();

        /// <summary>
        /// Balance sheet line items
        /// </summary>
        [JsonProperty("balance_sheet")]
        public Dictionary<string, PolygonFinancialValue> BalanceSheet { get; set; } = new();

        /// <summary>
        /// Cash flow statement line items
        /// </summary>
        [JsonProperty("cash_flow_statement")]
        public Dictionary<string, PolygonFinancialValue> CashFlowStatement { get; set; } = new();
    }

    /// <summary>
    /// A single financial value with metadata
    /// </summary>
    public class PolygonFinancialValue
    {
        /// <summary>
        /// The numerical value
        /// </summary>
        [JsonProperty("value")]
        public double Value { get; set; }

        /// <summary>
        /// The unit of measurement (e.g. "USD")
        /// </summary>
        [JsonProperty("unit")]
        public string Unit { get; set; } = string.Empty;

        /// <summary>
        /// Human-readable label
        /// </summary>
        [JsonProperty("label")]
        public string Label { get; set; } = string.Empty;

        /// <summary>
        /// Display order
        /// </summary>
        [JsonProperty("order")]
        public int Order { get; set; }
    }
}
