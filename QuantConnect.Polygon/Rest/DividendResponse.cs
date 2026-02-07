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

using Newtonsoft.Json;

namespace QuantConnect.Lean.DataSource.Polygon
{
    /// <summary>
    /// Models a Polygon.io REST API response containing dividends
    /// </summary>
    public class DividendResponse : BaseResultsResponse<PolygonDividend>
    {
    }

    /// <summary>
    /// Represents a single dividend from Polygon.io
    /// </summary>
    public class PolygonDividend
    {
        /// <summary>
        /// The ex-dividend date (YYYY-MM-DD format)
        /// </summary>
        [JsonProperty("ex_dividend_date")]
        public string ExDividendDate { get; set; } = string.Empty;

        /// <summary>
        /// The cash amount of the dividend per share
        /// </summary>
        [JsonProperty("cash_amount")]
        public decimal CashAmount { get; set; }

        /// <summary>
        /// The ticker symbol
        /// </summary>
        [JsonProperty("ticker")]
        public string Ticker { get; set; } = string.Empty;

        /// <summary>
        /// The type of dividend (e.g., "CD" for cash dividend, "SC" for special cash, "LT" for long-term capital gain, "ST" for short-term capital gain)
        /// </summary>
        [JsonProperty("dividend_type")]
        public string DividendType { get; set; } = string.Empty;

        /// <summary>
        /// The currency in which the dividend is paid
        /// </summary>
        [JsonProperty("currency")]
        public string Currency { get; set; } = string.Empty;

        /// <summary>
        /// The declaration date
        /// </summary>
        [JsonProperty("declaration_date")]
        public string? DeclarationDate { get; set; }

        /// <summary>
        /// The record date
        /// </summary>
        [JsonProperty("record_date")]
        public string? RecordDate { get; set; }

        /// <summary>
        /// The pay date
        /// </summary>
        [JsonProperty("pay_date")]
        public string? PayDate { get; set; }

        /// <summary>
        /// The frequency of the dividend (e.g., 4 for quarterly, 12 for monthly)
        /// </summary>
        [JsonProperty("frequency")]
        public int Frequency { get; set; }

        /// <summary>
        /// Parses the ex-dividend date string to a DateTime
        /// </summary>
        public DateTime GetExDividendDate()
        {
            if (DateTime.TryParseExact(ExDividendDate, "yyyy-MM-dd",
                System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.None, out var date))
            {
                return date;
            }
            return DateTime.MinValue;
        }
    }
}
