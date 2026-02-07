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
    /// Response from Polygon's financial ratios API endpoint
    /// </summary>
    public class FinancialRatiosResponse : BaseResultsResponse<FinancialRatios>
    {
    }

    /// <summary>
    /// Financial ratios for a company from Polygon
    /// </summary>
    public class FinancialRatios
    {
        /// <summary>
        /// The ticker symbol
        /// </summary>
        [JsonProperty("ticker")]
        public string Ticker { get; set; } = string.Empty;

        /// <summary>
        /// The market capitalization
        /// </summary>
        [JsonProperty("market_cap")]
        public decimal? MarketCap { get; set; }

        /// <summary>
        /// The price-to-earnings ratio
        /// </summary>
        [JsonProperty("price_earnings_ratio")]
        public decimal? PERatio { get; set; }

        /// <summary>
        /// The price-to-book ratio
        /// </summary>
        [JsonProperty("price_book_ratio")]
        public decimal? PBRatio { get; set; }

        /// <summary>
        /// Return on equity
        /// </summary>
        [JsonProperty("return_on_equity")]
        public decimal? ROE { get; set; }

        /// <summary>
        /// Return on assets
        /// </summary>
        [JsonProperty("return_on_assets")]
        public decimal? ROA { get; set; }

        /// <summary>
        /// The dividend yield
        /// </summary>
        [JsonProperty("dividend_yield")]
        public decimal? DividendYield { get; set; }

        /// <summary>
        /// The price-to-sales ratio
        /// </summary>
        [JsonProperty("price_sales_ratio")]
        public decimal? PSRatio { get; set; }

        /// <summary>
        /// The debt-to-equity ratio
        /// </summary>
        [JsonProperty("debt_equity_ratio")]
        public decimal? DebtEquityRatio { get; set; }

        /// <summary>
        /// The current ratio
        /// </summary>
        [JsonProperty("current_ratio")]
        public decimal? CurrentRatio { get; set; }

        /// <summary>
        /// The enterprise value
        /// </summary>
        [JsonProperty("enterprise_value")]
        public decimal? EnterpriseValue { get; set; }
    }
}
