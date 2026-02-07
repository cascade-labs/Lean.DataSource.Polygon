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
    /// Response from Polygon's /v2/snapshot/locale/us/markets/stocks/tickers API endpoint
    /// Contains snapshot data for all US stocks in a single call
    /// </summary>
    public class StockSnapshotResponse : BaseResponse
    {
        /// <summary>
        /// The number of tickers returned
        /// </summary>
        [JsonProperty("count")]
        public int Count { get; set; }

        /// <summary>
        /// List of stock snapshots
        /// </summary>
        [JsonProperty("tickers")]
        public List<StockSnapshot> Tickers { get; set; } = new();
    }

    /// <summary>
    /// Snapshot data for a single stock ticker
    /// </summary>
    public class StockSnapshot
    {
        /// <summary>
        /// The ticker symbol
        /// </summary>
        [JsonProperty("ticker")]
        public string Ticker { get; set; } = string.Empty;

        /// <summary>
        /// The most recent daily bar for this ticker
        /// </summary>
        [JsonProperty("day")]
        public DayAggregate? Day { get; set; }

        /// <summary>
        /// The previous day's bar for this ticker
        /// </summary>
        [JsonProperty("prevDay")]
        public DayAggregate? PrevDay { get; set; }

        /// <summary>
        /// The most recent minute bar for this ticker
        /// </summary>
        [JsonProperty("min")]
        public DayAggregate? Min { get; set; }

        /// <summary>
        /// The change in price from the previous day
        /// </summary>
        [JsonProperty("todaysChange")]
        public decimal TodaysChange { get; set; }

        /// <summary>
        /// The percent change in price from the previous day
        /// </summary>
        [JsonProperty("todaysChangePerc")]
        public decimal TodaysChangePercent { get; set; }

        /// <summary>
        /// The last updated timestamp in Unix nanoseconds
        /// </summary>
        [JsonProperty("updated")]
        public long Updated { get; set; }
    }

    /// <summary>
    /// Aggregate bar data (daily or minute)
    /// </summary>
    public class DayAggregate
    {
        /// <summary>
        /// The open price
        /// </summary>
        [JsonProperty("o")]
        public decimal Open { get; set; }

        /// <summary>
        /// The high price
        /// </summary>
        [JsonProperty("h")]
        public decimal High { get; set; }

        /// <summary>
        /// The low price
        /// </summary>
        [JsonProperty("l")]
        public decimal Low { get; set; }

        /// <summary>
        /// The close price
        /// </summary>
        [JsonProperty("c")]
        public decimal Close { get; set; }

        /// <summary>
        /// The trading volume
        /// </summary>
        [JsonProperty("v")]
        public decimal Volume { get; set; }

        /// <summary>
        /// The volume weighted average price
        /// </summary>
        [JsonProperty("vw")]
        public decimal VWAP { get; set; }
    }
}
