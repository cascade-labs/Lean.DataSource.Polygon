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
    /// Models a Polygon.io REST API response for ticker events
    /// </summary>
    public class TickerEventsResponse : BaseResponse
    {
        /// <summary>
        /// The results containing ticker event information
        /// </summary>
        [JsonProperty("results")]
        public TickerEventsResults? Results { get; set; }
    }

    /// <summary>
    /// Contains the results of a ticker events query
    /// </summary>
    public class TickerEventsResults
    {
        /// <summary>
        /// The ticker symbol name
        /// </summary>
        [JsonProperty("name")]
        public string? Name { get; set; }

        /// <summary>
        /// The composite FIGI identifier
        /// </summary>
        [JsonProperty("composite_figi")]
        public string? CompositeFigi { get; set; }

        /// <summary>
        /// The CIK number
        /// </summary>
        [JsonProperty("cik")]
        public string? Cik { get; set; }

        /// <summary>
        /// List of ticker events (symbol changes, delistings, etc.)
        /// </summary>
        [JsonProperty("events")]
        public List<TickerEvent>? Events { get; set; }
    }

    /// <summary>
    /// Represents a single ticker event (ticker change, delisting, etc.)
    /// </summary>
    public class TickerEvent
    {
        /// <summary>
        /// The type of event (e.g., "ticker_change", "delisted")
        /// </summary>
        [JsonProperty("type")]
        public string Type { get; set; } = string.Empty;

        /// <summary>
        /// The date of the event (YYYY-MM-DD format)
        /// </summary>
        [JsonProperty("date")]
        public string Date { get; set; } = string.Empty;

        /// <summary>
        /// Details about a ticker change event (present when type is "ticker_change")
        /// </summary>
        [JsonProperty("ticker_change")]
        public TickerChangeInfo? TickerChange { get; set; }

        /// <summary>
        /// Parses the date string to a DateTime
        /// </summary>
        public DateTime GetDate()
        {
            if (DateTime.TryParseExact(Date, "yyyy-MM-dd",
                System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.None, out var date))
            {
                return date;
            }
            return DateTime.MinValue;
        }
    }

    /// <summary>
    /// Contains information about a ticker change event
    /// </summary>
    public class TickerChangeInfo
    {
        /// <summary>
        /// The ticker symbol before the change
        /// </summary>
        [JsonProperty("ticker")]
        public string Ticker { get; set; } = string.Empty;
    }
}
