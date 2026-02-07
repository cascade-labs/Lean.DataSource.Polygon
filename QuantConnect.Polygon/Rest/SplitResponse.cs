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
    /// Models a Polygon.io REST API response containing stock splits
    /// </summary>
    public class SplitResponse : BaseResultsResponse<PolygonSplit>
    {
    }

    /// <summary>
    /// Represents a single stock split from Polygon.io
    /// </summary>
    public class PolygonSplit
    {
        /// <summary>
        /// The execution date of the stock split (YYYY-MM-DD format)
        /// </summary>
        [JsonProperty("execution_date")]
        public string ExecutionDate { get; set; } = string.Empty;

        /// <summary>
        /// The original number of shares before the split
        /// </summary>
        [JsonProperty("split_from")]
        public decimal SplitFrom { get; set; }

        /// <summary>
        /// The new number of shares after the split
        /// </summary>
        [JsonProperty("split_to")]
        public decimal SplitTo { get; set; }

        /// <summary>
        /// The ticker symbol of the stock split
        /// </summary>
        [JsonProperty("ticker")]
        public string Ticker { get; set; } = string.Empty;

        /// <summary>
        /// The split factor: SplitFrom / SplitTo (old_shares / new_shares)
        /// For a 2:1 forward split: SplitFrom=1, SplitTo=2, factor = 0.5
        /// For a 1:10 reverse split: SplitFrom=10, SplitTo=1, factor = 10
        /// This matches LEAN's Split.SplitFactor convention (old_shares/new_shares) and can be passed directly.
        /// </summary>
        public decimal SplitFactor => SplitTo != 0 ? SplitFrom / SplitTo : 1m;

        /// <summary>
        /// Parses the execution date string to a DateTime
        /// </summary>
        public DateTime GetExecutionDate()
        {
            if (DateTime.TryParseExact(ExecutionDate, "yyyy-MM-dd",
                System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.None, out var date))
            {
                return date;
            }
            return DateTime.MinValue;
        }
    }
}
