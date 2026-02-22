using QuantConnect.Data.Market;
using QuantConnect.Logging;
using System.Globalization;

namespace QuantConnect.Lean.DataSource.Polygon
{
    /// <summary>
    /// Parses Polygon flat file CSVs (day_aggs, minute_aggs) into Lean TradeBar objects.
    /// CSV columns: ticker,volume,open,close,high,low,window_start,transactions
    /// </summary>
    public static class PolygonFlatFileParser
    {
        /// <summary>
        /// Parses an aggs CSV stream and yields TradeBars filtered by the underlying ticker.
        /// Times are returned in UTC (caller must convert to data time zone).
        /// </summary>
        public static IEnumerable<TradeBar> ParseAggs(Stream csvStream, string? underlyingTicker,
            PolygonSymbolMapper mapper, TimeSpan period)
        {
            var prefix = underlyingTicker != null ? $"O:{underlyingTicker}" : null;

            using var reader = new StreamReader(csvStream);

            var header = reader.ReadLine();
            if (header == null)
            {
                yield break;
            }

            var columns = header.Split(',');
            var columnIndex = BuildColumnIndex(columns);
            if (columnIndex == null)
            {
                Log.Error($"PolygonFlatFileParser: Unexpected CSV header: {header}");
                yield break;
            }

            string? line;
            var lineNumber = 1;
            while ((line = reader.ReadLine()) != null)
            {
                lineNumber++;
                TradeBar? bar;
                try
                {
                    bar = ParseLine(line, columnIndex, prefix, mapper, period);
                }
                catch (Exception ex)
                {
                    Log.Error($"PolygonFlatFileParser: Error parsing line {lineNumber}: {ex.Message}");
                    continue;
                }

                if (bar != null)
                {
                    yield return bar;
                }
            }
        }

        /// <summary>
        /// Parses an aggs CSV stream and returns all TradeBars grouped by Symbol.
        /// Times are returned in UTC (caller must convert to data time zone).
        /// Within each group, bars are in file order (typically chronological).
        /// </summary>
        public static Dictionary<Symbol, List<TradeBar>> ParseAggsGrouped(Stream csvStream, string underlyingTicker,
            PolygonSymbolMapper mapper, TimeSpan period)
        {
            var result = new Dictionary<Symbol, List<TradeBar>>();
            var prefix = $"O:{underlyingTicker}";

            using var reader = new StreamReader(csvStream);

            var header = reader.ReadLine();
            if (header == null)
            {
                return result;
            }

            var columns = header.Split(',');
            var columnIndex = BuildColumnIndex(columns);
            if (columnIndex == null)
            {
                Log.Error($"PolygonFlatFileParser: Unexpected CSV header: {header}");
                return result;
            }

            string? line;
            var lineNumber = 1;
            while ((line = reader.ReadLine()) != null)
            {
                lineNumber++;
                try
                {
                    var bar = ParseLine(line, columnIndex, prefix, mapper, period);
                    if (bar != null)
                    {
                        if (!result.TryGetValue(bar.Symbol, out var list))
                        {
                            list = new List<TradeBar>();
                            result[bar.Symbol] = list;
                        }
                        list.Add(bar);
                    }
                }
                catch (Exception ex)
                {
                    Log.Error($"PolygonFlatFileParser: Error parsing line {lineNumber}: {ex.Message}");
                }
            }

            return result;
        }

        private static TradeBar? ParseLine(string line, ColumnIndex columnIndex, string? prefix,
            PolygonSymbolMapper mapper, TimeSpan period)
        {
            var fields = line.Split(',');
            if (fields.Length < columnIndex.MinFields)
            {
                return null;
            }

            var ticker = fields[columnIndex.Ticker];

            // Filter by underlying ticker prefix
            if (prefix != null)
            {
                if (!ticker.StartsWith(prefix))
                {
                    return null;
                }

                // Ensure we don't match "SPY" with "SPYG" - next char after prefix must be a digit
                if (ticker.Length > prefix.Length && !char.IsDigit(ticker[prefix.Length]))
                {
                    return null;
                }
            }

            // Only process option tickers
            if (!ticker.StartsWith("O:"))
            {
                return null;
            }

            var symbol = mapper.GetLeanSymbol(ticker);
            return CreateTradeBar(fields, columnIndex, symbol, period);
        }

        private static TradeBar? CreateTradeBar(string[] fields, ColumnIndex idx, Symbol symbol, TimeSpan period)
        {
            if (!decimal.TryParse(fields[idx.Open], NumberStyles.Any, CultureInfo.InvariantCulture, out var open) ||
                !decimal.TryParse(fields[idx.High], NumberStyles.Any, CultureInfo.InvariantCulture, out var high) ||
                !decimal.TryParse(fields[idx.Low], NumberStyles.Any, CultureInfo.InvariantCulture, out var low) ||
                !decimal.TryParse(fields[idx.Close], NumberStyles.Any, CultureInfo.InvariantCulture, out var close) ||
                !decimal.TryParse(fields[idx.Volume], NumberStyles.Any, CultureInfo.InvariantCulture, out var volume))
            {
                return null;
            }

            if (!long.TryParse(fields[idx.WindowStart], NumberStyles.Any, CultureInfo.InvariantCulture, out var windowStartRaw))
            {
                return null;
            }

            // Polygon flat files use nanoseconds for window_start; convert to milliseconds
            var windowStartMs = windowStartRaw > 1_000_000_000_000_000L ? windowStartRaw / 1_000_000 : windowStartRaw;
            var time = Time.UnixMillisecondTimeStampToDateTime(windowStartMs);

            return new TradeBar(time, symbol, open, high, low, close, volume, period);
        }

        private static ColumnIndex? BuildColumnIndex(string[] columns)
        {
            var index = new ColumnIndex();
            for (int i = 0; i < columns.Length; i++)
            {
                switch (columns[i].Trim().ToLowerInvariant())
                {
                    case "ticker": index.Ticker = i; break;
                    case "volume": index.Volume = i; break;
                    case "open": index.Open = i; break;
                    case "close": index.Close = i; break;
                    case "high": index.High = i; break;
                    case "low": index.Low = i; break;
                    case "window_start": index.WindowStart = i; break;
                    case "transactions": index.Transactions = i; break;
                }
            }

            if (index.Ticker == -1 || index.Open == -1 || index.High == -1 ||
                index.Low == -1 || index.Close == -1 || index.Volume == -1 ||
                index.WindowStart == -1)
            {
                return null;
            }

            index.MinFields = Math.Max(Math.Max(Math.Max(Math.Max(Math.Max(Math.Max(
                index.Ticker, index.Volume), index.Open), index.Close),
                index.High), index.Low), index.WindowStart) + 1;

            return index;
        }

        private class ColumnIndex
        {
            public int Ticker = -1;
            public int Volume = -1;
            public int Open = -1;
            public int Close = -1;
            public int High = -1;
            public int Low = -1;
            public int WindowStart = -1;
            public int Transactions = -1;
            public int MinFields;
        }
    }
}
