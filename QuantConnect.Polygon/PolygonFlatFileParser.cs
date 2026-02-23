using NodaTime;
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

        /// <summary>
        /// Parses a stock quotes CSV stream and yields Ticks filtered by the requested ticker.
        /// Times are converted from UTC to the exchange time zone.
        /// </summary>
        public static IEnumerable<Tick> ParseStockQuotes(Stream csvStream, Symbol symbol, string ticker,
            DateTimeZone exchangeTimeZone)
        {
            using var reader = new StreamReader(csvStream);

            var header = reader.ReadLine();
            if (header == null)
            {
                yield break;
            }

            var columns = header.Split(',');
            var columnIndex = BuildQuoteColumnIndex(columns);
            if (columnIndex == null)
            {
                Log.Error($"PolygonFlatFileParser.ParseStockQuotes: Unexpected CSV header: {header}");
                yield break;
            }

            string? line;
            var lineNumber = 1;
            while ((line = reader.ReadLine()) != null)
            {
                lineNumber++;
                Tick? tick;
                try
                {
                    tick = ParseQuoteLine(line, columnIndex, ticker, symbol, exchangeTimeZone);
                }
                catch (Exception ex)
                {
                    Log.Error($"PolygonFlatFileParser.ParseStockQuotes: Error parsing line {lineNumber}: {ex.Message}");
                    continue;
                }

                if (tick != null)
                {
                    yield return tick;
                }
            }
        }

        private static Tick? ParseQuoteLine(string line, QuoteColumnIndex idx, string ticker,
            Symbol symbol, DateTimeZone exchangeTimeZone)
        {
            var fields = line.Split(',');
            if (fields.Length < idx.MinFields)
            {
                return null;
            }

            if (!string.Equals(fields[idx.Ticker], ticker, StringComparison.OrdinalIgnoreCase))
            {
                return null;
            }

            if (!long.TryParse(fields[idx.SipTimestamp], NumberStyles.Any, CultureInfo.InvariantCulture, out var sipTimestampNs))
            {
                return null;
            }

            if (!decimal.TryParse(fields[idx.BidPrice], NumberStyles.Any, CultureInfo.InvariantCulture, out var bidPrice) ||
                !decimal.TryParse(fields[idx.BidSize], NumberStyles.Any, CultureInfo.InvariantCulture, out var bidSize) ||
                !decimal.TryParse(fields[idx.AskPrice], NumberStyles.Any, CultureInfo.InvariantCulture, out var askPrice) ||
                !decimal.TryParse(fields[idx.AskSize], NumberStyles.Any, CultureInfo.InvariantCulture, out var askSize))
            {
                return null;
            }

            var bidExchangeStr = idx.BidExchange >= 0 && idx.BidExchange < fields.Length
                ? fields[idx.BidExchange] : string.Empty;

            // Convert nanosecond timestamp to milliseconds, then to UTC DateTime, then to exchange time zone
            var sipTimestampMs = sipTimestampNs / 1_000_000;
            var utcTime = Time.UnixMillisecondTimeStampToDateTime(sipTimestampMs);
            var exchangeTime = utcTime.ConvertFromUtc(exchangeTimeZone);

            return new Tick(exchangeTime, symbol, string.Empty, bidExchangeStr,
                bidSize, bidPrice, askSize, askPrice);
        }

        private static QuoteColumnIndex? BuildQuoteColumnIndex(string[] columns)
        {
            var index = new QuoteColumnIndex();
            for (int i = 0; i < columns.Length; i++)
            {
                switch (columns[i].Trim().ToLowerInvariant())
                {
                    case "ticker": index.Ticker = i; break;
                    case "sip_timestamp": index.SipTimestamp = i; break;
                    case "bid_price": index.BidPrice = i; break;
                    case "bid_size": index.BidSize = i; break;
                    case "ask_price": index.AskPrice = i; break;
                    case "ask_size": index.AskSize = i; break;
                    case "bid_exchange": index.BidExchange = i; break;
                }
            }

            if (index.Ticker == -1 || index.SipTimestamp == -1 ||
                index.BidPrice == -1 || index.BidSize == -1 ||
                index.AskPrice == -1 || index.AskSize == -1)
            {
                return null;
            }

            index.MinFields = new[] {
                index.Ticker, index.SipTimestamp, index.BidPrice, index.BidSize,
                index.AskPrice, index.AskSize
            }.Max() + 1;

            return index;
        }

        private class QuoteColumnIndex
        {
            public int Ticker = -1;
            public int SipTimestamp = -1;
            public int BidPrice = -1;
            public int BidSize = -1;
            public int AskPrice = -1;
            public int AskSize = -1;
            public int BidExchange = -1;
            public int MinFields;
        }

        /// <summary>
        /// Parses a stock trades CSV stream and yields Ticks filtered by the requested ticker.
        /// Times are converted from UTC to the exchange time zone.
        /// </summary>
        public static IEnumerable<Tick> ParseStockTrades(Stream csvStream, Symbol symbol, string ticker,
            DateTimeZone exchangeTimeZone)
        {
            using var reader = new StreamReader(csvStream);

            var header = reader.ReadLine();
            if (header == null)
            {
                yield break;
            }

            var columns = header.Split(',');
            var columnIndex = BuildTradeColumnIndex(columns);
            if (columnIndex == null)
            {
                Log.Error($"PolygonFlatFileParser.ParseStockTrades: Unexpected CSV header: {header}");
                yield break;
            }

            string? line;
            var lineNumber = 1;
            while ((line = reader.ReadLine()) != null)
            {
                lineNumber++;
                Tick? tick;
                try
                {
                    tick = ParseTradeLine(line, columnIndex, ticker, symbol, exchangeTimeZone);
                }
                catch (Exception ex)
                {
                    Log.Error($"PolygonFlatFileParser.ParseStockTrades: Error parsing line {lineNumber}: {ex.Message}");
                    continue;
                }

                if (tick != null)
                {
                    yield return tick;
                }
            }
        }

        private static Tick? ParseTradeLine(string line, TradeColumnIndex idx, string ticker,
            Symbol symbol, DateTimeZone exchangeTimeZone)
        {
            var fields = line.Split(',');
            if (fields.Length < idx.MinFields)
            {
                return null;
            }

            if (!string.Equals(fields[idx.Ticker], ticker, StringComparison.OrdinalIgnoreCase))
            {
                return null;
            }

            if (!long.TryParse(fields[idx.SipTimestamp], NumberStyles.Any, CultureInfo.InvariantCulture, out var sipTimestampNs))
            {
                return null;
            }

            if (!decimal.TryParse(fields[idx.Price], NumberStyles.Any, CultureInfo.InvariantCulture, out var price) ||
                !decimal.TryParse(fields[idx.Size], NumberStyles.Any, CultureInfo.InvariantCulture, out var size))
            {
                return null;
            }

            var exchangeStr = idx.Exchange >= 0 && idx.Exchange < fields.Length
                ? fields[idx.Exchange] : string.Empty;

            // Convert nanosecond timestamp to milliseconds, then to UTC DateTime, then to exchange time zone
            var sipTimestampMs = sipTimestampNs / 1_000_000;
            var utcTime = Time.UnixMillisecondTimeStampToDateTime(sipTimestampMs);
            var exchangeTime = utcTime.ConvertFromUtc(exchangeTimeZone);

            return new Tick(exchangeTime, symbol, string.Empty, exchangeStr, size, price);
        }

        private static TradeColumnIndex? BuildTradeColumnIndex(string[] columns)
        {
            var index = new TradeColumnIndex();
            for (int i = 0; i < columns.Length; i++)
            {
                switch (columns[i].Trim().ToLowerInvariant())
                {
                    case "ticker": index.Ticker = i; break;
                    case "sip_timestamp": index.SipTimestamp = i; break;
                    case "price": index.Price = i; break;
                    case "size": index.Size = i; break;
                    case "exchange": index.Exchange = i; break;
                }
            }

            if (index.Ticker == -1 || index.SipTimestamp == -1 ||
                index.Price == -1 || index.Size == -1)
            {
                return null;
            }

            index.MinFields = new[] {
                index.Ticker, index.SipTimestamp, index.Price, index.Size
            }.Max() + 1;

            return index;
        }

        private class TradeColumnIndex
        {
            public int Ticker = -1;
            public int SipTimestamp = -1;
            public int Price = -1;
            public int Size = -1;
            public int Exchange = -1;
            public int MinFields;
        }
    }
}
