/*
 * CASCADELABS.IO
 * Cascade Labs LLC
 */

namespace QuantConnect.Lean.DataSource.Polygon
{
    /// <summary>
    /// Maps LEAN FundamentalProperty enum names to Polygon API financial statement fields
    /// </summary>
    public static class PolygonFinancialPropertyMap
    {
        /// <summary>
        /// Which financial statement a field belongs to
        /// </summary>
        public enum StatementType
        {
            IncomeStatement,
            BalanceSheet,
            CashFlowStatement,
            Computed
        }

        /// <summary>
        /// Parsed result of a LEAN financial property name
        /// </summary>
        public readonly struct PropertyInfo
        {
            public readonly string BaseName;
            public readonly string Period;
            public readonly string PolygonField;
            public readonly StatementType Statement;

            public PropertyInfo(string baseName, string period, string polygonField, StatementType statement)
            {
                BaseName = baseName;
                Period = period;
                PolygonField = polygonField;
                Statement = statement;
            }
        }

        /// <summary>
        /// Maps LEAN property base names (without period suffix) to (polygon_field, statement_type)
        /// </summary>
        private static readonly Dictionary<string, (string PolygonField, StatementType Statement)> PropertyMappings = new()
        {
            // Income Statement
            ["FinancialStatements_IncomeStatement_TotalRevenue"] = ("revenues", StatementType.IncomeStatement),
            ["FinancialStatements_IncomeStatement_CostOfRevenue"] = ("cost_of_revenue", StatementType.IncomeStatement),
            ["FinancialStatements_IncomeStatement_GrossProfit"] = ("gross_profit", StatementType.IncomeStatement),
            ["FinancialStatements_IncomeStatement_OperatingIncome"] = ("operating_income_loss", StatementType.IncomeStatement),
            ["FinancialStatements_IncomeStatement_NetIncome"] = ("net_income_loss", StatementType.IncomeStatement),
            ["FinancialStatements_IncomeStatement_OperatingRevenue"] = ("operating_expenses", StatementType.IncomeStatement),

            // Balance Sheet
            ["FinancialStatements_BalanceSheet_TotalAssets"] = ("assets", StatementType.BalanceSheet),
            ["FinancialStatements_BalanceSheet_CurrentAssets"] = ("current_assets", StatementType.BalanceSheet),
            ["FinancialStatements_BalanceSheet_CurrentLiabilities"] = ("current_liabilities", StatementType.BalanceSheet),
            ["FinancialStatements_BalanceSheet_StockholdersEquity"] = ("equity_attributable_to_parent", StatementType.BalanceSheet),
            ["FinancialStatements_BalanceSheet_TotalEquity"] = ("equity", StatementType.BalanceSheet),

            // Cash Flow Statement
            ["FinancialStatements_CashFlowStatement_OperatingCashFlow"] = ("net_cash_flow_from_operating_activities", StatementType.CashFlowStatement),
            ["FinancialStatements_CashFlowStatement_InvestingCashFlow"] = ("net_cash_flow_from_investing_activities", StatementType.CashFlowStatement),
            ["FinancialStatements_CashFlowStatement_FinancingCashFlow"] = ("net_cash_flow_from_financing_activities", StatementType.CashFlowStatement),
            ["FinancialStatements_CashFlowStatement_CapitalExpenditure"] = ("capital_expenditure", StatementType.CashFlowStatement),

            // Computed fields
            ["FinancialStatements_CashFlowStatement_FreeCashFlow"] = ("__computed_fcf__", StatementType.Computed),
        };

        /// <summary>
        /// Valid period suffixes
        /// </summary>
        private static readonly HashSet<string> ValidPeriods = new()
        {
            "OneMonth", "TwoMonths", "ThreeMonths", "SixMonths", "NineMonths", "TwelveMonths"
        };

        /// <summary>
        /// Fast lookup set for all supported property prefixes
        /// </summary>
        private static readonly HashSet<string> SupportedPrefixes;

        static PolygonFinancialPropertyMap()
        {
            SupportedPrefixes = new HashSet<string>(PropertyMappings.Keys);
        }

        /// <summary>
        /// Returns true if the given property name is a financial statement property that we can serve
        /// </summary>
        public static bool IsFinancialProperty(string? propertyName)
        {
            if (string.IsNullOrEmpty(propertyName))
            {
                return false;
            }

            if (propertyName == "CompanyProfile_MarketCap" || propertyName == "HasFundamentalData")
            {
                return true;
            }

            var parsed = ParsePropertyName(propertyName);
            return parsed.HasValue;
        }

        /// <summary>
        /// Parses a property name like "FinancialStatements_IncomeStatement_TotalRevenue_TwelveMonths"
        /// into its base name and period suffix. Returns null if not a recognized financial property.
        /// </summary>
        public static PropertyInfo? ParsePropertyName(string propertyName)
        {
            if (string.IsNullOrEmpty(propertyName))
            {
                return null;
            }

            // Try each known prefix to find a match
            foreach (var kvp in PropertyMappings)
            {
                var prefix = kvp.Key;
                if (propertyName.StartsWith(prefix) && propertyName.Length > prefix.Length + 1)
                {
                    var period = propertyName.Substring(prefix.Length + 1); // skip the underscore
                    if (ValidPeriods.Contains(period))
                    {
                        return new PropertyInfo(prefix, period, kvp.Value.PolygonField, kvp.Value.Statement);
                    }
                }
            }

            return null;
        }

        /// <summary>
        /// Gets the value from a financial result for a given polygon field name and statement type.
        /// Returns double.NaN if not found.
        /// </summary>
        public static double GetFieldValue(PolygonFinancialResult result, string polygonField, StatementType statement)
        {
            Dictionary<string, PolygonFinancialValue>? dict = statement switch
            {
                StatementType.IncomeStatement => result.Financials.IncomeStatement,
                StatementType.BalanceSheet => result.Financials.BalanceSheet,
                StatementType.CashFlowStatement => result.Financials.CashFlowStatement,
                _ => null
            };

            if (dict != null && dict.TryGetValue(polygonField, out var val))
            {
                return val.Value;
            }

            return double.NaN;
        }

        /// <summary>
        /// Computes Free Cash Flow = Operating Cash Flow - |Capital Expenditure|
        /// </summary>
        public static double ComputeFreeCashFlow(PolygonFinancialResult result)
        {
            var ocf = GetFieldValue(result, "net_cash_flow_from_operating_activities", StatementType.CashFlowStatement);
            var capex = GetFieldValue(result, "capital_expenditure", StatementType.CashFlowStatement);

            if (double.IsNaN(ocf))
            {
                return double.NaN;
            }

            // CapEx is typically negative in Polygon; FCF = OCF + CapEx (which subtracts since CapEx < 0)
            // If capex is missing, we can't compute FCF
            if (double.IsNaN(capex))
            {
                return double.NaN;
            }

            return ocf + capex;
        }

        /// <summary>
        /// Returns true if the statement type represents a balance sheet (stock) item
        /// vs a flow item (income statement, cash flow). Stock items use most recent value
        /// for TTM rather than summing 4 quarters.
        /// </summary>
        public static bool IsStockItem(StatementType statement)
        {
            return statement == StatementType.BalanceSheet;
        }
    }
}
