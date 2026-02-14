/*
 * CASCADELABS.IO
 * Cascade Labs LLC
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Newtonsoft.Json;
using NUnit.Framework;
using QuantConnect.Configuration;

namespace QuantConnect.Lean.DataSource.Polygon.Tests
{
    [TestFixture]
    public class PolygonFinancialDataServiceTests
    {
        #region Sample JSON for deserialization tests

        private const string SampleFinancialsJson = @"{
            ""results"": [
                {
                    ""ticker"": ""AAPL"",
                    ""fiscal_year"": ""2023"",
                    ""fiscal_period"": ""Q1"",
                    ""start_date"": ""2022-10-01"",
                    ""end_date"": ""2022-12-31"",
                    ""filing_date"": ""2023-02-03"",
                    ""timeframe"": ""quarterly"",
                    ""financials"": {
                        ""income_statement"": {
                            ""revenues"": { ""value"": 117154000000, ""unit"": ""USD"", ""label"": ""Revenues"", ""order"": 100 },
                            ""cost_of_revenue"": { ""value"": 66822000000, ""unit"": ""USD"", ""label"": ""Cost Of Revenue"", ""order"": 200 },
                            ""gross_profit"": { ""value"": 50332000000, ""unit"": ""USD"", ""label"": ""Gross Profit"", ""order"": 300 },
                            ""operating_income_loss"": { ""value"": 36016000000, ""unit"": ""USD"", ""label"": ""Operating Income"", ""order"": 400 },
                            ""net_income_loss"": { ""value"": 29998000000, ""unit"": ""USD"", ""label"": ""Net Income"", ""order"": 500 }
                        },
                        ""balance_sheet"": {
                            ""assets"": { ""value"": 346747000000, ""unit"": ""USD"", ""label"": ""Total Assets"", ""order"": 100 },
                            ""current_assets"": { ""value"": 128777000000, ""unit"": ""USD"", ""label"": ""Current Assets"", ""order"": 200 },
                            ""current_liabilities"": { ""value"": 137286000000, ""unit"": ""USD"", ""label"": ""Current Liabilities"", ""order"": 300 },
                            ""equity"": { ""value"": 56727000000, ""unit"": ""USD"", ""label"": ""Total Equity"", ""order"": 400 }
                        },
                        ""cash_flow_statement"": {
                            ""net_cash_flow_from_operating_activities"": { ""value"": 34005000000, ""unit"": ""USD"", ""label"": ""Operating Cash Flow"", ""order"": 100 },
                            ""net_cash_flow_from_investing_activities"": { ""value"": -1445000000, ""unit"": ""USD"", ""label"": ""Investing Cash Flow"", ""order"": 200 },
                            ""net_cash_flow_from_financing_activities"": { ""value"": -35563000000, ""unit"": ""USD"", ""label"": ""Financing Cash Flow"", ""order"": 300 },
                            ""capital_expenditure"": { ""value"": -3787000000, ""unit"": ""USD"", ""label"": ""Capital Expenditure"", ""order"": 400 }
                        }
                    }
                }
            ],
            ""status"": ""OK"",
            ""next_url"": null
        }";

        #endregion

        #region Response Model Deserialization

        [Test]
        public void DeserializesFinancialsResponse()
        {
            var response = JsonConvert.DeserializeObject<FinancialsResponse>(SampleFinancialsJson);

            Assert.That(response, Is.Not.Null);
            Assert.That(response!.Status, Is.EqualTo("OK"));

            var results = response.Results.ToList();
            Assert.That(results, Has.Count.EqualTo(1));

            var result = results[0];
            Assert.That(result.Ticker, Is.EqualTo("AAPL"));
            Assert.That(result.FiscalYear, Is.EqualTo("2023"));
            Assert.That(result.FiscalPeriod, Is.EqualTo("Q1"));
            Assert.That(result.Timeframe, Is.EqualTo("quarterly"));
            Assert.That(result.FilingDate, Is.EqualTo("2023-02-03"));
        }

        [Test]
        public void DeserializesIncomeStatement()
        {
            var response = JsonConvert.DeserializeObject<FinancialsResponse>(SampleFinancialsJson);
            var result = response!.Results.First();

            Assert.That(result.Financials.IncomeStatement, Contains.Key("revenues"));
            Assert.That(result.Financials.IncomeStatement["revenues"].Value, Is.EqualTo(117154000000));
            Assert.That(result.Financials.IncomeStatement["revenues"].Unit, Is.EqualTo("USD"));
            Assert.That(result.Financials.IncomeStatement["net_income_loss"].Value, Is.EqualTo(29998000000));
        }

        [Test]
        public void DeserializesBalanceSheet()
        {
            var response = JsonConvert.DeserializeObject<FinancialsResponse>(SampleFinancialsJson);
            var result = response!.Results.First();

            Assert.That(result.Financials.BalanceSheet, Contains.Key("assets"));
            Assert.That(result.Financials.BalanceSheet["assets"].Value, Is.EqualTo(346747000000));
            Assert.That(result.Financials.BalanceSheet["equity"].Value, Is.EqualTo(56727000000));
        }

        [Test]
        public void DeserializesCashFlowStatement()
        {
            var response = JsonConvert.DeserializeObject<FinancialsResponse>(SampleFinancialsJson);
            var result = response!.Results.First();

            Assert.That(result.Financials.CashFlowStatement, Contains.Key("net_cash_flow_from_operating_activities"));
            Assert.That(result.Financials.CashFlowStatement["net_cash_flow_from_operating_activities"].Value, Is.EqualTo(34005000000));
            Assert.That(result.Financials.CashFlowStatement["capital_expenditure"].Value, Is.EqualTo(-3787000000));
        }

        [Test]
        public void ParsesFilingDate()
        {
            var response = JsonConvert.DeserializeObject<FinancialsResponse>(SampleFinancialsJson);
            var result = response!.Results.First();

            Assert.That(result.GetFilingDate(), Is.EqualTo(new DateTime(2023, 2, 3)));
        }

        [Test]
        public void ParsesEndDate()
        {
            var response = JsonConvert.DeserializeObject<FinancialsResponse>(SampleFinancialsJson);
            var result = response!.Results.First();

            Assert.That(result.GetEndDate(), Is.EqualTo(new DateTime(2022, 12, 31)));
        }

        [Test]
        public void InvalidDateReturnsMinValue()
        {
            var result = new PolygonFinancialResult { FilingDate = "invalid", EndDate = "" };
            Assert.That(result.GetFilingDate(), Is.EqualTo(DateTime.MinValue));
            Assert.That(result.GetEndDate(), Is.EqualTo(DateTime.MinValue));
        }

        #endregion

        #region Property Map Tests

        [Test]
        [TestCase("FinancialStatements_IncomeStatement_TotalRevenue_TwelveMonths", true)]
        [TestCase("FinancialStatements_IncomeStatement_NetIncome_ThreeMonths", true)]
        [TestCase("FinancialStatements_BalanceSheet_TotalAssets_ThreeMonths", true)]
        [TestCase("FinancialStatements_CashFlowStatement_OperatingCashFlow_TwelveMonths", true)]
        [TestCase("FinancialStatements_CashFlowStatement_FreeCashFlow_ThreeMonths", true)]
        [TestCase("CompanyProfile_MarketCap", true)]
        [TestCase("HasFundamentalData", true)]
        [TestCase("Price", false)]
        [TestCase("Volume", false)]
        [TestCase("DollarVolume", false)]
        [TestCase("", false)]
        [TestCase(null!, false)]
        [TestCase("FinancialStatements_IncomeStatement_TotalRevenue", false)] // no period suffix
        [TestCase("FinancialStatements_IncomeStatement_TotalRevenue_InvalidPeriod", false)]
        public void IsFinancialProperty(string propertyName, bool expected)
        {
            Assert.That(PolygonFinancialPropertyMap.IsFinancialProperty(propertyName), Is.EqualTo(expected));
        }

        [Test]
        public void ParsePropertyName_TotalRevenue_TwelveMonths()
        {
            var parsed = PolygonFinancialPropertyMap.ParsePropertyName("FinancialStatements_IncomeStatement_TotalRevenue_TwelveMonths");

            Assert.That(parsed, Is.Not.Null);
            Assert.That(parsed!.Value.BaseName, Is.EqualTo("FinancialStatements_IncomeStatement_TotalRevenue"));
            Assert.That(parsed.Value.Period, Is.EqualTo("TwelveMonths"));
            Assert.That(parsed.Value.PolygonField, Is.EqualTo("revenues"));
            Assert.That(parsed.Value.Statement, Is.EqualTo(PolygonFinancialPropertyMap.StatementType.IncomeStatement));
        }

        [Test]
        public void ParsePropertyName_FreeCashFlow_ThreeMonths()
        {
            var parsed = PolygonFinancialPropertyMap.ParsePropertyName("FinancialStatements_CashFlowStatement_FreeCashFlow_ThreeMonths");

            Assert.That(parsed, Is.Not.Null);
            Assert.That(parsed!.Value.Statement, Is.EqualTo(PolygonFinancialPropertyMap.StatementType.Computed));
        }

        [Test]
        public void ParsePropertyName_BalanceSheet_TotalAssets()
        {
            var parsed = PolygonFinancialPropertyMap.ParsePropertyName("FinancialStatements_BalanceSheet_TotalAssets_ThreeMonths");

            Assert.That(parsed, Is.Not.Null);
            Assert.That(parsed!.Value.PolygonField, Is.EqualTo("assets"));
            Assert.That(parsed.Value.Statement, Is.EqualTo(PolygonFinancialPropertyMap.StatementType.BalanceSheet));
        }

        [Test]
        public void ParsePropertyName_InvalidProperty_ReturnsNull()
        {
            Assert.That(PolygonFinancialPropertyMap.ParsePropertyName("Price"), Is.Null);
            Assert.That(PolygonFinancialPropertyMap.ParsePropertyName(""), Is.Null);
            Assert.That(PolygonFinancialPropertyMap.ParsePropertyName(null!), Is.Null);
        }

        #endregion

        #region GetFieldValue and ComputeFreeCashFlow

        [Test]
        public void GetFieldValue_ReturnsCorrectValue()
        {
            var result = CreateTestFiling(
                filingDate: "2023-02-03",
                revenues: 100000,
                totalAssets: 500000,
                ocf: 50000
            );

            var value = PolygonFinancialPropertyMap.GetFieldValue(result, "revenues", PolygonFinancialPropertyMap.StatementType.IncomeStatement);
            Assert.That(value, Is.EqualTo(100000));
        }

        [Test]
        public void GetFieldValue_MissingField_ReturnsNaN()
        {
            var result = CreateTestFiling(filingDate: "2023-02-03");
            var value = PolygonFinancialPropertyMap.GetFieldValue(result, "nonexistent_field", PolygonFinancialPropertyMap.StatementType.IncomeStatement);
            Assert.That(double.IsNaN(value), Is.True);
        }

        [Test]
        public void ComputeFreeCashFlow_Correct()
        {
            var result = CreateTestFiling(filingDate: "2023-02-03", ocf: 50000, capex: -10000);
            var fcf = PolygonFinancialPropertyMap.ComputeFreeCashFlow(result);

            // FCF = OCF + CapEx = 50000 + (-10000) = 40000
            Assert.That(fcf, Is.EqualTo(40000));
        }

        [Test]
        public void ComputeFreeCashFlow_MissingOCF_ReturnsNaN()
        {
            var result = CreateTestFiling(filingDate: "2023-02-03", capex: -10000);
            var fcf = PolygonFinancialPropertyMap.ComputeFreeCashFlow(result);
            Assert.That(double.IsNaN(fcf), Is.True);
        }

        [Test]
        public void ComputeFreeCashFlow_MissingCapEx_ReturnsNaN()
        {
            var result = CreateTestFiling(filingDate: "2023-02-03", ocf: 50000);
            var fcf = PolygonFinancialPropertyMap.ComputeFreeCashFlow(result);
            Assert.That(double.IsNaN(fcf), Is.True);
        }

        #endregion

        #region Point-in-Time Lookup

        [Test]
        public void PointInTime_ReturnsNaN_BeforeFirstFiling()
        {
            // Filing date is 2023-02-03, querying before that should return NaN
            var filings = new List<PolygonFinancialResult>
            {
                CreateTestFiling("2023-02-03", revenues: 100000, timeframe: "quarterly")
            };

            var service = CreateServiceWithFilings("AAPL", filings);
            var value = service.GetFinancialValue("AAPL", new DateTime(2023, 1, 1),
                "FinancialStatements_IncomeStatement_TotalRevenue_ThreeMonths");

            Assert.That(double.IsNaN(value), Is.True);
        }

        [Test]
        public void PointInTime_ReturnsMostRecentFiling()
        {
            var filings = new List<PolygonFinancialResult>
            {
                CreateTestFiling("2023-02-03", revenues: 100000, timeframe: "quarterly"),
                CreateTestFiling("2023-05-05", revenues: 120000, timeframe: "quarterly"),
            };

            var service = CreateServiceWithFilings("AAPL", filings);

            // Query after second filing: should get 120000
            var value = service.GetFinancialValue("AAPL", new DateTime(2023, 6, 1),
                "FinancialStatements_IncomeStatement_TotalRevenue_ThreeMonths");
            Assert.That(value, Is.EqualTo(120000));

            // Query between first and second filing: should get 100000
            var value2 = service.GetFinancialValue("AAPL", new DateTime(2023, 3, 1),
                "FinancialStatements_IncomeStatement_TotalRevenue_ThreeMonths");
            Assert.That(value2, Is.EqualTo(100000));
        }

        #endregion

        #region TTM Computation

        [Test]
        public void TTM_FlowItem_SumsFourQuarters()
        {
            var filings = new List<PolygonFinancialResult>
            {
                CreateTestFiling("2023-02-03", revenues: 100000, timeframe: "quarterly"),
                CreateTestFiling("2023-05-05", revenues: 110000, timeframe: "quarterly"),
                CreateTestFiling("2023-08-04", revenues: 120000, timeframe: "quarterly"),
                CreateTestFiling("2023-11-03", revenues: 130000, timeframe: "quarterly"),
            };

            var service = CreateServiceWithFilings("AAPL", filings);
            var value = service.GetFinancialValue("AAPL", new DateTime(2023, 12, 1),
                "FinancialStatements_IncomeStatement_TotalRevenue_TwelveMonths");

            Assert.That(value, Is.EqualTo(100000 + 110000 + 120000 + 130000));
        }

        [Test]
        public void TTM_FlowItem_LessThan4Quarters_ReturnsNaN()
        {
            var filings = new List<PolygonFinancialResult>
            {
                CreateTestFiling("2023-02-03", revenues: 100000, timeframe: "quarterly"),
                CreateTestFiling("2023-05-05", revenues: 110000, timeframe: "quarterly"),
                CreateTestFiling("2023-08-04", revenues: 120000, timeframe: "quarterly"),
            };

            var service = CreateServiceWithFilings("AAPL", filings);
            var value = service.GetFinancialValue("AAPL", new DateTime(2023, 12, 1),
                "FinancialStatements_IncomeStatement_TotalRevenue_TwelveMonths");

            Assert.That(double.IsNaN(value), Is.True);
        }

        [Test]
        public void TTM_BalanceSheet_UsesMostRecentValue_NotSum()
        {
            var filings = new List<PolygonFinancialResult>
            {
                CreateTestFiling("2023-02-03", totalAssets: 300000, timeframe: "quarterly"),
                CreateTestFiling("2023-05-05", totalAssets: 310000, timeframe: "quarterly"),
                CreateTestFiling("2023-08-04", totalAssets: 320000, timeframe: "quarterly"),
                CreateTestFiling("2023-11-03", totalAssets: 330000, timeframe: "quarterly"),
            };

            var service = CreateServiceWithFilings("AAPL", filings);
            var value = service.GetFinancialValue("AAPL", new DateTime(2023, 12, 1),
                "FinancialStatements_BalanceSheet_TotalAssets_TwelveMonths");

            // Balance sheet: should return most recent value, not sum
            Assert.That(value, Is.EqualTo(330000));
        }

        [Test]
        public void TTM_FreeCashFlow_Computed_SumsFourQuarters()
        {
            var filings = new List<PolygonFinancialResult>
            {
                CreateTestFiling("2023-02-03", ocf: 30000, capex: -5000, timeframe: "quarterly"),
                CreateTestFiling("2023-05-05", ocf: 32000, capex: -6000, timeframe: "quarterly"),
                CreateTestFiling("2023-08-04", ocf: 28000, capex: -4000, timeframe: "quarterly"),
                CreateTestFiling("2023-11-03", ocf: 35000, capex: -7000, timeframe: "quarterly"),
            };

            var service = CreateServiceWithFilings("AAPL", filings);
            var value = service.GetFinancialValue("AAPL", new DateTime(2023, 12, 1),
                "FinancialStatements_CashFlowStatement_FreeCashFlow_TwelveMonths");

            // FCF = (30000-5000) + (32000-6000) + (28000-4000) + (35000-7000) = 103000
            Assert.That(value, Is.EqualTo(25000 + 26000 + 24000 + 28000));
        }

        #endregion

        #region Unsupported Periods

        [Test]
        [TestCase("FinancialStatements_IncomeStatement_TotalRevenue_OneMonth")]
        [TestCase("FinancialStatements_IncomeStatement_TotalRevenue_TwoMonths")]
        [TestCase("FinancialStatements_IncomeStatement_TotalRevenue_SixMonths")]
        [TestCase("FinancialStatements_IncomeStatement_TotalRevenue_NineMonths")]
        public void UnsupportedPeriods_ReturnNaN(string propertyName)
        {
            var filings = new List<PolygonFinancialResult>
            {
                CreateTestFiling("2023-02-03", revenues: 100000, timeframe: "quarterly"),
            };

            var service = CreateServiceWithFilings("AAPL", filings);
            var value = service.GetFinancialValue("AAPL", new DateTime(2023, 6, 1), propertyName);
            Assert.That(double.IsNaN(value), Is.True);
        }

        #endregion

        #region IsStockItem

        [Test]
        public void BalanceSheet_IsStockItem()
        {
            Assert.That(PolygonFinancialPropertyMap.IsStockItem(PolygonFinancialPropertyMap.StatementType.BalanceSheet), Is.True);
        }

        [Test]
        public void IncomeStatement_IsNotStockItem()
        {
            Assert.That(PolygonFinancialPropertyMap.IsStockItem(PolygonFinancialPropertyMap.StatementType.IncomeStatement), Is.False);
        }

        [Test]
        public void CashFlowStatement_IsNotStockItem()
        {
            Assert.That(PolygonFinancialPropertyMap.IsStockItem(PolygonFinancialPropertyMap.StatementType.CashFlowStatement), Is.False);
        }

        #endregion

        #region Integration Test (requires API key)

        [Test, Explicit("Requires polygon-api-key configuration")]
        public void IntegrationTest_FetchAAPL()
        {
            var apiKey = Config.Get("polygon-api-key");
            if (string.IsNullOrEmpty(apiKey))
            {
                Assert.Ignore("polygon-api-key not configured");
            }

            using var restClient = new PolygonRestApiClient(apiKey);
            var service = new PolygonFinancialDataService(restClient, liveMode: false);

            // Should have financial data
            Assert.That(service.HasFinancialData("AAPL"), Is.True);

            // TotalRevenue TTM should be non-NaN for a recent date
            var revenue = service.GetFinancialValue("AAPL", new DateTime(2024, 6, 1),
                "FinancialStatements_IncomeStatement_TotalRevenue_TwelveMonths");
            Assert.That(double.IsNaN(revenue), Is.False, "TotalRevenue TTM should not be NaN");
            Assert.That(revenue, Is.GreaterThan(0), "TotalRevenue TTM should be positive");

            // OperatingCashFlow quarterly should be non-NaN
            var ocf = service.GetFinancialValue("AAPL", new DateTime(2024, 6, 1),
                "FinancialStatements_CashFlowStatement_OperatingCashFlow_ThreeMonths");
            Assert.That(double.IsNaN(ocf), Is.False, "OperatingCashFlow quarterly should not be NaN");

            // Date before any filing should return NaN
            var earlyRevenue = service.GetFinancialValue("AAPL", new DateTime(1990, 1, 1),
                "FinancialStatements_IncomeStatement_TotalRevenue_ThreeMonths");
            Assert.That(double.IsNaN(earlyRevenue), Is.True, "Very early date should return NaN");
        }

        #endregion

        #region Helpers

        /// <summary>
        /// Creates a test filing with specified values. Only fields you provide will be added.
        /// </summary>
        private static PolygonFinancialResult CreateTestFiling(
            string filingDate,
            double revenues = double.NaN,
            double totalAssets = double.NaN,
            double ocf = double.NaN,
            double capex = double.NaN,
            string timeframe = "quarterly")
        {
            var result = new PolygonFinancialResult
            {
                Ticker = "AAPL",
                FiscalYear = "2023",
                FiscalPeriod = "Q1",
                FilingDate = filingDate,
                EndDate = "2023-01-01",
                Timeframe = timeframe,
                Financials = new PolygonFinancials()
            };

            if (!double.IsNaN(revenues))
            {
                result.Financials.IncomeStatement["revenues"] = new PolygonFinancialValue { Value = revenues, Unit = "USD" };
            }

            if (!double.IsNaN(totalAssets))
            {
                result.Financials.BalanceSheet["assets"] = new PolygonFinancialValue { Value = totalAssets, Unit = "USD" };
            }

            if (!double.IsNaN(ocf))
            {
                result.Financials.CashFlowStatement["net_cash_flow_from_operating_activities"] = new PolygonFinancialValue { Value = ocf, Unit = "USD" };
            }

            if (!double.IsNaN(capex))
            {
                result.Financials.CashFlowStatement["capital_expenditure"] = new PolygonFinancialValue { Value = capex, Unit = "USD" };
            }

            return result;
        }

        /// <summary>
        /// Creates a PolygonFinancialDataService with pre-loaded filings for testing
        /// (bypasses API/disk by injecting directly into the cache via reflection).
        /// </summary>
        private static PolygonFinancialDataService CreateServiceWithFilings(string ticker, List<PolygonFinancialResult> filings)
        {
            // Create service with a dummy rest client (won't be used since we pre-load cache)
            var service = new PolygonFinancialDataService(null!, liveMode: false);

            // Use reflection to inject filings directly into the cache
            var cacheField = typeof(PolygonFinancialDataService).GetField("_cache",
                BindingFlags.NonPublic | BindingFlags.Instance);
            var cache = (ConcurrentDictionary<string, List<PolygonFinancialResult>>)cacheField!.GetValue(service)!;
            cache[ticker.ToUpperInvariant()] = filings;

            var loadedField = typeof(PolygonFinancialDataService).GetField("_loadedAt",
                BindingFlags.NonPublic | BindingFlags.Instance);
            var loaded = (ConcurrentDictionary<string, DateTime>)loadedField!.GetValue(service)!;
            loaded[ticker.ToUpperInvariant()] = DateTime.UtcNow;

            return service;
        }

        #endregion
    }
}
