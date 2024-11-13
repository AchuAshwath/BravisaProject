CREATE SCHEMA IF NOT EXISTS "Reports"
    AUTHORIZATION postgres;

CREATE SCHEMA IF NOT EXISTS dash_process
    AUTHORIZATION postgres;

CREATE SCHEMA IF NOT EXISTS logs
    AUTHORIZATION postgres;

CREATE SCHEMA IF NOT EXISTS mf_analysis
    AUTHORIZATION postgres;

CREATE TABLE public."SchemeNAVCurrentPrices"
(
    "SecurityCode" double precision NOT NULL,
    "Ticker" character varying(255) NOT NULL,
    "DateTime" date NOT NULL,
    "NAVAmount" money,
    "RepurchaseLoad" money,
    "RepurchasePrice" money,
    "SaleLoad" money,
    "SalePrice" money,
    "PrevNAVAmount" money,
    "PrevRepurchasePrice" money,
    "PrevSalePrice" money,
    "PercentageChange" money,
    "Prev1WeekAmount" money,
    "Prev1WeekPer" money,
    "Prev1MonthAmount" money,
    "Prev1MonthPer" money,
    "Prev3MonthsAmount" money,
    "Prev3MonthsPer" money,
    "Prev6MonthsAmount" money,
    "Prev6MonthsPer" money,
    "Prev9MonthsAmount" money,
    "Prev9MonthsPer" money,
    "PrevYearAmount" money,
    "PrevYearPer" money,
    "Prev2YearAmount" money,
    "Prev2YearPer" money,
    "Prev2YearCompPer" money,
    "Prev3YearAmount" money,
    "Prev3YearPer" money,
    "Prev3YearCompPer" money,
    "Prev5YearAmount" money,
    "Prev5YearPer" money,
    "Prev5YearCompPer" money,
    "ListDate" date,
    "ListAmount" money,
    "ListPer" money,
    "ListCompPer" money,
    "FiftyTwoWeekHighNAV" money,
    "FiftyTwoWeekLowNAV" money,
    "FiftyTwoWeekHighNAVDate" date,
    "FiftyTwoWeekLowNAVDate" date,
    "Rank" integer
);


CREATE TABLE public."AnnualGeneralMeeting"
(
    "CompanyCode" double precision NOT NULL,
    "DateOfAnnouncement" date NOT NULL,
    "AGMDate" date,
    "YearEnding" date,
    "Purpose" character varying(50) NOT NULL,
    "RecordDate" date,
    "BookClosureStartDate" date,
    "BookClosureEndDate" date,
    "Remarks" character varying(255),
    "DeleteFlag" boolean,
    "ModifiedDate" timestamp without time zone
);


CREATE TABLE public."Auditors"
(
    "Companycode" double precision NOT NULL,
    "AgencyName" character varying(50),
    "ModifiedDate" timestamp without time zone
);


CREATE TABLE public."BSE"
(
    "SC_CODE" double precision,
    "SC_NAME" character varying,
    "SC_GROUP" character varying(2),
    "SC_TYPE" character(1),
    "OPEN" double precision,
    "HIGH" double precision,
    "LOW" double precision,
    "CLOSE" double precision,
    "LAST" double precision,
    "PREVCLOSE" double precision,
    "NO_TRADES" double precision,
    "NO_OF_SHRS" double precision,
    "NET_TURNOV" double precision,
    "TDCLOINDI" character varying,
    "ISIN_CODE" character varying,
    "TRADING_DATE" date
);


CREATE TABLE public."BTTDivisor"
(
    "IndexName" character varying,
    "BTTIndexValue" double precision,
    "Divisor" double precision,
    "Date" date,
    "OS" bigint
);



CREATE TABLE public."BTTList"
(
    "CompanyName" character varying,
    "ISIN" character varying,
    "NSECode" character varying,
    "BSECode" double precision,
    "Date" date,
    "MFList" boolean,
    "BTTDate" date NOT NULL,
    "CompanyCode" double precision NOT NULL
);




CREATE TABLE public."BackgroundInfo"
(
    "CompanyCode" double precision,
    "CompanyName" character varying,
    "ShortCompanyName" character varying,
    "TickerName" character varying,
    "ParentCompanyCode" double precision,
    "PersonDescription" character varying,
    "PersonName" character varying,
    "ExecutiveDescription" character varying,
    "ExecutiveName" character varying,
    "BSECode" bigint,
    "BSEGroup" character(10),
    "NSECode" character varying,
    "ISINCode" character varying,
    "Sourcetype" character(10),
    "IncorporationDate" date,
    "FirstPublicIssueDate" date,
    "Auditeddate" date,
    "FaceValue" numeric,
    "Marketlot" numeric,
    "IndustryCode" bigint,
    "IndustryName" character varying,
    "BusinessGroupCode" bigint,
    "BusinessGroupName" character varying,
    "MajorSector" integer,
    "latestFinYear" character varying,
    "ForemostFinYear" character varying,
    "LatestHalfYear" character varying,
    "ForemostHalfYear" character varying,
    "LatestQtrYear" character varying,
    "ForemostQtrYear" character varying,
    "ModifiedDate" timestamp without time zone
);


CREATE TABLE public."Bankers"
(
    "Companycode" double precision NOT NULL,
    "AgencyName" character varying(50),
    "ModifiedDate" timestamp without time zone
);


CREATE TABLE public."Blockdeals"
(
    "CompanyCode" double precision,
    "Exchange" character varying,
    "Symbol" character varying,
    "DealDate" date,
    "DealType" character varying,
    "ClientName" character varying,
    "Quantity" bigint,
    "Price" double precision,
    "ModifiedDate" date,
    "DeleteFlag" boolean
);


CREATE TABLE public."BoardMeetings"
(
    "CompanyCode" double precision NOT NULL,
    "BoardMeetDate" date NOT NULL,
    "Purpose" character varying(255),
    "Remarks" character varying(1000),
    "DeleteFlag" boolean,
    "ModifiedDate" timestamp without time zone
);


CREATE TABLE public."Bonus"
(
    "CompanyCode" double precision NOT NULL,
    "DateOfAnnouncement" date NOT NULL,
    "RatioExisting" double precision,
    "RatioOfferred" double precision,
    "RecordDate" date,
    "BookClosureStartDate" date,
    "BookClosureEndDate" date,
    "XBDate" date,
    "Remarks" character varying(255),
    "DeleteFlag" boolean,
    "ModifiedDate" timestamp without time zone
);


CREATE TABLE public."BulkDeals"
(
    "CompanyCode" double precision,
    "Exchange" character varying,
    "Symbol" character varying,
    "DealDate" date,
    "DealType" character varying,
    "ClientName" character varying,
    "Quantity" bigint,
    "Price" double precision,
    "ModifiedDate" date,
    "DeleteFlag" boolean
);


CREATE TABLE public."CapitalStructure"
(
    companycode double precision NOT NULL,
    "FromYear" double precision NOT NULL,
    "ToYear" double precision NOT NULL,
    "InstrumentName" character varying,
    "ReferencePercentage" double precision,
    "AuthorizedShares" double precision,
    "AuthorizedFaceValue" double precision,
    "AuthorizedCapital" double precision,
    "IssuedShares" double precision,
    "IssuedCapital" double precision,
    "NoOfPaidUpShares" double precision,
    "PaidUpFaceValue" double precision,
    "PaidUpCapital" double precision,
    "TotalArrears" double precision,
    "Reasonsforchange" text,
    "ModifiedDate" timestamp without time zone
);


CREATE TABLE public."CompanyMaster"
(
    "CompanyCode" double precision,
    "CompanyName" character varying(75),
    "ShortCompanyName" character varying(20),
    "IndustryCode" smallint,
    "IndustryName" character varying(50),
    "BusinessGroupCode" smallint,
    "BusinessGroupName" character varying(50),
    "IncorporationDate" date,
    "IncorporationYear" character varying(10),
    "FirstPublicIssueDate" date,
    "FirstPublicIssueYear" character varying(10),
    "MajorSector" smallint,
    "CINNo" character varying(30),
    "Remarks" character varying(255),
    "ModifiedDate" timestamp without time zone,
    "DeleteFlag " boolean
);


CREATE TABLE public."CompanyNameChange"
(
    "CompanyCode" double precision NOT NULL,
    "EffectiveDate" date NOT NULL,
    "OldCompanyName" character varying(75) NOT NULL,
    "NewCompanyName" character varying(75) NOT NULL,
    "Remarks" character varying(255),
    "ModifiedDate" timestamp without time zone
);


CREATE TABLE public."ConsolidatedHalfYearlyResults"
(
    "CompanyCode" double precision NOT NULL,
    "YearEnding" date NOT NULL,
    "Months" smallint NOT NULL,
    "Half" smallint NOT NULL,
    "OperatingIncome" double precision,
    "OtherOperatingIncome" double precision,
    "TotalIncomeFromOperations" double precision,
    "IntOrDiscOnAdvOrBills" double precision,
    "IncomeOnInvestements" double precision,
    "IntOnBalancesWithRBI" double precision,
    "Others" double precision,
    "OtherRecurringIncome" double precision,
    "StockAdjustment" double precision,
    "RawMaterialConsumed" double precision,
    "PurchaseOfTradedGoods" double precision,
    "PowerAndFuel" double precision,
    "EmployeeExpenses" double precision,
    "Excise" double precision,
    "AdminAndSellingExpenses" double precision,
    "ResearchAndDevelopmentExpenses" double precision,
    "ExpensesCapitalised" double precision,
    "OtherExpeses" double precision,
    "PL_Before_OtherInc_Int_Excpltem_Tax" double precision,
    "PL_Before_Int_ExcpItem_Tax" double precision,
    "InterestCharges" double precision,
    "PL_Before_ExcpItem_Tax" double precision,
    "ExceptionalItems" double precision,
    "Depreciation" double precision,
    "OperatingProfitBeforeProvisionsAndContingencies" double precision,
    "BankProvisionsMade" double precision,
    "PL_Before_Tax" double precision,
    "TaxCharges" double precision,
    "PL_After_TaxFromOrdineryActivities" double precision,
    "ExtraOrdinaryItem" double precision,
    "ReportedPAT" double precision,
    "MinorityInterest" double precision,
    "ShareOfPLOfAssociates" double precision,
    "NetPLAfterMIAssociates" double precision,
    "CostOfInvestInSubsidiary" double precision,
    "PriorYearAdj" double precision,
    "ReservesWrittenBack" double precision,
    "EquityCapital" double precision,
    "ReservesAndSurplus" double precision,
    "EqDividendRate" double precision,
    "AggregateOfNonPromotoNoOfShares" double precision,
    "AggregateOfNonPromotoHoldingPercent" double precision,
    "GovernmentShare" double precision,
    "CapitalAdequacyRatio" double precision,
    "CapitalAdequacyBasell" double precision,
    "GrossNPA" double precision,
    "NetNPA" double precision,
    "Per_OfGrossNPA" double precision,
    "Per_OfNetNPA" double precision,
    "ReturnOnAssets_Per" double precision,
    "BeforeBasicEPS" double precision,
    "asBeforeDilutedEPS" double precision,
    "AfterBasicEPS" double precision,
    "AfterDilutedEPS" double precision,
    "En_NumberOfShares" double precision,
    "En_Per_Share_As_Per_Of_Tot_Sh_Hol_of_Pro_And_Group" double precision,
    "En_Per_Share_As_Per_Of_Tot_Sh_Cap_of_Company" double precision,
    "NonEn_NumberOfShares" double precision,
    "NonEn_Per_Share_As_Per_Of_Tot_Sh_Hol_of_Pro_And_Group" double precision,
    "NonEn_Per_Share_As_Per_Of_Tot_Sh_Cap_of_Company" double precision,
    "ModifiedDate" timestamp without time zone
);


CREATE TABLE public."ConsolidatedNineMonthsResults"
(
    "CompanyCode" double precision NOT NULL,
    "YearEnding" date NOT NULL,
    "Months" smallint NOT NULL,
    "Nine" smallint NOT NULL,
    "OperatingIncome" double precision,
    "OtherOperatingIncome" double precision,
    "TotalIncomeFromOperations" double precision,
    "IntOrDiscOnAdvOrBills" double precision,
    "IncomeOnInvestements" double precision,
    "IntOnBalancesWithRBI" double precision,
    "Others" double precision,
    "OtherRecurringIncome" double precision,
    "StockAdjustment" double precision,
    "RawMaterialConsumed" double precision,
    "PurchaseOfTradedGoods" double precision,
    "PowerAndFuel" double precision,
    "EmployeeExpenses" double precision,
    "Excise" double precision,
    "AdminAndSellingExpenses" double precision,
    "ResearchAndDevelopmentExpenses" double precision,
    "ExpensesCapitalised" double precision,
    "OtherExpeses" double precision,
    "PL_Before_OtherInc_Int_Excpltem_Tax" double precision,
    "PL_Before_Int_ExcpItem_Tax" double precision,
    "InterestCharges" double precision,
    "PL_Before_ExcpItem_Tax" double precision,
    "ExceptionalItems" double precision,
    "Depreciation" double precision,
    "OperatingProfitBeforeProvisionsAndContingencies" double precision,
    "BankProvisionsMade" double precision,
    "PL_Before_Tax" double precision,
    "TaxCharges" double precision,
    "PL_After_TaxFromOrdineryActivities" double precision,
    "ExtraOrdinaryItem" double precision,
    "ReportedPAT" double precision,
    "MinorityInterest" double precision,
    "ShareOfPLOfAssociates" double precision,
    "NetPLAfterMIAssociates" double precision,
    "CostOfInvestInSubsidiary" double precision,
    "PriorYearAdj" double precision,
    "ReservesWrittenBack" double precision,
    "EquityCapital" double precision,
    "ReservesAndSurplus" double precision,
    "EqDividendRate" double precision,
    "AggregateOfNonPromotoNoOfShares" double precision,
    "AggregateOfNonPromotoHoldingPercent" double precision,
    "GovernmentShare" double precision,
    "CapitalAdequacyRatio" double precision,
    "CapitalAdequacyBasell" double precision,
    "GrossNPA" double precision,
    "NetNPA" double precision,
    "Per_OfGrossNPA" double precision,
    "Per_OfNetNPA" double precision,
    "ReturnOnAssets_Per" double precision,
    "BeforeBasicEPS" double precision,
    "asBeforeDilutedEPS" double precision,
    "AfterBasicEPS" double precision,
    "AfterDilutedEPS" double precision,
    "En_NumberOfShares" double precision,
    "En_Per_Share_As_Per_Of_Tot_Sh_Hol_of_Pro_And_Group" double precision,
    "En_Per_Share_As_Per_Of_Tot_Sh_Cap_of_Company" double precision,
    "NonEn_NumberOfShares" double precision,
    "NonEn_Per_Share_As_Per_Of_Tot_Sh_Hol_of_Pro_And_Group" double precision,
    "NonEn_Per_Share_As_Per_Of_Tot_Sh_Cap_of_Company" double precision,
    "ModifiedDate" timestamp without time zone
);


CREATE TABLE public."ConsolidatedQuarterlyResults"
(
    "CompanyCode" double precision NOT NULL,
    "YearEnding" date NOT NULL,
    "Months" smallint NOT NULL,
    "Quarter" smallint NOT NULL,
    "OperatingIncome" double precision,
    "OtherOperatingIncome" double precision,
    "TotalIncomeFromOperations" double precision,
    "IntOrDiscOnAdvOrBills" double precision,
    "IncomeOnInvestements" double precision,
    "IntOnBalancesWithRBI" double precision,
    "Others" double precision,
    "OtherRecurringIncome" double precision,
    "StockAdjustment" double precision,
    "RawMaterialConsumed" double precision,
    "PurchaseOfTradedGoods" double precision,
    "PowerAndFuel" double precision,
    "EmployeeExpenses" double precision,
    "Excise" double precision,
    "AdminAndSellingExpenses" double precision,
    "ResearchAndDevelopmentExpenses" double precision,
    "ExpensesCapitalised" double precision,
    "OtherExpeses" double precision,
    "PL_Before_OtherInc_Int_Excpltem_Tax" double precision,
    "PL_Before_Int_ExcpItem_Tax" double precision,
    "InterestCharges" double precision,
    "PL_Before_ExcpItem_Tax" double precision,
    "ExceptionalItems" double precision,
    "Depreciation" double precision,
    "OperatingProfitBeforeProvisionsAndContingencies" double precision,
    "BankProvisionsMade" double precision,
    "PL_Before_Tax" double precision,
    "TaxCharges" double precision,
    "PL_After_TaxFromOrdineryActivities" double precision,
    "ExtraOrdinaryItem" double precision,
    "ReportedPAT" double precision,
    "MinorityInterest" double precision,
    "ShareOfPLOfAssociates" double precision,
    "NetPLAfterMIAssociates" double precision,
    "CostOfInvestInSubsidiary" double precision,
    "PriorYearAdj" double precision,
    "ReservesWrittenBack" double precision,
    "EquityCapital" double precision,
    "ReservesAndSurplus" double precision,
    "EqDividendRate" double precision,
    "AggregateOfNonPromotoNoOfShares" double precision,
    "AggregateOfNonPromotoHoldingPercent" double precision,
    "GovernmentShare" double precision,
    "CapitalAdequacyRatio" double precision,
    "CapitalAdequacyBasell" double precision,
    "GrossNPA" double precision,
    "NetNPA" double precision,
    "Per_OfGrossNPA" double precision,
    "Per_OfNetNPA" double precision,
    "ReturnOnAssets_Per" double precision,
    "BeforeBasicEPS" double precision,
    "asBeforeDilutedEPS" double precision,
    "AfterBasicEPS" double precision,
    "AfterDilutedEPS" double precision,
    "En_NumberOfShares" double precision,
    "En_Per_Share_As_Per_Of_Tot_Sh_Hol_of_Pro_And_Group" double precision,
    "En_Per_Share_As_Per_Of_Tot_Sh_Cap_of_Company" double precision,
    "NonEn_NumberOfShares" double precision,
    "NonEn_Per_Share_As_Per_Of_Tot_Sh_Hol_of_Pro_And_Group" numeric,
    "NonEn_Per_Share_As_Per_Of_Tot_Sh_Cap_of_Company" double precision,
    "ModifiedDate" timestamp without time zone
);



CREATE TABLE public."ConsolidatedQuarterlyEPS"
(
    "CompanyCode" double precision NOT NULL,
    "YearEnding" date NOT NULL,
    "Months" smallint,
    "Quarter" smallint,
    "Sales" double precision,
    "Expenses" double precision,
    "EBIDTA" double precision,
    "Interest" double precision,
    "Depreciation" double precision,
    "Extraordinary" double precision,
    "OPM" double precision,
    "Tax" double precision,
    "PATRAW" double precision,
    "PAT" double precision,
    "Equity" double precision,
    "Reserves" double precision,
    "EPS" double precision,
    "NPM" double precision,
    "Ext_Flag" boolean,
    "Q1 EPS Growth" double precision,
    "Q1 Sales Growth" double precision,
    "Q2 EPS" double precision,
    "Q2 EPS Growth" double precision,
    "Q2 Sales" double precision,
    "Q2 Sales Growth" double precision,
    "E_ERS" double precision
);


CREATE TABLE public."ConsolidatedTTM"
(
    "CompanyCode" double precision NOT NULL,
    "YearEnding" date NOT NULL,
    "Months" smallint,
    "Quarter" smallint,
    "Sales" double precision,
    "Expenses" double precision,
    "EBIDTA" double precision,
    "Interest" double precision,
    "Depreciation" double precision,
    "Extraordinary" double precision,
    "OPM" double precision,
    "Tax" double precision,
    "PAT" double precision,
    "Equity" double precision,
    "Reserves" double precision,
    "EPS" double precision,
    "NPM" double precision,
    "E_ERS" double precision
);


CREATE TABLE public."CurrentData"
(
    "Companycode" double precision,
    "ResultType" character(2),
    "Yearending" date,
    "Months" integer,
    "Facevalue" double precision,
    "BonusEquity" double precision,
    "NumberOfEquityShares" double precision,
    "CurrentEquity" double precision,
    "CurrentReserves" double precision,
    "TotalDebt" double precision,
    "CashAndBankBalance" double precision,
    "Sales" double precision,
    "PrevYearSales" double precision,
    "OperatingProfit" double precision,
    "GrossProfit" double precision,
    "Depriciation" double precision,
    "Tax" double precision,
    "ReportedPAT" double precision,
    "PrevYearReportedPAT" double precision,
    "Dividend" double precision,
    "DividendPerShare" double precision,
    "EPS" double precision,
    "BookValue" double precision,
    "Price" double precision,
    "PriceDate" date,
    "IndustryPE" double precision,
    "BetaValue" double precision,
    "PledgedShares" double precision,
    "TrailingYear" date,
    "LastFourQuarterersSales" double precision,
    "LastFourQuartersTotalExpenditure" numeric,
    "LastFourQuartersInterestCharges" double precision,
    "LastFourQuartersGrossProfit" double precision,
    "LastFourQuartersDepreciation" double precision,
    "LastFourQuartersBankProvisionsMade" double precision,
    "LastFourQuartersTaxCharges" double precision,
    "LastFourQuartersExtraOrdinaryItem" double precision,
    "LastFourQuartersReportedPAT" double precision,
    "LastFourQuartersPriorYearAdj" double precision,
    "TrailingEPS" double precision,
    "TrailingCEPS" double precision,
    "ModifiedDate" timestamp without time zone
);


CREATE TABLE public."Dividend"
(
    "CompanyCode" double precision NOT NULL,
    "DateOfAnnouncement" date NOT NULL,
    "InterimOrFinal" character varying(20),
    "InstrumentType" smallint,
    "InstrumentTypeDescription" character varying(100),
    "Percentage" numeric,
    "Value" numeric,
    "RecordDate" date,
    "BookClosureStartDate" date,
    "BookClosureEndDate" date,
    "XDDate" date,
    "Remarks" character varying(255),
    "DelFlag" boolean,
    "ModifiedDate" timestamp with time zone
);


CREATE TABLE public."ErrorLog"
(
    "TIMESTAMP" timestamp without time zone NOT NULL,
    "STATUS" character varying,
    "REPORT" character varying
);


CREATE TABLE public."FinanceBankingVI"
(
    "Companycode" double precision NOT NULL,
    "YearEnding" date NOT NULL,
    "Months" smallint NOT NULL,
    "Type" character(2) NOT NULL,
    "FaceValue" numeric,
    "InterestDiscountOnAdvancesBills" numeric,
    "IncomeFromInvestments" numeric,
    "InterestOnBalanceWithRBIAndOtherInterBankFunds" numeric,
    "Others" numeric,
    "TotalInterestEarned" numeric,
    "OtherIncome" numeric,
    "TotalIncome" numeric,
    "InterestExpended" numeric,
    "PaymentsToAndProvisionsForEmployees" numeric,
    "Depreciation" numeric,
    "DepreciationOnLeasedAssets" numeric,
    "OperatingExpensesExcludesEmployeeCostAndDepreciation" numeric,
    "TotalOperatingExpenses" numeric,
    "ProvisionTowardsIncomeTax" numeric,
    "ProvisionTowardsDeferredTax" numeric,
    "ProvisionTowardsOtherTaxes" numeric,
    "OtherProvisionsAndContingencies" numeric,
    "TotalProvisionsAndContingencies" numeric,
    "TotalExpenditure" numeric,
    "NetProfitLossForTheYear" numeric,
    "PriorPeriodItems" numeric,
    "ExtraordinaryItems" numeric,
    "NetProfitLossForTheYearAfterEI" numeric,
    "ProfitLossBroughtForward" numeric,
    "TransferredOnAmalgamation" numeric,
    "TotalProfitLoss" numeric,
    "TransferToFromStatutoryReserve" numeric,
    "TransferToFromReserveFund" numeric,
    "TransferToFromSpecialReserve" numeric,
    "TransferToFromCapitalReserve" numeric,
    "TransferToFromGeneralReserve" numeric,
    "TransferToFromInvestmentReserve" numeric,
    "TransferToFromRevenueAndOtherReserves" numeric,
    "DividendForThePreviousYear" numeric,
    "EquityShareDividend" numeric,
    "PreferenceShareDividend" numeric,
    "TaxOnDividend" numeric,
    "BalanceCarriedOverToBalanceSheet" numeric,
    "TotalAppropriations" numeric,
    "EquityShares" numeric,
    "EquityCapital" numeric,
    "PreferenceCapital" numeric,
    "TotalShareCapital" numeric,
    "RevaluationReserves" numeric,
    "ReservesAndSurplus" numeric,
    "TotalReservesAndSurplus" numeric,
    "MoneyAgainstShareWarrants" numeric,
    "EmployeesStockOptions" numeric,
    "TotalShareHoldersFunds" numeric,
    "EquityShareApplicationMoney" numeric,
    "PrefShareApplicationMoney" numeric,
    "ShareCapitalSuspense" numeric,
    "Deposits" numeric,
    "Borrowings" numeric,
    "OtherLiabilitiesAndProvisions" numeric,
    "TotalCapitalAndLiabilities" numeric,
    "CashAndBalancesWithReserveBankOfIndia" numeric,
    "BalancesWithBanksMoneyAtCallAndShortNotice" numeric,
    "Investments" numeric,
    "Advances" numeric,
    "FixedAssets" numeric,
    "OtherAssets" numeric,
    "TotalAssets" numeric,
    "NetProfitLossBeforeExtraordinaryItemsAndTax" numeric,
    "NetCashFlowFromOperatingActivities" numeric,
    "NetCashUsedInInvestingActivities" numeric,
    "NetCashUsedFromFinancingActivities" numeric,
    "ForeignExchangeGainsLosses" numeric,
    "NetIncDecInCashAndCashEquivalents" numeric,
    "CashAndCashEquivalentBeginOfYear" numeric,
    "CashAndCashEquivalentEndOfYear" numeric,
    "BasicEPS" numeric,
    "DilutedEPS" numeric,
    "NumberOfBranches" numeric,
    "NumberOfEmployees" numeric,
    "CapitalAdequacyRatios" numeric,
    "KeyPerformanceTier1" numeric,
    "KeyPerformanceTier2" numeric,
    "GrossNPARs" numeric,
    "GrossNPAPercentage" numeric,
    "NetNPARs" numeric,
    "NetNPAPercentage" numeric,
    "NetNPAToAdvancesPercentage" numeric,
    "BillsForCollection" numeric,
    "ContingentLiabilities" numeric,
    "EquityDividendRate" numeric,
    "BonusEquityShareCapital" numeric,
    "ModifiedDate" timestamp without time zone
);


CREATE TABLE public."FinanceConsolidatedBankingVI"
(
    "Companycode" double precision NOT NULL,
    "YearEnding" date NOT NULL,
    "Months" smallint NOT NULL,
    "Type" character(1) NOT NULL,
    "FaceValue" numeric,
    "InterestDiscountOnAdvancesBills" numeric,
    "IncomeFromInvestments" numeric,
    "InterestOnBalanceWithRBIAndOtherInterBankFunds" numeric,
    "Others" numeric,
    "TotalInterestEarned" numeric,
    "OtherIncome" numeric,
    "TotalIncome" numeric,
    "InterestExpended" numeric,
    "PaymentsToAndProvisionsForEmployees" numeric,
    "Depreciation" numeric,
    "DepreciationOnLeasedAssets" numeric,
    "OperatingExpensesExcludesEmployeeCostAndDepreciation" numeric,
    "TotalOperatingExpenses" numeric,
    "ProvisionTowardsIncomeTax" numeric,
    "ProvisionTowardsDeferredTax" numeric,
    "ProvisionTowardsOtherTaxes" numeric,
    "OtherProvisionsAndContingencies" numeric,
    "TotalProvisionsAndContingencies" numeric,
    "TotalExpenditure" numeric,
    "NetProfitLossForTheYear" numeric,
    "PriorPeriodItems" numeric,
    "ExtraordinaryItems" numeric,
    "NetProfitLossForTheYearAfterEI" numeric,
    "IncomeMinorityInterest" numeric,
    "ShareOfProfitLossFromAssociate" numeric,
    "ProfitLossAfterMIAndAssociates" numeric,
    "ProfitLossBroughtForward" numeric,
    "TransferredOnAmalgamation" numeric,
    "TotalProfitLoss" numeric,
    "TransferToFromStatutoryReserve" numeric,
    "TransferToFromReserveFund" numeric,
    "TransferToFromSpecialReserve" numeric,
    "TransferToFromCapitalReserve" numeric,
    "TransferToFromGeneralReserve" numeric,
    "TransferToFromInvestmentReserve" numeric,
    "TransferToFromRevenueAndOtherReserves" numeric,
    "DividendForThePreviousYear" numeric,
    "EquityShareDividend" numeric,
    "PreferenceShareDividend" numeric,
    "TaxOnDividend" numeric,
    "BalanceCarriedOverToBalanceSheet" numeric,
    "TotalAppropriations" numeric,
    "EquityShares" numeric,
    "EquityCapital" numeric,
    "PreferenceCapital" numeric,
    "TotalShareCapital" numeric,
    "RevaluationReserves" numeric,
    "ReservesAndSurplus" numeric,
    "TotalReservesAndSurplus" numeric,
    "MoneyAgainstShareWarrants" numeric,
    "EmployeesStockOptions" numeric,
    "TotalShareHoldersFunds" numeric,
    "EquityShareApplicationMoney" numeric,
    "PrefShareApplicationMoney" numeric,
    "ShareCapitalSuspense" numeric,
    "LiabilityMinorityInterest" numeric,
    "PolicyHoldersFunds" numeric,
    "LiabilityGroupShareInJointVentures" numeric,
    "Deposits" numeric,
    "Borrowings" numeric,
    "OtherLiabilitiesAndProvisions" numeric,
    "TotalCapitalAndLiabilities" numeric,
    "CashAndBalancesWithReserveBankOfIndia" numeric,
    "BalancesWithBanksMoneyAtCallAndShortNotice" numeric,
    "Investments" numeric,
    "Advances" numeric,
    "FixedAssets" numeric,
    "OtherAssets" numeric,
    "AssetMinorityInterest" numeric,
    "AssetGroupShareInJointVentures" numeric,
    "TotalAssets" numeric,
    "NetProfitLossBeforeExtraordinaryItemsAndTax" numeric,
    "NetCashFlowFromOperatingActivities" numeric,
    "NetCashUsedInInvestingActivities" numeric,
    "NetCashUsedFromFinancingActivities" numeric,
    "ForeignExchangeGainsLosses" numeric,
    "NetIncDecInCashAndCashEquivalents" numeric,
    "CashAndCashEquivalentBeginOfYear" numeric,
    "CashAndCashEquivalentEndOfYear" numeric,
    "BasicEPS" numeric,
    "DilutedEPS" numeric,
    "NumberOfBranches" numeric,
    "NumberOfEmployees" numeric,
    "CapitalAdequacyRatios" numeric,
    "KeyPerformanceTier1" numeric,
    "KeyPerformanceTier2" numeric,
    "GrossNPARs" numeric,
    "GrossNPAPercentage" numeric,
    "NetNPARs" numeric,
    "NetNPAPercentage" numeric,
    "NetNPAToAdvancesPercentage" numeric,
    "BillsForCollection" numeric,
    "ContingentLiabilities" numeric,
    "BonusEquityShareCapital" numeric,
    "ModifiedDate" timestamp without time zone
);


CREATE TABLE public."FinanceConsolidatedNonBankingVI"
(
    "Companycode" double precision NOT NULL,
    "YearEnding" date NOT NULL,
    "Months" smallint NOT NULL,
    "Type" character(1) NOT NULL,
    "FaceValue" numeric,
    "RevenueFromOperationsGross" numeric,
    "LessExciseSeviceTaxOtherLevies" numeric,
    "RevenueFromOperationsNet" numeric,
    "OtherOperatingRevenues" numeric,
    "TotalOperatingRevenues" numeric,
    "OtherIncome" numeric,
    "GroupShareInJointVenturesInIncome" numeric,
    "TotalRevenue" numeric,
    "CostOfMaterialsConsumed" numeric,
    "PurchaseOfStockInTrade" numeric,
    "PurchaseOfCrudeOilAndOthers" numeric,
    "CostOfPowerPurchased" numeric,
    "CostOfFuel" numeric,
    "AircraftFuelExpenses" numeric,
    "AircraftLeaseRentals" numeric,
    "OperatingAndDirectExpenses" numeric,
    "ChangesIninventoriesOfFGWIPAndStockInTrade" numeric,
    "EmployeeBenefitExpenses" numeric,
    "FinanceCosts" numeric,
    "ProvisionsAndContingencies" numeric,
    "DepreciationAndAmortisationExpenses" numeric,
    "MiscExpensesWOff" numeric,
    "OtherExpenses" numeric,
    "LessInterUnitSegmentDivisionTransfer" numeric,
    "LessTransferToFromInvestmentFixedAssetsOthers" numeric,
    "LessAmountsTransferToCapitalAccounts" numeric,
    "LessShareOfLossFromPartnershipFirm" numeric,
    "GroupShareInJointVenturesInExpenditure" numeric,
    "TotalExpenses" numeric,
    "ProfitLossBeforeExceptionalExtraOrdinaryItemsAndTax" numeric,
    "ExceptionalItems" numeric,
    "ProfitLossBeforeTax" numeric,
    "CurrentTax" numeric,
    "LessMATCredit" numeric,
    "DeferredTax" numeric,
    "OtherDirectTaxes" numeric,
    "TaxForEarlierYears" numeric,
    "TotalTaxExpensesContinuedOperations" numeric,
    "ProfitLossAfterTaxAndBeforeExtraOrdinaryItems" numeric,
    "PriorPeriodItems" numeric,
    "ExtraordinaryItems" numeric,
    "ProfitLossFromContinuingOperations" numeric,
    "ProfitLossFromDiscontinuingOperations" numeric,
    "TotalTaxExpensesDiscontinuingOperations" numeric,
    "NetProfitLossFromDiscontinuingOperations" numeric,
    "ProfitLossForThePeriod" numeric,
    "IncomeMinorityInterest" numeric,
    "ShareOfProfitLossOfAssociates" numeric,
    "ProfitLossAfterMIAndAssociates" numeric,
    "EquityShares" numeric,
    "EquityCapital" numeric,
    "PreferenceCapital" numeric,
    "TotalShareCapital" numeric,
    "RevaluationReserves" numeric,
    "ReservesAndSurplus" numeric,
    "TotalReservesAndSurplus" numeric,
    "MoneyAgainstShareWarrants" numeric,
    "EmployeesStockOptions" numeric,
    "TotalShareHoldersFunds" numeric,
    "PrefSharesIssuedBySubsidiaryCos" numeric,
    "EquityShareApplicationMoney" numeric,
    "PreferenceShareApplicationMoney" numeric,
    "ShareCapitalSuspense" numeric,
    "HybridDebtOtherSecurities" numeric,
    "StatutoryConsumerReserves" numeric,
    "SpecialApprnTowardsProjectCost" numeric,
    "ServiceLineContributionFromConsumers" numeric,
    "GovernmentOtherGrants" numeric,
    "LiabilityMinorityInterest" numeric,
    "PolicyHoldersFunds" numeric,
    "LiabilityGroupShareInJointVentures" numeric,
    "LongTermBorrowings" numeric,
    "DeferredTaxLiabilities" numeric,
    "OtherLongTermLiabilities" numeric,
    "LongTermProvisions" numeric,
    "TotalNonCurrentLiabilities" numeric,
    "ForeignCurrencyMonetaryItemTranslationDifferenceLiabilityAccoun" numeric,
    "ShortTermBorrowings" numeric,
    "TradePayables" numeric,
    "OtherCurrentLiabilities" numeric,
    "ShortTermProvisions" numeric,
    "TotalCurrentLiabilities" numeric,
    "TotalCapitalAndLiabilities" numeric,
    "TangibleAssets" numeric,
    "IntangibleAssets" numeric,
    "CapitalWorkInProgress" numeric,
    "IntangibleAssetsUnderDevelopment" numeric,
    "OtherAssets" numeric,
    "NonCurrentInvestmentsUnquotedBookValue" numeric,
    "ConstructionStores" numeric,
    "MiningDevelopmentExpenditure" numeric,
    "AssetsHeldForSale" numeric,
    "TotalFixedAssets" numeric,
    "GoodwillOnConsolidation" numeric,
    "NonCurrentInvestments" numeric,
    "DeferredTaxAssets" numeric,
    "LongTermLoansAndAdvances" numeric,
    "OtherNonCurrentAssets" numeric,
    "TotalNonCurrentAssets" numeric,
    "AssetMinorityInterest" numeric,
    "AssetGroupShareInJointVentures" numeric,
    "ForeignCurrencyMonetaryItemTranslationDiffAcct" numeric,
    "CurrentInvestments" numeric,
    "Inventories" numeric,
    "TradeReceivables" numeric,
    "CashAndCashEquivalents" numeric,
    "ShortTermLoansAndAdvances" numeric,
    "OtherCurrentAssets" numeric,
    "TotalCurrentAssets" numeric,
    "TotalAssets" numeric,
    "NetProfitLossBeforeExtraordinaryItemsAndTax" numeric,
    "NetCashFlowFromOperatingActivities" numeric,
    "NetCashUsedInInvestingActivities" numeric,
    "NetCashUsedFromFinancingActivities" numeric,
    "ForeignExchangeGainsLosses" numeric,
    "AdjustmentsOnAmalgamationMergerDemergerOthers" numeric,
    "NetIncDecInCashAndCashEquivalents" numeric,
    "CashAndCashEquivalentBeginOfYear" numeric,
    "CashAndCashEquivalentEndOfYear" numeric,
    "BasicEPS" numeric,
    "DilutedEPS" numeric,
    "ContingentLiabilities" numeric,
    "CIFValueOfRawMaterials" numeric,
    "CIFValueOfStoresSparesAndLooseTools" numeric,
    "CIFValueOfOtherGoods" numeric,
    "CIFValueOfCapitalGoods" numeric,
    "ExpenditureInForeignCurrency" numeric,
    "DividendRemittanceInForeignCurrency" numeric,
    "ForeignExchangeFOB" numeric,
    "ForeignExchangeOtherEarnings" numeric,
    "ImportedRawMaterials" numeric,
    "IndigenousRawMaterials" numeric,
    "ImportedStoresAndSpares" numeric,
    "IndigenousStoresAndSpares" numeric,
    "EquityShareDividend" numeric,
    "PreferenceShareDividend" numeric,
    "TaxOnDividend" numeric,
    "BonusEquityShareCapital" numeric,
    "NonCurrentInvestmentsQuotedMarketValue" numeric,
    "CurrentInvestmentsQuotedMarketValue" numeric,
    "CurrentInvestmentsUnquotedBookValue" numeric,
    "ModifiedDate" timestamp without time zone
);


CREATE TABLE public."FinanceNonBankingVI"
(
    "Companycode" double precision NOT NULL,
    "YearEnding" date NOT NULL,
    "Months" smallint NOT NULL,
    "Type" character(1) NOT NULL,
    "FaceValue" numeric,
    "RevenueFromOperationsGross" numeric,
    "LessExciseSeviceTaxOtherLevies" numeric,
    "RevenueFromOperationsNet" numeric,
    "OtherOperatingRevenues" numeric,
    "TotalOperatingRevenues" numeric,
    "OtherIncome" numeric,
    "TotalRevenue" numeric,
    "CostOfMaterialsConsumed" numeric,
    "PurchaseOfStockInTrade" numeric,
    "PurchaseOfCrudeOilAndOthers" numeric,
    "CostOfPowerPurchased" numeric,
    "CostOfFuel" numeric,
    "AircraftFuelExpenses" numeric,
    "AircraftLeaseRentals" numeric,
    "OperatingAndDirectExpenses" numeric,
    "ChangesIninventoriesOfFGWIPAndStockInTrade" numeric,
    "EmployeeBenefitExpenses" numeric,
    "FinanceCosts" numeric,
    "ProvisionsAndContingencies" numeric,
    "DepreciationAndAmortisationExpenses" numeric,
    "MiscExpensesWOff" numeric,
    "OtherExpenses" numeric,
    "LessInterUnitSegmentDivisionTransfer" numeric,
    "LessTransferToFromInvestmentFixedAssetsOthers" numeric,
    "LessAmountsTransferToCapitalAccounts" numeric,
    "LessShareOfLossFromPartnershipFirm" numeric,
    "TotalExpenses" numeric,
    "ProfitLossBeforeExceptionalExtraOrdinaryItemsAndTax" numeric,
    "ExceptionalItems" numeric,
    "ProfitLossBeforeTax" numeric,
    "CurrentTax" numeric,
    "LessMATCredit" numeric,
    "DeferredTax" numeric,
    "OtherDirectTaxes" numeric,
    "TaxForEarlierYears" numeric,
    "TotalTaxExpensesContinuedOperations" numeric,
    "ProfitLossAfterTaxAndBeforeExtraOrdinaryItems" numeric,
    "PriorPeriodItems" numeric,
    "ExtraordinaryItems" numeric,
    "ProfitLossFromContinuingOperations" numeric,
    "ProfitLossFromDiscontinuingOperations" numeric,
    "TotalTaxExpensesDiscontinuingOperations" numeric,
    "NetProfitLossFromDiscontinuingOperations" numeric,
    "ProfitLossForThePeriod" numeric,
    "EquityShares" numeric,
    "EquityCapital" numeric,
    "PreferenceCapital" numeric,
    "TotalShareCapital" numeric,
    "RevaluationReserves" numeric,
    "ReservesAndSurplus" numeric,
    "TotalReservesAndSurplus" numeric,
    "MoneyAgainstShareWarrants" numeric,
    "EmployeesStockOptions" numeric,
    "TotalShareHoldersFunds" numeric,
    "EquityShareApplicationMoney" numeric,
    "PreferenceShareApplicationMoney" numeric,
    "ShareCapitalSuspense" numeric,
    "HybridDebtOtherSecurities" numeric,
    "StatutoryConsumerReserves" numeric,
    "SpecialApprnTowardsProjectCost" numeric,
    "ServiceLineContributionFromConsumers" numeric,
    "GovernmentOtherGrants" numeric,
    "LongTermBorrowings" numeric,
    "DeferredTaxLiabilities" numeric,
    "OtherLongTermLiabilities" numeric,
    "LongTermProvisions" numeric,
    "TotalNonCurrentLiabilities" numeric,
    "ForeignCurrencyMonetaryItemTranslationDifferenceLiabilityAccoun" numeric,
    "ShortTermBorrowings" numeric,
    "TradePayables" numeric,
    "OtherCurrentLiabilities" numeric,
    "ShortTermProvisions" numeric,
    "TotalCurrentLiabilities" numeric,
    "TotalCapitalAndLiabilities" numeric,
    "TangibleAssets" numeric,
    "IntangibleAssets" numeric,
    "CapitalWorkInProgress" numeric,
    "IntangibleAssetsUnderDevelopment" numeric,
    "OtherAssets" numeric,
    "ConstructionStores" numeric,
    "MiningDevelopmentExpenditure" numeric,
    "AssetsHeldForSale" numeric,
    "TotalFixedAssets" numeric,
    "NonCurrentInvestments" numeric,
    "DeferredTaxAssets" numeric,
    "LongTermLoansAndAdvances" numeric,
    "OtherNonCurrentAssets" numeric,
    "TotalNonCurrentAssets" numeric,
    "ForeignCurrencyMonetaryItemTranslationDiffAcct" numeric,
    "CurrentInvestments" numeric,
    "Inventories" numeric,
    "TradeReceivables" numeric,
    "CashAndCashEquivalents" numeric,
    "ShortTermLoansAndAdvances" numeric,
    "OtherCurrentAssets" numeric,
    "TotalCurrentAssets" numeric,
    "TotalAssets" numeric,
    "NetProfitLossBeforeExtraordinaryItemsAndTax" numeric,
    "NetCashFlowFromOperatingActivities" numeric,
    "NetCashUsedInInvestingActivities" numeric,
    "NetCashUsedFromFinancingActivities" numeric,
    "ForeignExchangeGainsLosses" numeric,
    "AdjustmentsOnAmalgamationMergerDemergerOthers" numeric,
    "NetIncDecInCashAndCashEquivalents" numeric,
    "CashAndCashEquivalentBeginOfYear" numeric,
    "CashAndCashEquivalentEndOfYear" numeric,
    "BasicEPS" numeric,
    "DilutedEPS" numeric,
    "ContingentLiabilities" numeric,
    "CIFValueOfRawMaterials" numeric,
    "CIFValueOfStoresSparesAndLooseTools" numeric,
    "CIFValueOfOtherGoods" numeric,
    "CIFValueOfCapitalGoods" numeric,
    "ExpenditureInForeignCurrency" numeric,
    "DividendRemittanceInForeignCurrency" numeric,
    "ForeignExchangeFOB" numeric,
    "ForeignExchangeOtherEarnings" numeric,
    "ImportedRawMaterials" numeric,
    "IndigenousRawMaterials" numeric,
    "ImportedStoresAndSpares" numeric,
    "IndigenousStoresAndSpares" numeric,
    "EquityShareDividend" numeric,
    "PreferenceShareDividend" numeric,
    "TaxOnDividend" numeric,
    "EquityDividendRate" numeric,
    "BonusEquityShareCapital" numeric,
    "NonCurrentInvestmentsQuotedMarketValue" numeric,
    "NonCurrentInvestmentsUnquotedBookValue" numeric,
    "CurrentInvestmentsQuotedMarketValue" numeric,
    "CurrentInvestmentsUnquotedBookValue" numeric,
    "ModifiedDate" timestamp without time zone
);


CREATE TABLE public."FundMaster"
(
    "CompanyCode" double precision NOT NULL,
    "CompanyName" character varying(100),
    "SponsorName" character varying(255),
    "TrusteeCompanyName" character varying(100),
    "MFSetUpDate" date,
    "AMCCode" double precision,
    "AMCName" character varying(100),
    "AMCIncDate" date,
    "CEOName" character varying(200),
    "CIOName" character varying(200),
    "FundManagerName" character varying(200),
    "ComplianceOfficerName" character varying(200),
    "InvServiceOfficerName" character varying(200),
    "AuditorsName" character varying(200),
    "CustodianName" character varying(200),
    "RegistrarName" character varying(200),
    "TypeOfMutualFund" character varying(50),
    "ModifiedDate" timestamp without time zone,
    "DeleteFlag" boolean
);


CREATE TABLE public."HalfYearlyResults"
(
    "CompanyCode" double precision NOT NULL,
    "YearEnding" date NOT NULL,
    "Months" smallint NOT NULL,
    "Half" smallint NOT NULL,
    "OperatingIncome" numeric,
    "OtherOperatingIncome" numeric,
    "TotalIncomeFromOperations" numeric,
    "IntOrDiscOnAdvOrBills" numeric,
    "IncomeOnInvestements" numeric,
    "IntOnBalancesWithRBI" numeric,
    "Others" numeric,
    "OtherRecurringIncome" numeric,
    "StockAdjustment" numeric,
    "RawMaterialConsumed" numeric,
    "PurchaseOfTradedGoods" numeric,
    "PowerAndFuel" numeric,
    "EmployeeExpenses" numeric,
    "Excise" numeric,
    "AdminAndSellingExpenses" numeric,
    "ResearchAndDevelopmentExpenses" numeric,
    "ExpensesCapitalised" numeric,
    "OtherExpeses" numeric,
    "PL_Before_OtherInc_Int_Excpltem_Tax" numeric,
    "PL_Before_Int_ExcpItem_Tax" numeric,
    "InterestCharges" numeric,
    "PL_Before_ExcpItem_Tax" numeric,
    "ExceptionalItems" numeric,
    "Depreciation" numeric,
    "OperatingProfitBeforeProvisionsAndContingencies" numeric,
    "BankProvisionsMade" numeric,
    "PL_Before_Tax" numeric,
    "TaxCharges" numeric,
    "PL_After_TaxFromOrdineryActivities" numeric,
    "ExtraOrdinaryItem" numeric,
    "ReportedPAT" numeric,
    "PrioryearAdj" numeric,
    "ReservesWrittenBack" numeric,
    "EquityCapital" numeric,
    "ReservesAndSurplus" numeric,
    "EqDividendRate" numeric,
    "AggregateOfNonPromotoNoOfShares" numeric,
    "AggregateOfNonPromotoHoldingPercent" numeric,
    "GovernmentShare" numeric,
    "CapitalAdequacyRatio" numeric,
    "CapitalAdequacyBasell" numeric,
    "GrossNPA" numeric,
    "NetNPA" numeric,
    "Percentage_OfGrossNPA" numeric,
    "Percentage_OfNetNPA" numeric,
    "ReturnOnAssets_Per" numeric,
    "BeforeBasicEPS" numeric,
    "BeforeDilutedEPS" numeric,
    "AfterBasicEPS" numeric,
    "AfterDilutedEPS" numeric,
    "En_NumberOfShares" numeric,
    "En_Per_Share_As_Per_Of_Tot_Sh_Hol_of_Pro_And_Group" numeric,
    "En_Per_Share_As_Per_Of_Tot_Sh_Cap_of_Company" numeric,
    "NonEn_NumberOfShares" numeric,
    "NonEn_Per_Share_As_Per_Of_Tot_Sh_Hol_of_Pro_And_Group" numeric,
    "NonEn_Per_Share_As_Per_Of_Tot_Sh_Cap_of_Company" numeric,
    "ModifiedDate" timestamp without time zone
);



CREATE TABLE public."IndexOHLC"
(
    "IndexName" character varying,
    "NSECode" character varying,
    "Date" date,
    "Open" double precision,
    "High" double precision,
    "Low" double precision,
    "Close" double precision,
    "Points Change" double precision,
    "Change" double precision,
    "Volume" double precision,
    "Turnover" double precision,
    "PE" double precision,
    "PB" double precision,
    "Div Yield" double precision
);




CREATE TABLE public."IndividualHolding"
(
    "CompanyCode" double precision,
    "ShareHoldingDate" date,
    "InstrumentDescription" character varying,
    "Name" character varying,
    "NoOfShares" numeric,
    "PromoterFlag" boolean,
    "ModifiedDate" timestamp without time zone
);


CREATE TABLE public."IndustryIndexList"
(
    "GenDate" date,
    "IndustryIndexName" character varying,
    "Industry" character varying,
    "Open" double precision,
    "High" double precision,
    "Low" double precision,
    "Close" double precision,
    "Volume" double precision,
    "PE" double precision,
    "EPS" double precision,
    "CompanyCount" integer,
    "OS" double precision,
    "Earnings Growth" double precision
);


CREATE TABLE public."IndustryMaster"
(
    "IndustryCode" double precision,
    "IndustryName" character varying,
    "BroadIndustryCode" double precision,
    "BroadIndustryName" character varying,
    "MajorSectorCode" double precision,
    "MajorSectorName" character varying,
    "ModifiedDate" timestamp without time zone,
    "DeleteFlag" boolean
);


CREATE TABLE public."KeyExecutives"
(
    "Companycode" double precision NOT NULL,
    "TitleDescription" character varying(40),
    "ExecutiveName" character varying(100),
    "DesignationDescription" character varying(100),
    "ModifiedDate" timestamp without time zone,
    "RowId" smallint
);


CREATE TABLE public."Locations"
(
    "CompanyCode" double precision NOT NULL,
    "AddressType" character varying(50) NOT NULL,
    "Address" character varying(255),
    "Address2" character varying(255),
    "Address3" character varying(255),
    "Address4" character varying(255),
    "CityName" character varying(50),
    "StateName" character varying(50),
    "CountryName" character varying(50),
    "Pincode" character varying(20),
    "TelephoneNumbers" character varying(50),
    "Phone2" character varying(50),
    "Phone3" character varying(50),
    "Phone4" character varying(50),
    "Telex" character varying(40),
    "Telex1" character varying(40),
    "Fax" character varying(50),
    "Fax1" character varying(50),
    "Grams" character varying(50),
    "EMailID" character varying(100),
    "WebSite" character varying(100),
    "ModifiedDate" timestamp without time zone
);


CREATE TABLE public."MFInvestments"
(
    "MFDate" date NOT NULL,
    "InstrumentType" character varying(50) NOT NULL,
    "Purchase" numeric,
    "Sales" numeric,
    "NetInvestment" numeric,
    "ModifiedDate" timestamp without time zone
);


CREATE TABLE public."MFMergeList"
(
    "SchemeCode" double precision,
    "SchemeName" character varying,
    "SchemePlanCode" integer,
    "SchemeCategoryDescription" character varying,
    "MainCategory" character varying,
    "SchemeTypeDescription" character varying,
    "AUM" double precision,
    "HoldingDate" date,
    "InvestedCompanyCode" double precision,
    "InvestedCompanyName" character varying,
    "Industry" character varying,
    "Quantity" double precision,
    "Percentage" double precision,
    "ISINCode" character varying,
    "Market Cap Class" character varying,
    "GenDate" date,
    "Market Rank" double precision
);


CREATE TABLE public."ManagementTeam"
(
    "Companycode" double precision NOT NULL,
    "TitleDescription" character varying(40),
    "PersonName" character varying(100),
    "DesignationDescription" character varying(100),
    "ModifiedDate" timestamp without time zone,
    "RowId" smallint
);


CREATE TABLE public."NSE"
(
    "SYMBOL" character varying,
    "SERIES" character varying,
    "OPEN" double precision,
    "HIGH" double precision,
    "LOW" double precision,
    "CLOSE" double precision,
    "LAST" double precision,
    "PREVCLOSE" double precision,
    "TOTTRDQTY" double precision,
    "TOTTRDVAL" double precision,
    "TIMESTAMP" date,
    "TOTALTRADES" double precision,
    "ISIN" character varying
);


CREATE TABLE public."News"
(
    "CompanyCode" double precision NOT NULL,
    "NewsDate" date NOT NULL,
    "Headlines" character varying(250) NOT NULL,
    "SourceName" character varying(50),
    "NewsTypeDescription" character varying(50),
    "NewsClassfication" character varying(50),
    "Synopsis" text,
    "ModifiedDate" timestamp without time zone
);


CREATE TABLE public."NinemonthsResults"
(
    "CompanyCode" double precision NOT NULL,
    "YearEnding" date NOT NULL,
    "Months" smallint NOT NULL,
    "Nine" smallint NOT NULL,
    "OperatingIncome" numeric,
    "OtherOperatingIncome" numeric,
    "TotalIncomeFromOperations" numeric,
    "IntOrDiscOnAdvOrBills" numeric,
    "IncomeOnInvestements" numeric,
    "IntOnBalancesWithRBI" numeric,
    "Others" numeric,
    "OtherRecurringIncome" numeric,
    "StockAdjustment" numeric,
    "RawMaterialConsumed" numeric,
    "PurchaseOfTradedGoods" numeric,
    "PowerAndFuel" numeric,
    "EmployeeExpenses" numeric,
    "Excise" numeric,
    "AdminAndSellingExpenses" numeric,
    "ResearchAndDevelopmentExpenses" numeric,
    "ExpensesCapitalised" numeric,
    "OtherExpeses" numeric,
    "PL_Before_OtherInc_Int_Excpltem_Tax" numeric,
    "PL_Before_Int_ExcpItem_Tax" numeric,
    "InterestCharges" numeric,
    "PL_Before_ExcpItem_Tax" numeric,
    "ExceptionalItems" numeric,
    "Depreciation" numeric,
    "OperatingProfitBeforeProvisionsAndContingencies" numeric,
    "BankProvisionsMade" numeric,
    "PL_Before_Tax" numeric,
    "TaxCharges" numeric,
    "PL_After_TaxFromOrdineryActivities" numeric,
    "ExtraOrdinaryItem" numeric,
    "ReportedPAT" numeric,
    "PrioryearAdj" numeric,
    "ReservesWrittenBack" numeric,
    "EquityCapital" numeric,
    "ReservesAndSurplus" numeric,
    "EqDividendRate" numeric,
    "AggregateOfNonPromotoNoOfShares" numeric,
    "AggregateOfNonPromotoHoldingPercent" numeric,
    "GovernmentShare" numeric,
    "CapitalAdequacyRatio" numeric,
    "CapitalAdequacyBasell" numeric,
    "GrossNPA" numeric,
    "NetNPA" numeric,
    "Percentage_OfGrossNPA" numeric,
    "Percentage_OfNetNPA" numeric,
    "ReturnOnAssets_Per" numeric,
    "BeforeBasicEPS" numeric,
    "BeforeDilutedEPS" numeric,
    "AfterBasicEPS" numeric,
    "AfterDilutedEPS" numeric,
    "En_NumberOfShares" numeric,
    "En_Per_Share_As_Per_Of_Tot_Sh_Hol_of_Pro_And_Group" numeric,
    "En_Per_Share_As_Per_Of_Tot_Sh_Cap_of_Company" numeric,
    "NonEn_NumberOfShares" numeric,
    "NonEn_Per_Share_As_Per_Of_Tot_Sh_Hol_of_Pro_And_Group" numeric,
    "NonEn_Per_Share_As_Per_Of_Tot_Sh_Cap_of_Company" numeric,
    "ModifiedDate" timestamp without time zone
);


CREATE TABLE public."OHLC"
(
    "Company" character varying,
    "NSECode" character varying,
    "BSECode" integer,
    "ISIN" character varying,
    "Open" double precision,
    "High" double precision,
    "Low" double precision,
    "Close" double precision,
    "Date" date,
    "Value" double precision,
    "Volume" numeric,
    "CompanyCode" double precision
);



CREATE TABLE public."PE"
(
    "CompanyCode" double precision,
    "NSECode" character varying,
    "BSECode" integer,
    "ISINCode" character varying,
    "Market Cap Value" double precision,
    "Market Cap Rank" integer,
    "Market Cap Class" character varying,
    "PE" double precision,
    "PE High" double precision,
    "PE High Date" date,
    "PE Low" double precision,
    "PE Low Date" date,
    "GenDate" date
);



CREATE TABLE public."PledgeShares"
(
    "CompanyCode" double precision,
    "SHPDate" date,
    "PledgeShares_Nos" bigint,
    "PledgeShares_Pct" double precision,
    "ModifiedDate" date,
    "DeleteFlag" boolean
);


CREATE TABLE public."Products"
(
    "CompanyCode" double precision,
    "YearEnding" date,
    "NoOfMonths" integer,
    "ProductCode" double precision,
    "ProductName" character varying,
    "LicensedCapacity" numeric,
    "LICUOM" character varying,
    "InstalledCapacity" numeric,
    "InstalledUOM" character varying,
    "ProductionQuantity" numeric,
    "ProductionUOM" character varying,
    "SalesQuantity" numeric,
    "SalesUOM" character varying,
    "SalesValue" numeric,
    "OpeningQuantity" numeric,
    "OpeningUOM" character varying,
    "OpeningValue" numeric,
    "ClosingQuantity" numeric,
    "ClosingUOM" character varying,
    "ClosingValue" numeric,
    "BoughtOutQuantity" numeric,
    "BroughtOutUOM" character varying,
    "ProductMix" numeric,
    "ModifiedDate" timestamp without time zone
);




CREATE TABLE public."QuarterlyResults"
(
    "CompanyCode" double precision NOT NULL,
    "YearEnding" date NOT NULL,
    "Months" smallint NOT NULL,
    "Quarter" smallint NOT NULL,
    "OperatingIncome" numeric,
    "OtherOperatingIncome" numeric,
    "TotalIncomeFromOperations" numeric,
    "IntOrDiscOnAdvOrBills" numeric,
    "IncomeOnInvestments" numeric,
    "IntOnBalancesWithRBI" numeric,
    "Others" numeric,
    "OtherRecurringIncome" numeric,
    "StockAdjustment" numeric,
    "RawMaterialConsumed" numeric,
    "PurchaseOfTradedGoods" numeric,
    "PowerAndFuel" numeric,
    "EmployeeExpenses" numeric,
    "Excise" numeric,
    "AdminAndSellingExpenses" numeric,
    "ResearchAndDevelopmentExpenses" numeric,
    "ExpensesCapitalised" numeric,
    "OtherExpeses" numeric,
    "PL_Before_OtherInc_Int_Excpltem_Tax" numeric,
    "PL_Before_Int_ExcpItem_Tax" numeric,
    "InterestCharges" numeric,
    "PL_Before_ExcpItem_Tax" numeric,
    "ExceptionalItems" numeric,
    "Depreciation" numeric,
    "OperatingProfitBeforeProvisionsAndContingencies" numeric,
    "BankProvisionsMade" numeric,
    "PL_Before_Tax" numeric,
    "TaxCharges" numeric,
    "PL_After_TaxFromOrdineryActivities" numeric,
    "ExtraOrdinaryItem" numeric,
    "ReportedPAT" numeric,
    "PrioryearAdj" numeric,
    "ReservesWrittenBack" numeric,
    "EquityCapital" numeric,
    "ReservesAndSurplus" numeric,
    "EqDividendRate" numeric,
    "AggregateOfNonPromotoNoOfShares" numeric,
    "AggregateOfNonPromotoHoldingPercent" numeric,
    "GovernmentShare" numeric,
    "CapitalAdequacyRatio" numeric,
    "CapitalAdequacyBasell" numeric,
    "GrossNPA" numeric,
    "NetNPA" numeric,
    "Percentage_OfGrossNPA" numeric,
    "Percentage_OfNetNPA" numeric,
    "ReturnOnAssets_Per" numeric,
    "BeforeBasicEPS" numeric,
    "BeforeDilutedEPS" numeric,
    "AfterBasicEPS" numeric,
    "AfterDilutedEPS" numeric,
    "En_NumberOfShares" numeric,
    "En_Per_Share_As_Per_Of_Tot_Sh_Hol_of_Pro_And_Group" numeric,
    "En_Per_Share_As_Per_Of_Tot_Sh_Cap_of_Company" numeric,
    "NonEn_NumberOfShares" numeric,
    "NonEn_Per_Share_As_Per_Of_Tot_Sh_Hol_of_Pro_And_Group" numeric,
    "NonEn_Per_Share_As_Per_Of_Tot_Sh_Cap_of_Company" numeric,
    "ModifiedDate" timestamp without time zone
);


CREATE TABLE public."RatiosBankingVI"
(
    "CompanyCode" double precision NOT NULL,
    "YearEnding" date NOT NULL,
    "Months" smallint NOT NULL,
    "Type" character(1) NOT NULL,
    "FaceValue" numeric,
    "BasicEPS" numeric,
    "DilutedEPS" numeric,
    "CashEPS" numeric,
    "BVPerShareExclRevalReserve" numeric,
    "BVPerShareInclRevalReserve" numeric,
    "DividendPerShare" numeric,
    "OperatingRevenuePerShare" numeric,
    "NPPerShare" numeric,
    "InterestIncomePerEmployee" numeric,
    "NPPerEmployee" numeric,
    "BusinessPerEmployee" numeric,
    "InterestIncomePerBranch" numeric,
    "NPPerBranches" numeric,
    "BusinessPerBranches" numeric,
    "NPM" numeric,
    "OPM" numeric,
    "ROA" numeric,
    "ROE" numeric,
    "NIM" numeric,
    "CostToIncome" numeric,
    "InterestIncomeByEarningAssets" numeric,
    "NonInterestIncomeByEarningAssets" numeric,
    "OperatingProfitByEarningAssets" numeric,
    "OperatingExpensesByEarningAssets" numeric,
    "InterestExpensesByEarningAssets" numeric,
    "EnterpriseValue" numeric,
    "EVPerNetSales" numeric,
    "PriceToBV" numeric,
    "PriceToSales" numeric,
    "RetentionRatios" numeric,
    "EarningsYield" numeric,
    "ModifiedDate" timestamp without time zone
);


CREATE TABLE public."RatiosConslidatedNonBankingVI"
(
    "CompanyCode" double precision NOT NULL,
    "YearEnding" date NOT NULL,
    "Months" smallint NOT NULL,
    "Type" character(1) NOT NULL,
    "FaceValue" numeric,
    "BasicEPS" numeric,
    "DilutedEPS" numeric,
    "CashEPS" numeric,
    "BVPerShareExclRevalReserve" numeric,
    "BVPerShareInclRevalReserve" numeric,
    "OperatingRevenuePerShare" numeric,
    "PBDITPerShare" numeric,
    "PBITPerShare" numeric,
    "PBTPerShare" numeric,
    "NPPerShare" numeric,
    "NPAfterMIPerShare" numeric,
    "PBDITMargin" numeric,
    "PBITMargin" numeric,
    "PBTMargin" numeric,
    "NPMargin" numeric,
    "NPAfterMIMargin" numeric,
    "RONW" numeric,
    "ROCE" numeric,
    "ReturnOnAssets" numeric,
    "LongTermDebtEquity" numeric,
    "DebtEquity" numeric,
    "AssetTurnover" numeric,
    "CurrentRatio" numeric,
    "QuickRatio" numeric,
    "InventoryTurnoverRatio" numeric,
    "DividendPayoutNP" numeric,
    "DividendPayoutCP" numeric,
    "EarningRetention" numeric,
    "CashEarningRetention" numeric,
    "InterestCoverage" numeric,
    "InterestCoveragePostTax" numeric,
    "EnterpriseValue" numeric,
    "EVPerNetSales" numeric,
    "EVPerEBITDA" numeric,
    "MarketCapPerSales" numeric,
    "RetentionRatios" numeric,
    "PriceToBV" numeric,
    "PriceToSales" numeric,
    "EarningsYield" numeric,
    "ModifiedDate" timestamp without time zone
);


CREATE TABLE public."RatiosConsolidatedBankingVI"
(
    "CompanyCode" double precision NOT NULL,
    "YearEnding" date NOT NULL,
    "Months" smallint NOT NULL,
    "Type" character(1) NOT NULL,
    "FaceValue" numeric,
    "BasicEPS" numeric,
    "DilutedEPS" numeric,
    "CashEPS" numeric,
    "BVPerShareExclRevalReserve" numeric,
    "BVPerShareInclRevalReserve" numeric,
    "DividendPerShare" numeric,
    "OperatingRevenuePerShare" numeric,
    "NPPerShare" numeric,
    "NPAfterMIPerShare" numeric,
    "InterestIncomePerEmployee" numeric,
    "NPPerEmployee" numeric,
    "NPAfterMIPerEmployee" numeric,
    "BusinessPerEmployee" numeric,
    "InterestIncomePerBranch" numeric,
    "NPPerBranches" numeric,
    "NPAfterMIPerBranches" numeric,
    "BusinessPerBranches" numeric,
    "NPM" numeric,
    "NPMAfterMI" numeric,
    "OPM" numeric,
    "ROA" numeric,
    "ROE" numeric,
    "NIM" numeric,
    "CostToIncome" numeric,
    "InterestIncomeByEarningAssets" numeric,
    "NonInterestIncomeByEarningAssets" numeric,
    "OperatingProfitByEarningAssets" numeric,
    "OperatingExpensesByEarningAssets" numeric,
    "InterestExpensesByEarningAssets" numeric,
    "EnterpriseValue" numeric,
    "EVPerNetSales" numeric,
    "PriceToBV" numeric,
    "PriceToSales" numeric,
    "RetentionRatios" numeric,
    "EarningsYield" numeric,
    "ModifiedDate" timestamp without time zone
);


CREATE TABLE public."RatiosMergeList"
(
    "CompanyCode" double precision,
    "CompanyName" character varying,
    "Industry" character varying,
    "ROEYearEnding" date,
    "Months" integer,
    "FaceValue" double precision,
    "SalesGrowth" double precision,
    "NPM" double precision,
    "ROE" double precision,
    "Debt" double precision,
    "TTMYearEnding" date,
    "GenDate" date
);




CREATE TABLE public."RatiosNonBankingVI"
(
    "CompanyCode" double precision NOT NULL,
    "YearEnding" date NOT NULL,
    "Months" smallint NOT NULL,
    "Type" character(1) NOT NULL,
    "FaceValue" numeric,
    "BasicEPS" numeric,
    "DilutedEPS" numeric,
    "CashEPS" numeric,
    "BVPerShareExclRevalReserve" numeric,
    "BVPerShareInclRevalReserve" numeric,
    "DividendPerShare" numeric,
    "OperatingRevenuePerShare" numeric,
    "PBDITPerShare" numeric,
    "PBITPerShare" numeric,
    "PBTPerShare" numeric,
    "NPperShare" numeric,
    "PBDITMargin" numeric,
    "PBITMargin" numeric,
    "PBTMargin" numeric,
    "NPMargin" numeric,
    "RONW" numeric,
    "ROCE" numeric,
    "ReturnOnAssets" numeric,
    "LongTermDebtEquity" numeric,
    "DebtEquity" numeric,
    "AssetTurnover" numeric,
    "CurrentRatio" numeric,
    "QuickRatio" numeric,
    "InventoryTurnoverRatio" numeric,
    "DividendPayoutNP" numeric,
    "DividendPayoutCP" numeric,
    "EarningRetention" numeric,
    "CashEarningRetention" numeric,
    "InterestCoverage" numeric,
    "InterestCoveragePostTax" numeric,
    "EnterpriseValue" numeric,
    "EVPerNetSales" numeric,
    "EVPerEBITDA" numeric,
    "MarketCapPerSales" numeric,
    "RetentionRatios" numeric,
    "PriceToBV" numeric,
    "PriceToSales" numeric,
    "EarningsYield" numeric,
    "ModifiedDate" timestamp without time zone
);


CREATE TABLE public."RawMaterials"
(
    "CompanyCode" double precision NOT NULL,
    "YearEnding" date NOT NULL,
    "ProductCode" numeric NOT NULL,
    "ProductName" character varying(75) NOT NULL,
    "RawMatUOM" character varying(50),
    "RawMatQuantity" numeric,
    "RawMatValue" numeric,
    "ProdMix" numeric,
    "ModifiedDate" timestamp without time zone
);


CREATE TABLE public."Registrars"
(
    "CompanyCode" double precision NOT NULL,
    "AgencyName" character varying(50),
    "Address" character varying(75),
    "Address2" character varying(75),
    "Address3" character varying(75),
    "Address4" character varying(75),
    "CityName" character varying(30),
    "StateName" character varying(30),
    "PinCode" character varying(10),
    "TelephoneNumbers" character varying(20),
    "Phone2" character varying(20),
    "Phone3" character varying(20),
    "Phone4" character varying(20),
    "Fax" character varying(20),
    "Telex" character varying(30),
    "Grams" character varying(30),
    "EmailId" character varying(50),
    "Website" character varying(50),
    "ModifiedDate" timestamp without time zone
);


CREATE TABLE public."QuarterlyEPS"
(
    "CompanyCode" double precision NOT NULL,
    "YearEnding" date NOT NULL,
    "Months" smallint,
    "Quarter" smallint,
    "Sales" double precision,
    "Expenses" double precision,
    "EBIDTA" double precision,
    "Interest" double precision,
    "Depreciation" double precision,
    "Extraordinary" double precision,
    "OPM" double precision,
    "Tax" double precision,
    "PATRAW" double precision,
    "PAT" double precision,
    "Equity" double precision,
    "Reserves" double precision,
    "EPS" double precision,
    "NPM" double precision,
    "Ext_Flag" boolean,
    "Q1 EPS Growth" double precision,
    "Q1 Sales Growth" double precision,
    "Q2 EPS" double precision,
    "Q2 EPS Growth" double precision,
    "Q2 Sales" double precision,
    "Q2 Sales Growth" double precision,
    "E_ERS" double precision
);


CREATE TABLE public."Rights"
(
    "CompanyCode" double precision NOT NULL,
    "DateOfAnnouncement" date NOT NULL,
    "ExistingInstrumentType" smallint,
    "ExistingInstrumentName" character varying(100),
    "RatioExisting" numeric,
    "RatioOffering" numeric,
    "OfferredInstrumentType" smallint,
    "OfferredInstrumentName" character varying(100),
    "FaceValueExistingInstrument" numeric,
    "FaceValueOfferedInstrument" numeric,
    "RightsPremium" numeric,
    "RecordDate" date,
    "BookClosureStartDate" date,
    "BookClosureEndDate" date,
    "XRDate" date,
    "Remarks" character varying(255),
    "DeleteFlag" boolean,
    "ModifiedDate" timestamp without time zone
);


CREATE TABLE public."SchemeBoardOfAMC"
(
    "CompanyCode" double precision NOT NULL,
    "YearEnding" date,
    "TitleDescription" character varying(50),
    "PersonName" character varying(50),
    "DesignationDescription" character varying(50),
    "ModifyDate" timestamp without time zone,
    "DeleteFlag" boolean,
    "RowId" integer
);


CREATE TABLE public."SchemeBoardOfTrustees"
(
    "CompanyCode" double precision NOT NULL,
    "YearEnding" date,
    "TitleDescription" character varying(50),
    "PersonName" character varying(50),
    "DesignationDescription" character varying(50),
    "ModifiedDate" timestamp without time zone,
    "DeleteFlag" boolean,
    "RowId" integer
);


CREATE TABLE public."SchemeBonusDetails"
(
    "SchemeCode" double precision NOT NULL,
    "SchemePlanCode" double precision NOT NULL,
    "BonusDate" date NOT NULL,
    "Ratio_Offered" character varying(100),
    "Ratio_Existing" character varying(100),
    "Remarks" character varying(255),
    "ModifiedDate" timestamp without time zone,
    "DeleteFlag" boolean,
    "RowId" integer
);


CREATE TABLE public."SchemeCategoryDetails"
(
    "SchemeClassCode" double precision,
    "SchemeClassDescription" character varying,
    "CategoryNavDate" date,
    "NoOfSchemes" integer,
    "OneDayReturn" numeric,
    "OneWeekReturn" numeric,
    "TwoWeeksReturn" numeric,
    "OneMonthReturn" numeric,
    "ThreeMonthsReturn" numeric,
    "SixMonthsReturn" numeric,
    "NineMonthsReturn" numeric,
    "OneYearReturn" numeric,
    "TwoYearReturn" numeric,
    "ThreeYearReturn" numeric,
    "FiveYearReturn" numeric,
    "InceptionReturn" numeric,
    "ModifiedDate" timestamp without time zone
);


CREATE TABLE public."SchemeCorporateDividendDetails"
(
    "SchemeCode" double precision,
    "SchemePlanCode" integer,
    "DividendDate" date,
    "Percentage" double precision,
    "DividendPerUnit" double precision,
    "DividendTypeDescription" character varying(50),
    "Remarks" character varying(255),
    "ModifiedDate" timestamp without time zone,
    "DeleteFlag" boolean,
    "RowId" integer
);


CREATE TABLE public."SchemeDividendDetails"
(
    "SchemeCode" double precision,
    "SchemePlanCode" double precision,
    "DividendTypeDescription" character varying,
    "DividendDate" date,
    "Percentage" double precision,
    "DividendPerUnit" double precision,
    "Remarks" character varying(255),
    "ModifiedDate" timestamp without time zone,
    "DeleteFlag" boolean,
    "RowId" integer
);


CREATE TABLE public."SchemeFundManagerProfile"
(
    "SchemeCode" double precision NOT NULL,
    "SchemePlanCode" double precision NOT NULL,
    "FundManagerName" character varying(100),
    "FMProfile" text,
    "ModifiedDate" timestamp without time zone,
    "DeleteFlag" boolean
);


CREATE TABLE public."SchemeLocations"
(
    "EntityCode" double precision NOT NULL,
    "LocationType" character varying(50) NOT NULL,
    "Address" character varying(255),
    "CityName" character varying(255),
    "StateName" character varying(255),
    "CountryName" character varying(25),
    "Pincode" character varying(10),
    "TelephoneNumbers" character varying(255),
    "Telex" character varying(255),
    "Fax" character varying(255),
    "Grams" character varying(25),
    "EMailID" character varying(255),
    "WebSite" character varying(255),
    "ModifiedDate" timestamp without time zone
);


CREATE TABLE public."SchemeMaster"
(
    "SchemeCode" double precision NOT NULL,
    "SchemeName" character varying,
    "CompanyCode" double precision,
    "AMCCode" double precision,
    "SchemeType" integer,
    "SchemeTypeDescription" character varying,
    "SchemePlanCode" double precision NOT NULL,
    "SchemePlanDescription" character varying,
    "SchemeClassCode" integer,
    "SchemeClassDescription" character varying,
    "SchemeCategoryCode" integer,
    "SchemeCategoryDescription" character varying,
    "MainCategory" character varying,
    "SubCategory" character varying,
    "LaunchDate" date,
    "OpenDate" date,
    "CloseDate" date,
    "RedemptionDate" date,
    "MinInvAmnt" numeric,
    "IntPrice" numeric,
    "IntPricUOM" integer,
    "IntPUOMDes" character varying,
    "Size" double precision,
    "SizeUOM" double precision,
    "SizeUOMDescription" character varying(255),
    "OfferPrice" double precision,
    "AmountRaised" double precision,
    "AmountRaisedUOM" double precision,
    "AmountRaisedUOMDescription" character varying,
    "FundManagerPrefix" character varying,
    "FundManagerName" character varying,
    "ListingInformation" boolean,
    "EntryLoad" character varying,
    "ExitLoad" character varying,
    "RedemptionFerq" character varying,
    "ISIN_Div_Payout_ISIN_Growth" character varying,
    "ISIN_Div_Reinvestment" character varying,
    "RegistrarName" character varying,
    "EntryLoad_WithSlab_Desc" character varying,
    "ExitLoad_WithSlab_Desc" character varying,
    "SIP_Min_Invt" character varying,
    "SIPEntryLoad" character varying,
    "SIPExitLoad" character varying,
    "ExpensesRatio" character varying,
    "PortFolioTurnOverRatio" character varying,
    "SWP" character varying,
    "STP" character varying,
    "RiskoMeter" character varying,
    "ModifiedDate" timestamp without time zone,
    "DeleteFlag" boolean
);



CREATE TABLE public."SchemeNAVDetails"
(
    "SecurityCode" character varying(21) NOT NULL,
    "NAVDate" date NOT NULL,
    "NAVAmount" numeric,
    "RepurchaseLoad" numeric,
    "RepurchasePrice" numeric,
    "SaleLoad" numeric,
    "SalePrice" numeric,
    "ModifiedDate" timestamp without time zone,
    "DeleteFlag" boolean
);


CREATE TABLE public."SchemeNAVMaster"
(
    "SchemeCode" double precision,
    "SecurityCode" double precision,
    "SchemePlanCode" integer,
    "SchemePlanDescription" character varying,
    "MappingCode" character varying,
    "MappingName" character varying,
    "IssuePrice" numeric,
    "Description" character varying,
    "IssueDate" date,
    "ExpiryDate" date,
    "FaceValue" numeric,
    "MarketLot" numeric,
    "ISINCode" character varying,
    "BenchMarkIndex" double precision,
    "BenchMarkIndexName" character varying,
    "ModifiedDate" timestamp without time zone,
    "DeleteFlag" boolean
);


CREATE TABLE public."SchemeObjectives"
(
    "SchemeCode" double precision NOT NULL,
    "SchemePlanCode" integer NOT NULL,
    "Objectives" text,
    "ModifiedDate" timestamp without time zone,
    "DeleteFlag" boolean
);


CREATE TABLE public."SchemePortfolioHeader"
(
    "SchemeCode" double precision NOT NULL,
    "SchemePlanCode" integer NOT NULL,
    "HoldingDate" date NOT NULL,
    "TotalMarketValue" numeric,
    "ModifiedDate" timestamp without time zone
);


CREATE TABLE public."SchemeRegistrars"
(
    "CompanyCode" double precision NOT NULL,
    "AgencyName" character varying(50) NOT NULL,
    "Address" character varying(255),
    "Address2" character varying(255),
    "Address3" character varying(255),
    "Address4" character varying(255),
    "CityName" character varying(25),
    "StateName" character varying(25),
    "PinCode" character varying(10),
    "TelephoneNumbers" character varying(20),
    "Phone2" character varying(20),
    "Phone3" character varying(20),
    "Phone4" character varying(20),
    "Fax" character varying(20),
    "Telex" character varying(20),
    "Grams" character varying(25),
    "EmailId" character varying(50),
    "Website" character varying(50),
    "ModifiedDate" timestamp without time zone
);


CREATE TABLE public."SchemeSplitsDetails"
(
    "SchemeCode" double precision NOT NULL,
    "SchemePlanCode" integer NOT NULL,
    "SplitsDate" date NOT NULL,
    "Old_FaceValue" numeric,
    "New_FaceValue" numeric,
    "Remarks" character varying(100),
    "ModifiedDate" timestamp without time zone,
    "DeleteFlag" boolean
);


CREATE TABLE public."SchemeTaxBenefit"
(
    "SchemeCode" double precision NOT NULL,
    "SchemePlanCode" double precision NOT NULL,
    "SchemePlanDescription" character varying(50) NOT NULL,
    "ITActSection" character varying(100),
    "TaxBenefits" text,
    "ModifyDate" timestamp without time zone,
    "DeleteFlag" boolean,
    "RowId" integer
);



CREATE TABLE public."SectorIndexList"
(
    "GenDate" date,
    "SectorIndexName" character varying,
    "Sector" character varying,
    "Open" double precision,
    "High" double precision,
    "Low" double precision,
    "Close" double precision,
    "Volume" double precision,
    "PE" double precision,
    "EPS" double precision,
    "CompanyCount" integer,
    "OS" double precision,
    "Earnings Growth" double precision
);




CREATE TABLE public."ShareHolding"
(
    "CompanyCode" double precision NOT NULL,
    "SHPDate" date NOT NULL,
    "Capital" numeric,
    "FaceValue" numeric,
    "NoOfShares" numeric,
    "Promoters" numeric,
    "Directors" numeric,
    "SubsidiaryCompanies" numeric,
    "OtherCompanies" numeric,
    "ICICI" numeric,
    "UTI" numeric,
    "IDBI" numeric,
    "GenInsuranceComp" numeric,
    "LifeInsuranceComp" numeric,
    "StateFinCorps" numeric,
    "InduFinCorpIndia" numeric,
    "ForeignNRI" numeric,
    "ForeignCollaborators" numeric,
    "ForeignOCB" numeric,
    "ForeignOthers" numeric,
    "ForeignInstitutions" numeric,
    "ForeignIndustries" numeric,
    "StateGovt" numeric,
    "CentralGovt" numeric,
    "GovtCompanies" numeric,
    "GovtOthers" numeric,
    "Others" numeric,
    "NBanksMutualFunds" numeric,
    "HoldingCompanies" numeric,
    "GeneralPublic" numeric,
    "Employees" numeric,
    "FinancialInstitutions" numeric,
    "ForeignPromoter" numeric,
    "GDR" numeric,
    "PersonActingInConcert" numeric,
    "Total" numeric,
    "ModifiedDate" timestamp without time zone
);


CREATE TABLE public."Splits"
(
    "CompanyCode" double precision,
    "DateOfAnnouncement" date,
    "OldFaceValue" numeric,
    "NewFaceValue" numeric,
    "RecordDate" date,
    "BookClosureStartDate" date,
    "BookClosureEndDate" date,
    "XSDate" date,
    "Remarks" character varying(255),
    "DeleteFlag" boolean,
    "ModifiedDate" timestamp without time zone
);




CREATE TABLE public."SubSectorIndexList"
(
    "GenDate" date,
    "SubSectorIndexName" character varying,
    "SubSector" character varying,
    "Open" double precision,
    "High" double precision,
    "Low" double precision,
    "Close" double precision,
    "Volume" double precision,
    "PE" double precision,
    "EPS" double precision,
    "CompanyCount" integer,
    "OS" double precision,
    "Earnings Growth" double precision
);



CREATE TABLE public."SchemewisePortfolio"
(
    "SchemeCode" double precision NOT NULL,
    "SchemePlanCode" integer,
    "HoldingDate" date NOT NULL,
    "InvestedCompanyCode" double precision,
    "InvestedCompanyName" character varying,
    "IndustryCode" integer,
    "IndustryName" character varying,
    "InstrumentName" character varying,
    "ListingInformation" boolean,
    "Quantity" money,
    "Percentage" money,
    "IsItNPA" boolean,
    "MarketValue" money,
    "PortfolioUOMDescription" character varying,
    "Rating" character varying,
    "ISINCode" character varying,
    "DeleteFlag" boolean,
    "ModifiedDate" timestamp without time zone,
    "RowId" integer
);


CREATE TABLE public."Subsidiaries"
(
    "ParentCompanyCode" double precision,
    "ParentCompanyName" character varying,
    "CompanyCode" double precision,
    "CompanyName" character varying,
    "HoldingPercentage" numeric,
    "Remarks" character varying,
    "ModifiedDate" timestamp without time zone,
    "DeleteFlag" boolean
);



CREATE TABLE public.index_btt_mapping
(
    "IndexName" character varying,
    "BTTCode" character varying
);


CREATE TABLE public.mf_category_mapping
(
    scheme_code double precision NOT NULL,
    scheme_name character varying,
    scheme_category character varying,
    date date NOT NULL,
    btt_scheme_code character varying,
    btt_scheme_category character varying
);




CREATE TABLE public.mf_category_mapping_copy
(
    scheme_code double precision,
    scheme_name character varying,
    scheme_category character varying,
    date date,
    btt_scheme_code character varying,
    btt_scheme_category character varying
);


CREATE TABLE public.mf_ohlc
(
    date date,
    btt_scheme_code character varying,
    open double precision,
    high double precision,
    low double precision,
    close double precision,
    volume double precision
);


CREATE TABLE public.nse_index_change
(
    symbol character varying,
    change double precision,
    date date
);



CREATE TABLE public.ohlc_monthly
(
    company_code double precision,
    company_name character varying,
    nse_code character varying,
    bse_code bigint,
    open double precision,
    high double precision,
    low double precision,
    close double precision,
    volume double precision,
    date date
);


CREATE TABLE public.ohlc_weekly
(
    company_code double precision,
    open double precision,
    high double precision,
    low double precision,
    close double precision,
    volume bigint,
    date date
);



CREATE TABLE public."TTM"
(
    "CompanyCode" double precision NOT NULL,
    "YearEnding" date NOT NULL,
    "Months" smallint,
    "Quarter" smallint,
    "Sales" double precision,
    "Expenses" double precision,
    "EBIDTA" double precision,
    "Interest" double precision,
    "Depreciation" double precision,
    "Extraordinary" double precision,
    "OPM" double precision,
    "Tax" double precision,
    "PAT" double precision,
    "Equity" double precision,
    "Reserves" double precision,
    "EPS" double precision,
    "NPM" double precision,
    "E_ERS" double precision
);


CREATE TABLE public.users
(
    username character varying,
    password character varying
);


CREATE TABLE public."SubIndustryIndexList"
(
    "GenDate" date,
    "SubIndustryIndexName" character varying,
    "SubIndustry" character varying,
    "Open" double precision,
    "High" double precision,
    "Low" double precision,
    "Close" double precision,
    "Volume" double precision,
    "PE" double precision,
    "EPS" double precision,
    "CompanyCount" integer,
    "OS" double precision,
    "Earnings Growth" double precision
);


CREATE TABLE public."IndustryMapping"
(
    "IndustryCode" integer,
    "IndustryName" character varying,
    "Industry" character varying,
    "Code" integer,
    "SubSector" character varying,
    "SubSectorCode" integer,
    "Sector" character varying,
    "SectorCode" integer,
    "SubIndustry" character varying,
    "SubIndustryCode" integer,
    "IndustryIndexName" character varying,
    "SubSectorIndexName" character varying,
    "SectorIndexName" character varying,
    "SubIndustryIndexName" character varying
);


CREATE TABLE public."IndustryList"
(
    "CompanyCode" double precision,
    "CompanyName" character varying,
    "NSECode" character varying,
    "BSECode" bigint,
    "ISIN" character varying,
    "Industry" character varying,
    "Sector" character varying,
    "SubSector" character varying,
    "SubIndustry" character varying,
    "Open" double precision,
    "High" double precision,
    "Low" double precision,
    "Close" double precision,
    "Volume" double precision,
    "PrevClose" double precision,
    "Change" double precision,
    "OS" double precision,
    "FaceValue" double precision,
    "FreeFloat" double precision,
    "GenDate" date,
    "FF_Open" double precision,
    "FF_High" double precision,
    "FF_Low" double precision,
    "FF_Close" double precision,
    "MCap_Open" double precision,
    "MCap_High" double precision,
    "MCap_Low" double precision,
    "MCap_Close" double precision,
    "IndustryIndexName" character varying,
    "SectorIndexName" character varying,
    "SubSectorIndexName" character varying,
    "SubIndustryIndexName" character varying,
    "PAT" double precision,
    "Equity" double precision,
    "OS_Close" double precision,
    prev_pat double precision,
    prev_equity double precision
);


CREATE TABLE public."IndexHistory"
(
    "TICKER" character varying,
    "DATE" date,
    "OPEN" double precision,
    "HIGH" double precision,
    "LOW" double precision,
    "CLOSE" double precision,
    "MCap_OPEN" double precision,
    "MCap_HIGH" double precision,
    "MCap_LOW" double precision,
    "MCap_CLOSE" double precision,
    "VOL" double precision
);


CREATE TABLE public."SubSectorDivisor"
(
    "IndexName" character varying,
    "IndexValue" double precision,
    "Divisor" double precision,
    "MCap_Divisor" double precision,
    "SumMCap_Open" double precision,
    "Date" date,
    "OS" double precision
);


CREATE TABLE public."SubIndustryDivisor"
(
    "IndexName" character varying,
    "IndexValue" double precision,
    "Divisor" double precision,
    "MCap_Divisor" double precision,
    "SumMCap_Open" double precision,
    "Date" date,
    "OS" double precision
);


CREATE TABLE public."IndustryDivisor"
(
    "IndexName" character varying,
    "IndexValue" double precision,
    "Divisor" double precision,
    "MCap_Divisor" double precision,
    "SumMCap_Open" double precision,
    "Date" date,
    "OS" double precision
);


CREATE TABLE public."SectorDivisor"
(
    "IndexName" character varying,
    "IndexValue" double precision,
    "Divisor" double precision,
    "MCap_Divisor" double precision,
    "SumMCap_Open" double precision,
    "Date" date,
    "OS" double precision
);


CREATE TABLE mf_analysis.ema50_daily
(
    date date,
    ema50_above_percentage double precision
);



CREATE TABLE mf_analysis.ema50_monthly
(
    date date,
    ema50_above_percentage double precision
);


CREATE TABLE mf_analysis.ema50_weekly
(
    date date,
    ema50_above_percentage double precision
);




CREATE TABLE mf_analysis.indicators
(
    company_code double precision,
    company_name character varying,
    nse_code character varying,
    bse_code integer,
    open double precision,
    high double precision,
    low double precision,
    close double precision,
    volume bigint,
    ema13 double precision,
    ema26 double precision,
    macd double precision,
    macd_signal double precision,
    macd_diff double precision,
    atr double precision,
    gen_date date,
    ema12 double precision,
    ema50 double precision
);



CREATE TABLE mf_analysis.indicators_monthly
(
    company_code double precision,
    company_name character varying,
    nse_code character varying,
    bse_code integer,
    open double precision,
    high double precision,
    low double precision,
    close double precision,
    volume double precision,
    ema13 double precision,
    ema26 double precision,
    macd double precision,
    macd_signal double precision,
    macd_diff double precision,
    atr double precision,
    gen_date date,
    ema12 double precision,
    ema50 double precision
);


CREATE TABLE mf_analysis.indicators_weekly
(
    company_code double precision,
    company_name character varying,
    nse_code character varying,
    bse_code integer,
    open double precision,
    high double precision,
    low double precision,
    close double precision,
    volume bigint,
    ema13 double precision,
    ema26 double precision,
    macd double precision,
    macd_signal double precision,
    macd_diff double precision,
    atr double precision,
    gen_date date,
    ema12 double precision,
    ema50 double precision
);



CREATE TABLE mf_analysis.market_quality_number
(
    index_name character varying,
    normal character varying,
    quiet character varying,
    very_volatile character varying,
    volatile character varying,
    atr_avg double precision,
    atr21 double precision,
    date date,
    mqn_condition character varying,
    mqn_val double precision,
    very_volatile_val double precision,
    volatile_val double precision,
    normal_val double precision
);



CREATE TABLE mf_analysis.trend_weightage_daily
(
    date date,
    weightage integer
);




CREATE TABLE mf_analysis.trend_weightage_monthly
(
    date date,
    weightage integer
);



CREATE TABLE mf_analysis.trend_weightage_weekly
(
    date date,
    weightage integer
);



CREATE TABLE mf_analysis.trends
(
    company_code double precision,
    company_name character varying,
    nse_code character varying,
    bse_code integer,
    rt_bullish_trending integer,
    rt_bearish_trending integer,
    rt_bullish_non_trending integer,
    rt_bearish_non_trending integer,
    long integer,
    short integer,
    long_sideways integer,
    short_sideways integer,
    buy integer,
    sell integer,
    gen_date date
);




CREATE TABLE mf_analysis.trends_monthly
(
    company_code double precision,
    company_name character varying,
    nse_code character varying,
    bse_code integer,
    rt_bullish_trending integer,
    rt_bearish_trending integer,
    rt_bullish_non_trending integer,
    rt_bearish_non_trending integer,
    long integer,
    short integer,
    long_sideways integer,
    short_sideways integer,
    buy integer,
    sell integer,
    gen_date date
);


CREATE TABLE mf_analysis.trends_weekly
(
    company_code double precision,
    company_name character varying,
    nse_code character varying,
    bse_code integer,
    rt_bullish_trending integer,
    rt_bearish_trending integer,
    rt_bullish_non_trending integer,
    rt_bearish_non_trending integer,
    long integer,
    short integer,
    long_sideways integer,
    short_sideways integer,
    buy integer,
    sell integer,
    gen_date date
);


CREATE TABLE dash_process.index_off_high
(
    date date,
    index_name character varying,
    less_than_5 double precision,
    "5_to_10" double precision,
    "10_to_15" double precision,
    "15_to_20" double precision,
    greater_than_20 double precision
);



CREATE TABLE dash_process.index_off_low
(
    date date,
    index_name character varying,
    less_than_5 double precision,
    "5_to_10" double precision,
    "10_to_15" double precision,
    "15_to_20" double precision,
    greater_than_20 double precision
);




CREATE TABLE dash_process.index_performance
(
    index_name character varying,
    "1day" double precision,
    "5day" double precision,
    "10day" double precision,
    "30day" double precision,
    "60day" double precision,
    "90day" double precision,
    "6month" double precision,
    "1year" double precision,
    "2year" double precision,
    "3year" double precision,
    "5year" double precision,
    date date
);




CREATE TABLE dash_process.stock_off_high
(
    date date,
    less_than_5 double precision,
    "5_to_10" double precision,
    "10_to_15" double precision,
    "15_to_20" double precision,
    greater_than_20 double precision
);



CREATE TABLE dash_process.stock_off_low
(
    date date,
    less_than_5 double precision,
    "5_to_10" double precision,
    "10_to_15" double precision,
    "15_to_20" double precision,
    greater_than_20 double precision
);



CREATE TABLE dash_process.stock_performance
(
    company_code double precision,
    "1day" double precision,
    "5day" double precision,
    "30day" double precision,
    "90day" double precision,
    "6month" double precision,
    "1year" double precision,
    "2year" double precision,
    "5year" double precision,
    date date,
    nse_code character varying,
    bse_code integer
);




CREATE TABLE "Reports"."CombinedRS"
(
    "CompanyCode" double precision,
    "CompanyName" character varying,
    "NSECode" character varying,
    "BSECode" bigint,
    "PRSRank" double precision,
    "ERSRank" double precision,
    "RRSRank" double precision,
    "FRSRank" double precision,
    "IRSRank" double precision,
    "CombiRS" double precision,
    "Rank" integer,
    "Value Average" double precision,
    "GenDate" date
);


CREATE TABLE "Reports"."FRS-MFRank"
(
    "CompanyCode" double precision,
    "CompanyName" character varying,
    "Date" date,
    "ISINCode" character varying,
    "Quantity" double precision,
    "OutstandingShares" double precision,
    "MFExposure" double precision,
    "MFRank" double precision
);


CREATE TABLE "Reports"."FRS-NAVCategoryAvg"
(
    btt_scheme_category character varying,
    "Date" date,
    "1 Day Average" double precision,
    "1 Week Average" double precision,
    "1 Month Average" double precision,
    "3 Month Average" double precision,
    "6 Month Average" double precision,
    "9 Month Average" double precision,
    "1 Year Average" double precision,
    "2 Year Average" double precision,
    "3 Year Average" double precision,
    "5 Year Average" double precision
);


CREATE TABLE "Reports"."FRS-NAVRank"
(
    "SchemeCode" double precision,
    "SchemeName" character varying,
    "Date" date,
    "Current" double precision,
    "1 Day" double precision,
    "1 Day Rank" double precision,
    "1 Week" double precision,
    "1 Week Rank" double precision,
    "1 Month" double precision,
    "1 Month Rank" double precision,
    "3 Month" double precision,
    "3 Month Rank" double precision,
    "6 Month" double precision,
    "6 Month Rank" double precision,
    "9 Month" double precision,
    "9 Month Rank" double precision,
    "1 Year" double precision,
    "1 Year Rank" double precision,
    "2 Year" double precision,
    "2 Year Rank" double precision,
    "3 Year" double precision,
    "3 Year Rank" double precision,
    "5 Year" double precision,
    "5 Year Rank" double precision,
    "AUM" double precision,
    "Scheme Rank" double precision,
    btt_scheme_category character varying,
    btt_scheme_code character varying
);


CREATE TABLE "Reports"."NewHighNewLow"
(
    "Date" date NOT NULL,
    "30DNH" bigint,
    "30DNL" bigint,
    "30DNHNL" bigint,
    "90DNH" bigint,
    "90DNL" bigint,
    "90DNHNL" bigint,
    "52WNH" bigint,
    "52WNL" bigint,
    "52WNHNL" bigint
);


CREATE TABLE "Reports"."PRS"
(
    "CompanyName" character varying,
    "NSECode" character varying,
    "BSECode" integer,
    "Open" double precision,
    "High" double precision,
    "Low" double precision,
    "Close" double precision,
    "Volume" bigint,
    "Value" double precision,
    "52 High" double precision,
    "52 Low" double precision,
    "52 High Date" date,
    "52 Low Date" date,
    "New High 52W" boolean,
    "New High 90D" boolean,
    "New High 30D" boolean,
    "New Low 52W" boolean,
    "New Low 90D" boolean,
    "New Low 30D" boolean,
    "RR 1D" double precision,
    "RR 5D" double precision,
    "RR 10D" double precision,
    "RR 30D" double precision,
    "RR 60D" double precision,
    "Change 52W" double precision,
    "Change 90D" double precision,
    "Change 30D" double precision,
    "RR 52W" double precision,
    "RR 90D" double precision,
    "RS 52W" double precision,
    "RS 90D" double precision,
    "RS 30D" double precision,
    "Combined Strength" double precision,
    "ISIN" character varying NOT NULL,
    "Date" date NOT NULL,
    "RR30_Replaced" boolean,
    "RR60_Replaced" boolean,
    "RR90_Replaced" boolean,
    "RR52W_Replaced" boolean,
    "Off-High" double precision,
    "Off-Low" double precision,
    "CompanyCode" double precision,
    "Value Average" double precision,
    "Market Cap Value" double precision,
    "Market Cap Class" character varying,
    "Market Cap Rank" integer,
    "PE" double precision,
    "PE High" double precision,
    "PE High Date" date,
    "PE Low" double precision,
    "PE Low Date" date
);


CREATE TABLE "Reports"."SMR"
(
    "CompanyCode" double precision,
    "NSESymbol" character varying,
    "BSESymbol" integer,
    "Industry" character varying,
    "ROEYearEnding" date,
    "Months" integer,
    "TTMYearEnding" date,
    "SalesGrowth" double precision,
    "Sales Growth Rank" double precision,
    "NPM" double precision,
    "NPM Rank" double precision,
    "ROE" double precision,
    "ROE Rank" double precision,
    "SMR Grade" character varying,
    "SMR" double precision,
    "SMR Rank" double precision,
    "SMRDate" date,
    "CompanyName" character varying,
    "ISIN" character varying
);


CREATE TABLE "Reports"."Consolidated_EPS"
(
    "CompanyCode" double precision,
    "NSECode" character varying,
    "BSECode" bigint,
    "CompanyName" character varying,
    "ISIN" character varying,
    "Months" smallint,
    "Quarter" smallint,
    "YearEnding" date,
    "Q1 EPS" double precision,
    "Q1 EPS Growth" double precision,
    "Q1 Sales" double precision,
    "Q1 Sales Growth" double precision,
    "Q2 EPS" double precision,
    "Q2 EPS Growth" double precision,
    "Q2 Sales" double precision,
    "Q2 Sales Growth" double precision,
    "TTM1 EPS Growth" double precision,
    "TTM1 Sales Growth" double precision,
    "TTM2 EPS Growth" double precision,
    "TTM2 Sales Growth" double precision,
    "TTM3 EPS Growth" double precision,
    "TTM3 Sales Growth" double precision,
    "NPM" double precision,
    "EPS Rating" double precision,
    "Ranking" double precision,
    "EPSDate" date,
    "E_ERS" double precision
);


CREATE TABLE "Reports"."EPS"
(
    "CompanyCode" double precision,
    "NSECode" character varying,
    "BSECode" bigint,
    "CompanyName" character varying,
    "ISIN" character varying,
    "Months" smallint,
    "Quarter" smallint,
    "YearEnding" date,
    "Q1 EPS" double precision,
    "Q1 EPS Growth" double precision,
    "Q1 Sales" double precision,
    "Q1 Sales Growth" double precision,
    "Q2 EPS" double precision,
    "Q2 EPS Growth" double precision,
    "Q2 Sales" double precision,
    "Q2 Sales Growth" double precision,
    "TTM1 EPS Growth" double precision,
    "TTM1 Sales Growth" double precision,
    "TTM2 EPS Growth" double precision,
    "TTM2 Sales Growth" double precision,
    "TTM3 EPS Growth" double precision,
    "TTM3 Sales Growth" double precision,
    "NPM" double precision,
    "EPS Rating" double precision,
    "Ranking" double precision,
    "EPSDate" date,
    "E_ERS" double precision
);


CREATE TABLE "Reports"."STANDALONE_EPS"
(
    "CompanyCode" double precision,
    "NSECode" character varying,
    "BSECode" bigint,
    "CompanyName" character varying,
    "ISIN" character varying,
    "Months" smallint,
    "Quarter" smallint,
    "YearEnding" date,
    "Q1 EPS" double precision,
    "Q1 EPS Growth" double precision,
    "Q1 Sales" double precision,
    "Q1 Sales Growth" double precision,
    "Q2 EPS" double precision,
    "Q2 EPS Growth" double precision,
    "Q2 Sales" double precision,
    "Q2 Sales Growth" double precision,
    "TTM1 EPS Growth" double precision,
    "TTM1 Sales Growth" double precision,
    "TTM2 EPS Growth" double precision,
    "TTM2 Sales Growth" double precision,
    "TTM3 EPS Growth" double precision,
    "TTM3 Sales Growth" double precision,
    "NPM" double precision,
    "EPS Rating" double precision,
    "Ranking" double precision,
    "EPSDate" date,
    "E_ERS" double precision
);


CREATE TABLE "Reports"."IRS"
(
    "GenDate" date,
    "IndexName" character varying,
    "Index" character varying,
    "Open" double precision,
    "High" double precision,
    "Low" double precision,
    "Close" double precision,
    "MCap_Open_Index" double precision,
    "MCap_High_Index" double precision,
    "MCap_Low_Index" double precision,
    "MCap_Close_Index" double precision,
    "Volume" double precision,
    "PE" double precision,
    "EPS" double precision,
    "CompanyCount" integer,
    "OS" double precision,
    "Rank" double precision,
    "Change" double precision,
    "Earnings Growth" double precision,
    ff_open_sum double precision,
    ff_high_sum double precision,
    ff_low_sum double precision,
    ff_close_sum double precision,
    "MCap_Open_sum" double precision,
    "MCap_High_sum" double precision,
    "MCap_Low_sum" double precision,
    "MCap_Close_sum" double precision,
    "PE High" double precision,
    "PE High Date" date,
    "PE Low" double precision,
    "PE Low Date" date
);

CREATE TABLE "logs"."report_generation"
(
    "date" date,
    "file_presence" boolean,
    "BTTList" boolean,
    "EPS" boolean,
    "EERS" boolean,
    "SMR" boolean,
    "OHLC" boolean,
    "IndexOHLC" boolean,
    "PE" boolean,
    "PRS" boolean,
    "IRS" boolean,
    "time" time without time zone
    "runtime" interval
)

CREATE TABLE "logs"."insert"
(
    "Date" date,
    "FB3" boolean,
    "FB1" boolean,
    "FB2" boolean,
    "time" time without time zone
    "runtime" interval
)

CREATE TABLE "logs"."split_bonus"
(
    "date" date, 
    "time" time without time zone,
    "CompanyCode" double precision,
    "CompanyName" character varying,
    "split_value" double precision,
    "bonus_value" double precision,
    "ShareHolding_Total" double precision,
    "split_date" date,
    "bonus_date" date
    "split_bonus_count" character varying,
    "runtime" interval
)

CREATE TABLE "logs"."OHLC"
(
    "date" date,
    "time" time without time zone,
    "BTT_count" double precision,
    "OHLC_count" double precision,
    "BTT_OHLC_count" double precision,
    "nse_file" character varying,
    "bse_file" character varying,
    "runtime" interval
)

