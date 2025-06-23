-- Step 1: Database Setup and Table Creation

CREATE DATABASE Colombia_FRED;
USE Colombia_FRED;


---- Table for Gross Domestic Product (Annual)
SELECT * FROM dbo.GDP
---- Table for Consumer Price Index - Total (Monthly)
SELECT * FROM dbo.CPI_Total
---- Table for CPI - Housing, Water, etc. (Quarterly)
SELECT * FROM dbo.CPI_Housing
---- Table for Residential Property Prices (Quarterly)
SELECT * FROM dbo.Property_Prices
---- Table for 10-Year Gov Bond Yields (Monthly)
SELECT * FROM dbo.Interest_Rates
---- Table for USD to COP Exchange Rate (Monthly)
SELECT * FROM dbo.Exchange_Rate
---- Table for Exports (Monthly)
SELECT * FROM dbo.exports

-- Step 2: Resampling and Standardizing Data with Common Table Expressions (CTEs)

/*
  This query uses Common Table Expressions (CTEs) to transform each dataset
  to a quarterly frequency before joining them all together in the final SELECT statement.
*/
WITH 
-- 1. Resample Monthly Interest Rates to Quarterly Averages
Quarterly_Interest_Rates AS (
    SELECT 
        DATEFROMPARTS(YEAR(observation_date), (DATEPART(quarter, observation_date) * 3) - 2, 1) AS Quarter_Start_Date,
        AVG(Bond_Yield_Percent) AS Avg_Quarterly_Bond_Yield
    FROM Interest_Rates
    GROUP BY YEAR(observation_date), DATEPART(quarter, observation_date)
),

-- 2. Resample Monthly Exchange Rates to Quarterly Averages
Quarterly_Exchange_Rate AS (
    SELECT
        DATEFROMPARTS(YEAR(observation_date), (DATEPART(quarter, observation_date) * 3) - 2, 1) AS Quarter_Start_Date,
        AVG(USD_COP_Rate) AS Avg_Quarterly_USD_COP_Rate
    FROM Exchange_Rate
    GROUP BY YEAR(observation_date), DATEPART(quarter, observation_date)
),

-- 3. Resample Monthly CPI to Quarterly Averages
Quarterly_CPI_Total AS (
    SELECT
        DATEFROMPARTS(YEAR(observation_date), (DATEPART(quarter, observation_date) * 3) - 2, 1) AS Quarter_Start_Date,
        AVG(CPI_Index) AS Avg_Quarterly_CPI
    FROM CPI_Total
    GROUP BY YEAR(observation_date), DATEPART(quarter, observation_date)
),

-- 4. Resample Monthly Exports to Quarterly SUMS
Quarterly_Exports AS (
    SELECT
        DATEFROMPARTS(YEAR(observation_date), (DATEPART(quarter, observation_date) * 3) - 2, 1) AS Quarter_Start_Date,
        SUM(Exports_USD) AS Total_Quarterly_Exports_USD
    FROM Exports
    GROUP BY YEAR(observation_date), DATEPART(quarter, observation_date)
),

-- 5. Standardize Annual GDP and Unpivot to create a quarterly timeline
GDP_Quarterly_Timeline AS (
    SELECT 
        GDP_USD, 
        QuarterDates.Quarter_Start_Date
    FROM GDP
    -- This CROSS APPLY creates four rows (one for each quarter) for each single row of annual GDP
    CROSS APPLY (
        VALUES 
            (DATEFROMPARTS(YEAR(observation_date), 1, 1)),
            (DATEFROMPARTS(YEAR(observation_date), 4, 1)),
            (DATEFROMPARTS(YEAR(observation_date), 7, 1)),
            (DATEFROMPARTS(YEAR(observation_date), 10, 1))
    ) AS QuarterDates (Quarter_Start_Date)
)

---- Step 3: Joining the Processed Data into a Master Table

-- 6. Final Join into a Master Analytical Table
SELECT
    gdp.Quarter_Start_Date,
    gdp.GDP_USD,
    p.Property_Price_Index,
    cpi_h.CPI_Housing_Index,
    cpi_t.Avg_Quarterly_CPI,
    ir.Avg_Quarterly_Bond_Yield,
    er.Avg_Quarterly_USD_COP_Rate,
    ex.Total_Quarterly_Exports_USD
FROM
    GDP_Quarterly_Timeline gdp
LEFT JOIN
    Quarterly_Interest_Rates ir ON gdp.Quarter_Start_Date = ir.Quarter_Start_Date
LEFT JOIN
    Quarterly_Exchange_Rate er ON gdp.Quarter_Start_Date = er.Quarter_Start_Date
LEFT JOIN
    Quarterly_CPI_Total cpi_t ON gdp.Quarter_Start_Date = cpi_t.Quarter_Start_Date
LEFT JOIN
    Quarterly_Exports ex ON gdp.Quarter_Start_Date = ex.Quarter_Start_Date
LEFT JOIN
    CPI_Housing cpi_h ON gdp.Quarter_Start_Date = cpi_h.observation_date -- Assumes this is already quarterly
LEFT JOIN
    Property_Prices p ON gdp.Quarter_Start_Date = p.observation_date -- Assumes this is also quarterly
WHERE
    gdp.Quarter_Start_Date >= '2009-01-01'
ORDER BY
    gdp.Quarter_Start_Date;

