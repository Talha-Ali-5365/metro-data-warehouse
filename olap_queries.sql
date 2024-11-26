USE metro_dw;

-- ============================================
-- Helper Functions and Views
-- ============================================

DROP FUNCTION IF EXISTS IsWeekend;
DROP FUNCTION IF EXISTS GetSeason;

DELIMITER //

CREATE FUNCTION IsWeekend(date_value INT) 
RETURNS VARCHAR(10)
DETERMINISTIC
BEGIN
    DECLARE day_of_week INT;
    SET day_of_week = (date_value % 100);
    RETURN CASE WHEN day_of_week IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END;
END //

CREATE FUNCTION GetSeason(month_num INT)
RETURNS VARCHAR(10)
DETERMINISTIC
BEGIN
    RETURN CASE
        WHEN month_num IN (3,4,5) THEN 'Spring'
        WHEN month_num IN (6,7,8) THEN 'Summer'
        WHEN month_num IN (9,10,11) THEN 'Fall'
        ELSE 'Winter'
    END;
END //

DELIMITER ;

-- ============================================
-- Q1: Top Revenue-Generating Products on Weekdays and Weekends with Monthly Drill-Down
-- ============================================
WITH RankedProducts AS (
    SELECT 
        dp.PRODUCT_NAME,
        IsWeekend(dd.DATE_KEY) as DAY_TYPE,
        dd.MONTH,
        dd.YEAR,
        SUM(fs.SALES_AMOUNT) as TOTAL_REVENUE,
        RANK() OVER (PARTITION BY IsWeekend(dd.DATE_KEY), dd.MONTH 
                     ORDER BY SUM(fs.SALES_AMOUNT) DESC) as REVENUE_RANK
    FROM FACT_SALES fs
    JOIN DIM_PRODUCT dp ON fs.PRODUCT_KEY = dp.PRODUCT_KEY
    JOIN DIM_DATE dd ON fs.DATE_KEY = dd.DATE_KEY
    WHERE dd.YEAR = 2019
    GROUP BY dp.PRODUCT_NAME, DAY_TYPE, dd.MONTH, dd.YEAR
)
SELECT *
FROM RankedProducts
WHERE REVENUE_RANK <= 5
ORDER BY MONTH, DAY_TYPE, REVENUE_RANK;

-- ============================================
-- Q2: Trend Analysis of Store Revenue Growth Rate Quarterly for 2017
-- ============================================
WITH QuarterlyRevenue AS (
    SELECT 
        ds.STORE_NAME,
        dd.QUARTER,
        SUM(fs.SALES_AMOUNT) as REVENUE
    FROM FACT_SALES fs
    JOIN DIM_STORE ds ON fs.STORE_KEY = ds.STORE_KEY
    JOIN DIM_DATE dd ON fs.DATE_KEY = dd.DATE_KEY
    WHERE dd.YEAR = 2017
    GROUP BY ds.STORE_NAME, dd.QUARTER
),
GrowthRate AS (
    SELECT 
        STORE_NAME,
        QUARTER,
        REVENUE,
        LAG(REVENUE) OVER (PARTITION BY STORE_NAME ORDER BY QUARTER) as PREV_QUARTER_REVENUE,
        ((REVENUE - LAG(REVENUE) OVER (PARTITION BY STORE_NAME ORDER BY QUARTER)) / 
         NULLIF(LAG(REVENUE) OVER (PARTITION BY STORE_NAME ORDER BY QUARTER), 0) * 100) as GROWTH_RATE
    FROM QuarterlyRevenue
)
SELECT 
    STORE_NAME,
    CONCAT('Q', QUARTER) as QUARTER,
    FORMAT(REVENUE, 2) as REVENUE,
    COALESCE(FORMAT(GROWTH_RATE, 2), 'N/A') as GROWTH_RATE_PERCENTAGE
FROM GrowthRate
ORDER BY STORE_NAME, QUARTER;

-- ============================================
-- Q3: Detailed Supplier Sales Contribution by Store and Product Category
-- ============================================
WITH StoreSales AS (
    SELECT 
        ds.STORE_NAME,
        dsup.SUPPLIER_NAME,
        dp.PRODUCT_NAME,
        SUM(fs.SALES_AMOUNT) as TOTAL_SALES,
        SUM(SUM(fs.SALES_AMOUNT)) OVER (PARTITION BY ds.STORE_NAME) as STORE_TOTAL,
        SUM(SUM(fs.SALES_AMOUNT)) OVER (PARTITION BY ds.STORE_NAME, dsup.SUPPLIER_NAME) as SUPPLIER_TOTAL
    FROM FACT_SALES fs
    JOIN DIM_STORE ds ON fs.STORE_KEY = ds.STORE_KEY
    JOIN DIM_SUPPLIER dsup ON fs.SUPPLIER_KEY = dsup.SUPPLIER_KEY
    JOIN DIM_PRODUCT dp ON fs.PRODUCT_KEY = dp.PRODUCT_KEY
    GROUP BY ds.STORE_NAME, dsup.SUPPLIER_NAME, dp.PRODUCT_NAME
)
SELECT 
    STORE_NAME,
    SUPPLIER_NAME,
    PRODUCT_NAME,
    FORMAT(TOTAL_SALES, 2) as TOTAL_SALES,
    FORMAT((TOTAL_SALES / STORE_TOTAL * 100), 2) as STORE_CONTRIBUTION_PCT,
    FORMAT((TOTAL_SALES / SUPPLIER_TOTAL * 100), 2) as SUPPLIER_CONTRIBUTION_PCT
FROM StoreSales
ORDER BY STORE_NAME, SUPPLIER_NAME, TOTAL_SALES DESC;

-- ============================================
-- Q4: Seasonal Analysis of Product Sales Using Dynamic Drill-Down
-- ============================================
SELECT 
    dp.PRODUCT_NAME,
    GetSeason(dd.MONTH) as SEASON,
    dd.YEAR,
    SUM(fs.QUANTITY) as TOTAL_QUANTITY,
    SUM(fs.SALES_AMOUNT) as TOTAL_SALES,
    COUNT(*) as NUMBER_OF_ORDERS
FROM FACT_SALES fs
JOIN DIM_PRODUCT dp ON fs.PRODUCT_KEY = dp.PRODUCT_KEY
JOIN DIM_DATE dd ON fs.DATE_KEY = dd.DATE_KEY
GROUP BY dp.PRODUCT_NAME, SEASON, dd.YEAR
ORDER BY dp.PRODUCT_NAME, dd.YEAR, 
    CASE GetSeason(dd.MONTH)
        WHEN 'Spring' THEN 1 
        WHEN 'Summer' THEN 2 
        WHEN 'Fall' THEN 3 
        WHEN 'Winter' THEN 4 
    END;

-- ============================================
-- Q5: Store-Wise and Supplier-Wise Monthly Revenue Volatility
-- ============================================
WITH MonthlyRevenue AS (
    SELECT 
        ds.STORE_NAME,
        dsup.SUPPLIER_NAME,
        dd.MONTH,
        dd.YEAR,
        SUM(fs.SALES_AMOUNT) as REVENUE
    FROM FACT_SALES fs
    JOIN DIM_STORE ds ON fs.STORE_KEY = ds.STORE_KEY
    JOIN DIM_SUPPLIER dsup ON fs.SUPPLIER_KEY = dsup.SUPPLIER_KEY
    JOIN DIM_DATE dd ON fs.DATE_KEY = dd.DATE_KEY
    GROUP BY ds.STORE_NAME, dsup.SUPPLIER_NAME, dd.MONTH, dd.YEAR
),
Volatility AS (
    SELECT 
        STORE_NAME,
        SUPPLIER_NAME,
        MONTH,
        YEAR,
        REVENUE,
        LAG(REVENUE) OVER (PARTITION BY STORE_NAME, SUPPLIER_NAME ORDER BY YEAR, MONTH) as PREV_REVENUE,
        ((REVENUE - LAG(REVENUE) OVER (PARTITION BY STORE_NAME, SUPPLIER_NAME ORDER BY YEAR, MONTH)) / 
         NULLIF(LAG(REVENUE) OVER (PARTITION BY STORE_NAME, SUPPLIER_NAME ORDER BY YEAR, MONTH), 0) * 100) as VOLATILITY
    FROM MonthlyRevenue
)
SELECT 
    STORE_NAME,
    SUPPLIER_NAME,
    CONCAT(YEAR, '-', LPAD(MONTH, 2, '0')) as MONTH_YEAR,
    FORMAT(REVENUE, 2) as REVENUE,
    COALESCE(FORMAT(VOLATILITY, 2), 'N/A') as REVENUE_CHANGE_PCT
FROM Volatility
WHERE VOLATILITY IS NOT NULL
ORDER BY ABS(VOLATILITY) DESC;

-- ============================================
-- Q6: Top 5 Products Purchased Together (Product Affinity Analysis)
-- ============================================
WITH ProductPairs AS (
    SELECT 
        dp1.PRODUCT_NAME as PRODUCT1,
        dp2.PRODUCT_NAME as PRODUCT2,
        COUNT(*) as PAIR_COUNT
    FROM FACT_SALES fs1
    JOIN FACT_SALES fs2 ON fs1.DATE_KEY = fs2.DATE_KEY 
        AND fs1.STORE_KEY = fs2.STORE_KEY
        AND fs1.PRODUCT_KEY < fs2.PRODUCT_KEY
    JOIN DIM_PRODUCT dp1 ON fs1.PRODUCT_KEY = dp1.PRODUCT_KEY
    JOIN DIM_PRODUCT dp2 ON fs2.PRODUCT_KEY = dp2.PRODUCT_KEY
    GROUP BY dp1.PRODUCT_NAME, dp2.PRODUCT_NAME
)
SELECT 
    PRODUCT1,
    PRODUCT2,
    PAIR_COUNT,
    DENSE_RANK() OVER (ORDER BY PAIR_COUNT DESC) as RANK
FROM ProductPairs
WHERE PAIR_COUNT > 1
ORDER BY PAIR_COUNT DESC
LIMIT 5;

-- ============================================
-- Q7: Yearly Revenue Trends with ROLLUP
-- ============================================
SELECT 
    COALESCE(ds.STORE_NAME, 'ALL STORES') as STORE_NAME,
    COALESCE(dsup.SUPPLIER_NAME, 'ALL SUPPLIERS') as SUPPLIER_NAME,
    COALESCE(dp.PRODUCT_NAME, 'ALL PRODUCTS') as PRODUCT_NAME,
    dd.YEAR,
    SUM(fs.SALES_AMOUNT) as TOTAL_REVENUE,
    COUNT(*) as NUMBER_OF_ORDERS
FROM FACT_SALES fs
JOIN DIM_STORE ds ON fs.STORE_KEY = ds.STORE_KEY
JOIN DIM_SUPPLIER dsup ON fs.SUPPLIER_KEY = dsup.SUPPLIER_KEY
JOIN DIM_PRODUCT dp ON fs.PRODUCT_KEY = dp.PRODUCT_KEY
JOIN DIM_DATE dd ON fs.DATE_KEY = dd.DATE_KEY
GROUP BY 
    ds.STORE_NAME, 
    dsup.SUPPLIER_NAME, 
    dp.PRODUCT_NAME,
    dd.YEAR
WITH ROLLUP;

-- ============================================
-- Q8: H1/H2 Revenue and Volume Analysis
-- ============================================
SELECT 
    dp.PRODUCT_NAME,
    SUM(CASE WHEN dd.MONTH <= 6 THEN fs.SALES_AMOUNT ELSE 0 END) as H1_REVENUE,
    SUM(CASE WHEN dd.MONTH <= 6 THEN fs.QUANTITY ELSE 0 END) as H1_QUANTITY,
    SUM(CASE WHEN dd.MONTH > 6 THEN fs.SALES_AMOUNT ELSE 0 END) as H2_REVENUE,
    SUM(CASE WHEN dd.MONTH > 6 THEN fs.QUANTITY ELSE 0 END) as H2_QUANTITY,
    SUM(fs.SALES_AMOUNT) as YEARLY_REVENUE,
    SUM(fs.QUANTITY) as YEARLY_QUANTITY,
    ((SUM(CASE WHEN dd.MONTH > 6 THEN fs.SALES_AMOUNT ELSE 0 END) - 
      SUM(CASE WHEN dd.MONTH <= 6 THEN fs.SALES_AMOUNT ELSE 0 END)) / 
     NULLIF(SUM(CASE WHEN dd.MONTH <= 6 THEN fs.SALES_AMOUNT ELSE 0 END), 0) * 100) as GROWTH_RATE
FROM FACT_SALES fs
JOIN DIM_PRODUCT dp ON fs.PRODUCT_KEY = dp.PRODUCT_KEY
JOIN DIM_DATE dd ON fs.DATE_KEY = dd.DATE_KEY
GROUP BY dp.PRODUCT_NAME
ORDER BY YEARLY_REVENUE DESC;

-- ============================================
-- Q9: Sales Spike Analysis and Outlier Detection
-- ============================================
WITH DailyStats AS (
    SELECT 
        dp.PRODUCT_NAME,
        dd.FULL_DATE as SALE_DATE,
        SUM(fs.SALES_AMOUNT) as DAILY_SALES,
        AVG(SUM(fs.SALES_AMOUNT)) OVER (PARTITION BY dp.PRODUCT_NAME) as AVG_DAILY_SALES,
        STDDEV(SUM(fs.SALES_AMOUNT)) OVER (PARTITION BY dp.PRODUCT_NAME) as STDDEV_DAILY_SALES
    FROM FACT_SALES fs
    JOIN DIM_PRODUCT dp ON fs.PRODUCT_KEY = dp.PRODUCT_KEY
    JOIN DIM_DATE dd ON fs.DATE_KEY = dd.DATE_KEY
    GROUP BY dp.PRODUCT_NAME, dd.FULL_DATE
)
SELECT 
    PRODUCT_NAME,
    SALE_DATE,
    FORMAT(DAILY_SALES, 2) as DAILY_SALES,
    FORMAT(AVG_DAILY_SALES, 2) as AVG_DAILY_SALES,
    FORMAT(((DAILY_SALES - AVG_DAILY_SALES) / AVG_DAILY_SALES * 100), 2) as DEVIATION_PCT,
    CASE 
        WHEN DAILY_SALES > (AVG_DAILY_SALES + 2 * STDDEV_DAILY_SALES) THEN 'HIGH SPIKE'
        WHEN DAILY_SALES < (AVG_DAILY_SALES - 2 * STDDEV_DAILY_SALES) THEN 'LOW SPIKE'
        ELSE 'NORMAL'
    END as SALES_PATTERN
FROM DailyStats
WHERE DAILY_SALES > (AVG_DAILY_SALES + 2 * STDDEV_DAILY_SALES)
   OR DAILY_SALES < (AVG_DAILY_SALES - 2 * STDDEV_DAILY_SALES)
ORDER BY ABS(DAILY_SALES - AVG_DAILY_SALES) DESC;

-- ============================================
-- Q10: REGION_STORE_QUARTERLY_SALES View
-- ============================================
CREATE OR REPLACE VIEW REGION_STORE_QUARTERLY_SALES AS
SELECT 
    ds.STORE_NAME,
    dd.YEAR,
    dd.QUARTER,
    COUNT(*) as TOTAL_ORDERS,
    SUM(fs.QUANTITY) as TOTAL_QUANTITY,
    SUM(fs.SALES_AMOUNT) as TOTAL_SALES,
    AVG(fs.SALES_AMOUNT) as AVG_ORDER_VALUE,
    SUM(fs.SALES_AMOUNT) / COUNT(*) as AVG_TRANSACTION_VALUE,
    COUNT(DISTINCT dd.MONTH) as ACTIVE_MONTHS
FROM FACT_SALES fs
JOIN DIM_STORE ds ON fs.STORE_KEY = ds.STORE_KEY
JOIN DIM_DATE dd ON fs.DATE_KEY = dd.DATE_KEY
WHERE dd.YEAR >= 2000  -- Filter out invalid years (data quality check)
GROUP BY 
    ds.STORE_NAME,
    dd.YEAR,
    dd.QUARTER
ORDER BY 
    ds.STORE_NAME,
    dd.YEAR,
    dd.QUARTER;
