USE PZENA
GO
 
ALTER PROCEDURE sp_rptAggregatePriceStats
    @Date DATE
AS
BEGIN

    DECLARE @StartDate DATE = DATEADD(YEAR, -1, @Date);

    WITH RESULT AS (
        SELECT 
            permaticker
            , date
            , close_price
			, close_price_adjusted
            , ROW_NUMBER() OVER (PARTITION BY permaticker ORDER BY date DESC) AS rn
            , COUNT(*) OVER (PARTITION BY permaticker) AS price_count
            , AVG(close_price_adjusted) OVER (PARTITION BY permaticker) AS avg_price
            , MIN(close_price_adjusted) OVER (PARTITION BY permaticker) AS min_price
            , MAX(close_price_adjusted) OVER (PARTITION BY permaticker) AS max_price
        FROM dbo.PRICES
        WHERE date BETWEEN @StartDate AND @Date
    )
    SELECT 
        r.permaticker
		, t.ticker
		, t.name
		, r.close_price_adjusted
		, r.avg_price
		, r.min_price
		, r.max_price
		, r.price_count
		, CASE WHEN r.price_count < 250 then 'WARNING: Less that a year of data is available' ELSE '' END AS Comment --year has at least 250 trading days
    FROM RESULT r
		INNER JOIN dbo.TICKERS t on t.permaticker = r.permaticker
    WHERE rn = 1
END
GO

EXECUTE sp_rptAggregatePriceStats @date = '2023-05-30' -- 4sec
