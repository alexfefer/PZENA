USE [PZENA]
GO
 
/****** Object:  Table [dbo].[PRICES]    Script Date: 8/29/2024 6:15:49 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[PRICES](
	[permaticker] INT NOT NULL,
	[date] [date] NOT NULL,
	[open_price] [float] NULL,
	[high_price] [float] NULL,
	[low_price] [float] NULL,
	[close_price] [float] NULL,
	[volume] [float] NULL,
	[close_price_adjusted] [float] NULL,
	[close_price_unadjusted] [float] NULL,
	[lastupdated] [datetime] NULL,
 CONSTRAINT [PK_PRICES] PRIMARY KEY CLUSTERED 
(
	[permaticker] ASC,
	[date] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[PRICES] ALTER COLUMN permaticker INT NOT NULL

/****** Object:  Index [PK_PRICES]    Script Date: 8/29/2024 7:15:39 PM ******/
ALTER TABLE [dbo].[PRICES] ADD  CONSTRAINT [PK_PRICES] PRIMARY KEY CLUSTERED 
(
	[permaticker] ASC,
	[date] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO
-- 1 MINUTE AFTER DATA IS LOADED.


ALTER TABLE [dbo].[PRICES]  drop  CONSTRAINT [FK_PRICES_TICKERS] -- dropping this constraint to speed up Merge
ALTER TABLE [dbo].[PRICES]  WITH CHECK ADD  CONSTRAINT [FK_PRICES_TICKERS] FOREIGN KEY([permaticker])
REFERENCES [dbo].[TICKERS] ([permaticker])
GO
ALTER TABLE [dbo].[PRICES] CHECK CONSTRAINT [FK_PRICES_TICKERS]
GO

ALTER TABLE dbo.PRICES ALTER COLUMN permaticker INT

-- USING INSERT instead of Merge. Merge is taking too long. Should work on a small delta
INSERT INTO dbo.PRICES
SELECT
		CONVERT(INT, t.permaticker) AS permaticker
	, CONVERT(date, p.date) AS date
	, CONVERT(FLOAT, p.[open]) AS open_price
	, CONVERT(FLOAT, p.high) AS high_price
	, CONVERT(FLOAT, p.low) AS low_price
	, CONVERT(FLOAT, p.[close]) AS close_price
	, CONVERT(FLOAT, p.volume) AS volume
	, CONVERT(FLOAT, p.closeadj) AS close_price_adjusted
	, CONVERT(FLOAT, p.closeunadj) AS close_price_unadjusted
	, CONVERT(DATETIME, p.lastupdated) AS lastupdated
FROM [dbo].[STG_PRICES] p
	INNER JOIN dbo.TICKERS t ON t.TICKER = p.TICKER
	
-- 43,753,427 rows affected -- TAKING 15 MINUTES


-- Investigate unmatched tickers
SELECT p.*
INTO #TEMP
FROM [dbo].[STG_PRICES] p
	LEFT OUTER JOIN dbo.TICKERS t ON t.TICKER = p.TICKER
WHERE t.TICKER IS NULL
--3567 rows affected

SELECT DISTINCT TICKER FROM #TEMP
-- The below tickers are in the PRICE file, but not in the TICKERS file. Need a Ticker history file to analyze properly.
TICKER
INXB
ESACW
ESAC
XHG
PUYI
