USE [PZENA]
GO
 
/****** Object:  Table [dbo].[TICKERS]    Script Date: 8/29/2024 4:04:26 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO
select ISNUMERIC(permaticker), permaticker 
from dbo.STG_TICKERS
where ISNUMERIC(permaticker) <> 1

SELECT TICKER, COUNT(*)
FROM dbo.STG_TICKERS
GROUP BY TICKER
HAVING COUNT(*) > 1

SELECT permaticker, COUNT(*)
FROM dbo.STG_TICKERS
GROUP BY permaticker
HAVING COUNT(*) > 1

CREATE TABLE [dbo].[TICKERS](
	[table] [char](3) NOT NULL,
	[permaticker] INT NOT NULL,
	[ticker] [varchar](50) NOT NULL,
	[name] [varchar](255) NOT NULL,
	[exchange] [varchar](50) NOT NULL,
	[isdelisted] [char](1) NOT NULL,
	[category] [varchar](255) NOT NULL,
	[cusips] [varchar](50) NULL,
	[siccode] [varchar](50) NULL,
	[sicsector] [varchar](255) NULL,
	[sicindustry] [varchar](255) NULL,
	[famasector] [varchar](255) NULL,
	[famaindustry] [varchar](255) NULL,
	[sector] [varchar](255) NULL,
	[industry] [varchar](255) NULL,
	[scalemarketcap] [varchar](255) NULL,
	[scalerevenue] [varchar](255) NULL,
	[relatedtickers] [varchar](255) NULL,
	[currency] [char](3) NOT NULL,
	[location] [varchar](255) NULL,
	[lastupdated] [date] NOT NULL,
	[firstadded] [date] NOT NULL,
	[firstpricedate] [date] NOT NULL,
	[lastpricedate] [date] NOT NULL,
	[firstquarter] [date] NULL,
	[lastquarter] [date] NULL,
	[secfilings] [varchar](500) NOT NULL,
	[companysite] [varchar](255) NULL,
 CONSTRAINT [PK_TICKERS] PRIMARY KEY CLUSTERED 
(
	[permaticker] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO


MERGE dbo.TICKERS AS TGT
USING (SELECT 
			  CONVERT(char(3), [table]) AS [table]
			, CONVERT(INT, permaticker) AS permaticker
			, CONVERT(varchar(50), ticker) AS ticker
			, CONVERT(varchar(255), [name]) AS [name]
			, CONVERT(varchar(50), exchange) AS exchange
			, CONVERT(char(1), isdelisted) AS isdelisted
			, CONVERT(varchar(255), category) AS category
			, CONVERT(varchar(50), CASE WHEN cusips = '' THEN NULL ELSE cusips END) AS cusips
			, CONVERT(varchar(50), CASE WHEN siccode = '' THEN NULL ELSE siccode END) AS siccode
			, CONVERT(varchar(255), CASE WHEN sicsector = '' THEN NULL ELSE sicsector END) AS sicsector
			, CONVERT(varchar(255), CASE WHEN sicindustry = '' THEN NULL ELSE sicindustry END) AS sicindustry
			, CONVERT(varchar(255), CASE WHEN famasector = '' THEN NULL ELSE famasector END) AS famasector
			, CONVERT(varchar(255), CASE WHEN famaindustry = '' THEN NULL ELSE famaindustry END) AS famaindustry
			, CONVERT(varchar(255), CASE WHEN sector = '' THEN NULL ELSE sector END) AS sector
			, CONVERT(varchar(255), CASE WHEN industry = '' THEN NULL ELSE industry END) AS industry
			, CONVERT(varchar(255), CASE WHEN scalemarketcap = '' THEN NULL ELSE scalemarketcap END) AS scalemarketcap
			, CONVERT(varchar(255), CASE WHEN scalerevenue = '' THEN NULL ELSE scalerevenue END) AS scalerevenue
			, CONVERT(varchar(255), CASE WHEN relatedtickers = '' THEN NULL ELSE relatedtickers END) AS relatedtickers
			, CONVERT(char(3), currency) AS currency
			, CONVERT(varchar(255), CASE WHEN [location] = '' THEN NULL ELSE [location] END) AS [location]
			, CONVERT(date, lastupdated) AS lastupdated
			, CONVERT(date, firstadded) AS firstadded
			, CONVERT(date, firstpricedate) AS firstpricedate
			, CONVERT(date, lastpricedate) AS lastpricedate
			, CONVERT(date, CASE WHEN firstquarter = '' THEN NULL ELSE firstquarter END) AS firstquarter
			, CONVERT(date, CASE WHEN lastquarter = '' THEN NULL ELSE lastquarter END) AS lastquarter
			, CONVERT(varchar(500), secfilings) AS secfilings
			, CONVERT(varchar(255), CASE WHEN companysite = '' THEN NULL ELSE companysite END) AS companysite
		FROM dbo.STG_TICKERS) AS SRC
    ON (TGT.permaticker = SRC.permaticker)
WHEN MATCHED
    THEN
        UPDATE
        SET  
			TGT.[table] = SRC.[table]
			--, TGT.permaticker = SRC.permaticker
			, TGT.ticker = SRC.ticker
			, TGT.[name] = SRC.[name]
			, TGT.exchange = SRC.exchange
			, TGT.isdelisted = SRC.isdelisted
			, TGT.category = SRC.category
			, TGT.cusips = SRC.cusips
			, TGT.siccode = SRC.siccode
			, TGT.sicsector = SRC.sicsector
			, TGT.sicindustry = SRC.sicindustry
			, TGT.famasector = SRC.famasector
			, TGT.famaindustry = SRC.famaindustry
			, TGT.sector = SRC.sector
			, TGT.industry = SRC.industry
			, TGT.scalemarketcap = SRC.scalemarketcap
			, TGT.scalerevenue = SRC.scalerevenue
			, TGT.relatedtickers = SRC.relatedtickers
			, TGT.currency = SRC.currency
			, TGT.[location] = SRC.[location]
			, TGT.lastupdated = SRC.lastupdated
			, TGT.firstadded = SRC.firstadded
			, TGT.firstpricedate = SRC.firstpricedate
			, TGT.lastpricedate = SRC.lastpricedate
			, TGT.firstquarter = SRC.firstquarter
			, TGT.lastquarter = SRC.lastquarter
			, TGT.secfilings = SRC.secfilings
			, TGT.companysite = SRC.companysite
WHEN NOT MATCHED
    THEN
        INSERT (
				[table]
				, permaticker
				, ticker
				, [name]
				, exchange
				, isdelisted
				, category
				, cusips
				, siccode
				, sicsector
				, sicindustry
				, famasector
				, famaindustry
				, sector
				, industry
				, scalemarketcap
				, scalerevenue
				, relatedtickers
				, currency
				, [location]
				, lastupdated
				, firstadded
				, firstpricedate
				, lastpricedate
				, firstquarter
				, lastquarter
				, secfilings
				, companysite
			)
			VALUES (
				  SRC.[table]
				, SRC.permaticker
				, SRC.ticker
				, SRC.[name]
				, SRC.exchange
				, SRC.isdelisted
				, SRC.category
				, SRC.cusips
				, SRC.siccode
				, SRC.sicsector
				, SRC.sicindustry
				, SRC.famasector
				, SRC.famaindustry
				, SRC.sector
				, SRC.industry
				, SRC.scalemarketcap
				, SRC.scalerevenue
				, SRC.relatedtickers
				, SRC.currency
				, SRC.[location]
				, SRC.lastupdated
				, SRC.firstadded
				, SRC.firstpricedate
				, SRC.lastpricedate
				, SRC.firstquarter
				, SRC.lastquarter
				, SRC.secfilings
				, SRC.companysite
			);




select count(*) from dbo.TICKERS
