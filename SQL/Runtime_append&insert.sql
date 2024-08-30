USE DATABASE stock_db;
USE SCHEMA stock;


CREATE OR REPLACE TABLE staging_dim_date(
    date_id number autoincrement,
    Date timestamp,
    Year number,
    Month number,
    Day number,
    Day_of_week number
);

INSERT INTO staging_dim_date (Date , Year , Month , Day , Day_of_week)
    SELECT DISTINCT
        Date,
        extract(YEAR from Date) as Year,
        extract(MONTH from Date) as Month,
        extract(DAY from Date ) as Day,
        extract(DOW from Date) as Day_of_week
    FROM
        stock_info;

MERGE INTO dim_date as d
USING staging_dim_date as sd
ON d.Date = sd.Date 
WHEN NOT MATCHED THEN
INSERT (Date, Year, Month, Day, Day_of_week)
VALUES (sd.Date, sd.Year, sd.Month, sd.Day, sd.Day_of_week);




CREATE OR REPLACE TABLE staging_dim_company(
    company_id number autoincrement,
    Symbol varchar(50),
    AssetType varchar(50),
    Name varchar(100),
    Description string,
    Exchange varchar(50),
    Currency varchar(50),
    Country varchar(70),
    Sector string,
    Industry string,
    MarketCapitalization Number
);

INSERT INTO staging_dim_company ( Symbol, AssetType, Name, Description, Exchange, Currency, Country, Sector, Industry, MarketCapitalization)
    SELECT DISTINCT
        Symbol, 
        AssetType,
        Name, 
        Description, 
        Exchange, 
        Currency, 
        Country, 
        Sector, 
        Industry, 
        MarketCapitalization
    FROM 
        company_info;

MERGE INTO dim_company as c
USING staging_dim_company as sc
ON c.Symbol = sc.Symbol
WHEN NOT MATCHED THEN
INSERT (Symbol, AssetType, Name, Description, Exchange, Currency, Country, Sector, Industry, MarketCapitalization)
VALUES (sc.Symbol, sc.AssetType, sc.Name, sc.Description, sc.Exchange, sc.Currency, sc.Country, sc.Sector, sc.Industry, sc.MarketCapitalization);




CREATE OR REPLACE TABLE staging_fact_weekly_stock_prize as (
    select c.company_id, d.date_id, s.Open, s.High, s.Low, s.Close, s.Stock_volume
    from stock_info as s
    inner join dim_company as c
    on s.Symbol = c.Symbol
    inner join dim_date as d 
    on s.Date = d.Date 
);

MERGE INTO fact_weekly_stock_prize as fs
USING staging_fact_weekly_stock_prize AS sfs
ON fs.company_id = sfs.company_id and fs.date_id = sfs.date_id
WHEN NOT MATCHED THEN
INSERT (company_id, date_id, Open, High, Low, Close, Stock_volume)
VALUES(sfs.company_id, sfs.date_id, sfs.Open, sfs.High, sfs.Low, sfs.Close, sfs.Stock_volume);

--select count(*) from dim_company;
--select count(*) from dim_date;
--select count(*) from fact_weekly_stock_prize;


drop table staging_dim_company;
drop table staging_dim_date;
drop table staging_fact_weekly_stock_prize;
truncate table stock_info;
truncate table company_info;
