USE DATABASE stock_db;
USE SCHEMA stock;

CREATE STORAGE INTEGRATION stock_integration
    type = external_stage
    storage_provider = s3
    storage_aws_role_arn = 'arn:aws:iam::010928224012:role/stock_snowflake_integration'
    enabled = true
    storage_allowed_locations = ( 's3://stock-data203/transformed_data/');

--desc integration stock_integration;

CREATE OR REPLACE FILE FORMAT stock_csv_format
    type = 'csv'
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL','null')
    empty_field_as_null = True
    field_optionally_enclosed_by = '"'; 

CREATE OR REPLACE STAGE stock_stage
    url = 's3://stock-data203/transformed_data/'
    file_format = stock_csv_format
    storage_integration = stock_integration;


CREATE OR REPLACE TABLE stock_info(
    Symbol string,
    Date timestamp,
    Open double,
    High double,
    Low double,
    Close double,
    Stock_volume number
);

COPY INTO stock_info From @stock_stage/stock_info/;

CREATE OR REPLACE PIPE stock_data
    auto_ingest = True
    as
    COPY INTO stock_info From @stock_stage/stock_info/;
    
--desc pipe stock_data;

CREATE OR REPLACE PIPE company_data
    auto_ingest = True
    as
    COPY INTO company_info From @stock_stage/company_info/;

--desc pipe company_data;
    
    
CREATE OR REPLACE TABLE company_info(
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

COPY INTO company_info From @stock_stage/company_info/;

--select * from company_info;


CREATE OR REPLACE TABLE dim_date (
    date_id number autoincrement,
    Date timestamp,
    Year number,
    Month number,
    Day number,
    Day_of_week number
);

INSERT INTO dim_date (Date , Year , Month , Day , Day_of_week)
    SELECT DISTINCT
        Date,
        extract(YEAR from Date) as Year,
        extract(MONTH from Date) as Month,
        extract(DAY from Date ) as Day,
        extract(DOW from Date) as Day_of_week
    FROM
        stock_info;

--select * from dim_date where year = 2024;



CREATE OR REPLACE TABLE dim_company(
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

INSERT INTO dim_company ( Symbol, AssetType, Name, Description, Exchange, Currency, Country, Sector, Industry, MarketCapitalization)
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

--select * from dim_company;


CREATE OR REPLACE TABLE fact_weekly_stock_prize as (
    select c.company_id, d.date_id, s.Open, s.High, s.Low, s.Close, s.Stock_volume
    from stock_info as s
    inner join dim_company as c
    on s.Symbol = c.Symbol
    inner join dim_date as d 
    on s.Date = d.Date 
);

select * from fact_weekly_stock_prize;
