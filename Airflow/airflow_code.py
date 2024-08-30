from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
from airflow.operators.python_operator import PythonOperator
import time


def sleep_function(seconds: int):
    time.sleep(seconds)
    
    
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    dag_id = 'stock_project',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

glue_extract_task = GlueJobOperator(
    task_id = 'glue_job_extract',
    job_name='stock_extract',
    script_location='script_location_uri',
    aws_conn_id='conn_id',
    region_name='region',
    iam_role_name='role_name',
    dag=dag,
)

s3_sensor_task1 = S3KeySensor(
    task_id='s3_key_sensor_task1',
    bucket_name='name',
    bucket_key='raw_data/stock_info/*.json',
    wildcard_match = True,
    aws_conn_id='conn_id',
    poke_interval=60,
    timeout=360,
    dag=dag,
)

s3_sensor_task2 = S3KeySensor(
    task_id='s3_key_sensor_task2',
    bucket_name='bucketname',
    bucket_key='raw_data/company_info/*.json',
    wildcard_match = True,
    aws_conn_id='conn_id',
    poke_interval=60,
    timeout=360,
    dag=dag,
)

glue_transform_task = GlueJobOperator(
    task_id = 'glue_job_transform',
    job_name='stock_transform',
    script_location='script_loaction_uri',
    aws_conn_id='conn_id',
    region_name='region',
    iam_role_name='iam_role',
    dag=dag,
)

sleep_task = PythonOperator(
        task_id='sleep_task',
        python_callable=sleep_function,
        op_kwargs={'seconds': 240},  # Adjust the number of seconds as needed
    )

snowflake_query_task = SQLExecuteQueryOperator(
    task_id='snowflake_query_task',
    sql = """ USE DATABASE stock_db;
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
                """,
    conn_id = 'conn_id',
    dag = dag,
)

glue_extract_task >> s3_sensor_task1 
glue_extract_task >> s3_sensor_task2
s3_sensor_task1 >> glue_transform_task
s3_sensor_task2 >> glue_transform_task
glue_transform_task >> sleep_task
sleep_task >> snowflake_query_task
