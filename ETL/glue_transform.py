import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import *
from datetime import datetime 
import boto3
import time 
from pyspark.sql.types import DateType,DoubleType,LongType
import json
import time 
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

def stock_info(data):

    week_data = data['Weekly Time Series']
    symbol = data["Meta Data"]["2. Symbol"]
    rows = [(symbol,date,metrics) for date,metrics in week_data.items()]
    df = spark.createDataFrame(rows ,['Symbol' , 'Date' , 'Metrics'])

    df_final = df.select(
    col('Symbol').alias('Symbol'),
    col('Date').cast(DateType()).alias('Date'),
    col("Metrics.`1. open`").cast(DoubleType()).alias('Open'),
    col("Metrics.`2. high`").cast(DoubleType()).alias('High'),
    col("Metrics.`3. low`").cast(DoubleType()).alias('Low'),
    col("Metrics.`4. close`").cast(DoubleType()).alias('Close'),
    col("Metrics.`5. volume`").cast(LongType()).alias('Volume')
    )

    return df_final


def company_info(data):
    row = [(data["Symbol"] , 
            data["AssetType"],
            data["Name"],
            data["Description"],
            data["Exchange"],
            data["Currency"],
            data["Country"],
            data["Sector"],
            data["Industry"],
            data["MarketCapitalization"]
            )]
    df = spark.createDataFrame(row,["Symbol" ,"AssetType","Name","Description","Exchange","Currency","Country","Sector","Industry","MarketCapitalization" ])
    df = df.withColumn("MarketCapitalization", col("MarketCapitalization").cast(LongType()))
    return df
    
    
s3 = boto3.client(
    's3',
    region_name='your_region',  
    aws_access_key_id='your_key',
    aws_secret_access_key='your_secret'
)

bucket = 'bucket_name'
file_path1 = 'raw_data/stock_info/'

file_key1 = []
for file in s3.list_objects(Bucket = bucket, Prefix = file_path1)['Contents']:
    if file['Key'].split('.')[-1] == 'json':
        key = file['Key']
        file_key1.append(key)
        response = s3.get_object(Bucket = bucket , Key = key) 
        content = response['Body']
        data = json.loads(content.read())

        write_location1 = f"s3://stock-data203/transformed_data/stock_info/{datetime.now().strftime('%Y-%m-%d_%H-%M-%S-%f')}"
        df_stock = stock_info(data)
        df_stock.write.mode("overwrite").option("header" , "true").format("csv").save(write_location1)


file_path2 = 'raw_data/company_info/'
file_key2 = []

df_list = []
for file in s3.list_objects(Bucket = bucket, Prefix = file_path2)['Contents']:
    if file['Key'].split('.')[-1] == 'json':
        key = file['Key']
        file_key2.append(key)
        response = s3.get_object(Bucket = bucket , Key = key)
        content = response['Body']
        data = json.loads(content.read())

        df_company = company_info(data)
        df_list.append(df_company)
df_final = df_list[0]
for df in df_list[1:]:
    df_final = df_final.unionAll(df)

write_location2 = f"s3://stock-data203/transformed_data/company_info/{datetime.now().strftime('%Y-%m-%d_%H-%M-%S-%f')}"
df_final.write.mode("overwrite").option("header" , "true").format("csv").save(write_location2)


for key in file_key1:
    s3.delete_object(Bucket = bucket , Key = key)
for key in file_key2:
    s3.delete_object(Bucket = bucket , Key = key)
    
job.commit()
