import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import requests
from pyspark.sql.functions import *
from datetime import datetime 
import boto3
import json
import time  
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

symbol = ['JPM' , 'PG' , 'BAC' , 'WFC' , 'GS' , 'KO' , 'PEP' , 'NKE' ,'MS','CL']
bucket = 'bucket_name' 


s3 = boto3.client(
    's3',
    region_name='your_region',  
    aws_access_key_id='Your_access_id',
    aws_secret_access_key='Your_secret_id'
)

for element in symbol:

    file_path1 = f"raw_data/stock_info/{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json"
    file_path2 = f"raw_data/company_info/{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json"


    url1 = f'https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol={element}&apikey=Your_key'
    r = requests.get(url1)
    data1 = r.json()
    json_data1 = json.dumps(data1)

    url2 = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={element}&apikey=Your_key'
    r = requests.get(url2)
    data2 = r.json()
    json_data2 = json.dumps(data2)

    s3.put_object(
    Bucket=bucket,
    Key=file_path1,
    Body=json_data1,
    ContentType='application/json'
    )

    s3.put_object(
    Bucket=bucket,
    Key=file_path2,
    Body=json_data2,
    ContentType='application/json'
    )

    time.sleep(1)

job.commit()
