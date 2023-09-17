# Q3)  You need to set up a scheduled Python script that extracts data from an API, transforms the data, and loads it into an S3 bucket. 
# Describe the steps you would take to set up the ETL process, 
# and write a sample Python script to perform the ETL operations.
##################################################################################################################################################################

# There are at least three ways to schedule call from API, ETL to S3.
# method 1. use eventbridge to schedule trigger the lambda depending on the cron expression, lambda will do the etl and load to S3

# method 2. With power automate premium, schedule local script can be trigger to automate the API to S3 bucket.

# method 3. lambda has a compute limit of 15 minutes. AWS glue is another etl tool which could be used for heavy etl 
# if the processing takes more than 15 minutes. However, glue is costly compared to lambda.

# Assumption: 
# - Assume the script is standalone not linked to other scripts. some of the functions are repeated in other questions. Importing function would be a good practice.
# - Assume that eventbridge has schedule to trigger lambda daily at specific time, 3am  cron(0 0 19 1/1 * ? *)
# - Data require small-medium workload (up to 1 million) and in CSV format. Else, Glue will be a better option.
# - This is a batch ETL not streaming (real-time) ETL
# - Lambda and S3 are in the same VPC
# - IAM role permission is set up

##################################################################################################################################################################
# This python (3.11.4) is adopted to lambda.
# A sample API call is given.

# Step 0. lambda schedule for daily extraction. The schedule comes on 3am to avoid heavy compution during daytime.
# Step 1. Connect API and S3 and check API connection
# Step 2. check for if bucket and file exist
# Step 3. Call the API to get the reponse
# Step 4. pandas to do the transformation
# Step 5. convert dataframe to parquet for compression
# Step 6. load the parquet to S3
##################################################################################################################################################################

import requests
from datetime import datetime
import os
import logging
import boto3
import pandas as pd
import configparser
import json


# Set up AWS credentials (ensure AWS CLI or environment variables are configured)
# default to be left as blank for aws_access_key_id and aws_secret_access_key
# get_aws_credentials method will obtain the credentials through '~/.aws/credentials'
# best practice is to store the credentials in secret manager for store and retrieval, not expose the credentials in the script
# Insert the region_name for testing
region_name = ''

config = configparser.RawConfigParser()
aws_credentials_path = '~/.aws/credentials'


# Specify your S3 bucket and file path
# comment out bucket_name and file_key as lambda_handler will obtain the parameters
# bucket_name = 'demo-alan-wong-s3'
# file_key = r'demo/ingestion/year=2023/month=09/day=15/xAPI-Edu-Data.csv'
# archive_file_key = r'demo/archive/xAPI-Edu-Data.csv'

# specifiy the URL, use a singapore api for get request
# change the URL to fit to the testing
URL = 'https://api.data.gov.sg/v1/environment/pm25'
key_prefix = 'demo/ingestion/year=2023/month=09/day=15/'

logging.basicConfig(level = logging.INFO)

# Obtain the access key and access key if no hardcoded values were given 
def get_aws_credentials(aws_credentials_path):
    path = os.path.expanduser(aws_credentials_path)
    config_path = config.read(path)
    if os.path.exists(config_path[0]):
        aws_access_key_id = config.get('default', 'aws_access_key_id')
        aws_secret_access_key = config.get('default', 'aws_secret_access_key')
        return aws_access_key_id, aws_secret_access_key
    else:
        logging.error(f'{config_path[0]} is not a aws config file')

def bucket_exist(client,bucket_name):
    buckets = client.list_buckets()['Buckets']
    for bucket in buckets:
        if bucket['Name'] == bucket_name:
            logging.info(f'This bucket {bucket_name} is found')
        else:
            logging.error(f'This bucket {bucket_name} is not found...')

def file_exist(client, bucket_name, file_key):
    keys = client.list_objects_v2(Bucket=bucket_name)['Contents']
    for key in keys:
        if key['Key'] == file_key:
            logging.info(f'{file_key} is found in {bucket_name}')

def file_format_parquet(file_key):
    file_extension = file_key.split('.')[-1]
    if file_extension == 'parquet':
        logging.info(f'{file_key} is a parquet file')
    else:
        logging.error(f'{file_key} is not parquet file')

def move_object(client,bucket_name,source_file_key,destination_file_key):\
    # copy the object to destination location
    client.copy_object(
    CopySource={'Bucket': bucket_name, 'Key': source_file_key},
    Bucket=bucket_name,
    Key=destination_file_key
    )

    # delete the object from the source location
    client.delete_object(Bucket=bucket_name, Key=source_file_key)

    logging.info(f'S3 object moved from {source_file_key} to {destination_file_key}')
def get_datetime_now():
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

def PARAMS():
    date_time = get_datetime_now()
    return {'date_time':date_time}

def request(url,parameters):
    r = requests.get(url,params=parameters)
    if r.status_code == 200:
        logging.info(f'successful API {r.status_code}')
        return r
    else:
        logging.error(f'error API {r.status_code}')

def get_body_reponse(response):
    if response['api_info']['status'] == 'healthy':
        return response

def json_normalised_dataframe(json):
    # extracting data in json format
    df = pd.json_normalize(json)
    # create timestamp for audit purpose
    df['created_date_time'] = get_datetime_now()
    return df

def transformation_function(df):
    # do transformation here
    return df

def get_ingestion_key_parquet_path(bucket, key_prefix, now):
    # obtain the s3 path to the parquet
    path = f'{key_prefix}/{datetime.now().strftime("%Y-%m-%d")}/transformed_{now}.parquet'
    logging.info(f'{path} will be stored in {bucket}')
    return path

def lambda_handler(event, context):

    logging.info("Received event: " + json.dumps(event, indent=2))
    # Get the bucket from the event and show its content type
    bucket_name = event['Records'][0]['s3']['bucket']['name']

    # 1. Connect API and S3 and check API connection
    aws_access_key_id, aws_secret_access_key = get_aws_credentials(aws_credentials_path)

    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, 
                      aws_secret_access_key=aws_secret_access_key, 
                      region_name=region_name)

    # 2. Check if the bucket exists
    bucket_exist(s3_client,bucket_name)

    datetime_now = get_datetime_now()
    try:
        # 3. Call an API to get a response
        r = request(URL,PARAMS())
        content = r.json()
        data = get_body_reponse(content)
        items = data['items']
        dataframe = json_normalised_dataframe(items)

        # 4. pandas to do the transformation
        transformed_dataframe = transformation_function(dataframe)
        # sample the dataframe
        logging.info(transformed_dataframe.head(5))

        # 5. convert dataframe to parquet for compression
        df_parquet = transformed_dataframe.to_parquet(engine='pyarrow')

        # specify the key to the parquet
        s3_key_parquet = get_ingestion_key_parquet_path(bucket_name, key_prefix, datetime_now)

        # 6. load the parquet to S3
        s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key_parquet,
        Body=df_parquet,  
        ContentType='parquet'  
        )

        logging.info("Data loaded from API to S3 successfully.")
    except Exception as e:
        logging.error("Error: Failed to get data from API to S3.")
        logging.error(e)


# uncomment this section for exact lambda usage on AWS
if __name__ == '__main__':
    # mock a fake event to lambda_handler
    # 0. lambda listen to S3 event, the script will run when event is sent
    # please modify the bucket name in fake_event to test the script
    file = open('fake_event.json')
    fake_event = json.load(file)
    logging.info(fake_event)
    lambda_handler(fake_event, 'context')
