# Q1)  You are given a CSV file with millions of rows stored in an S3 bucket. 
# Using Python and Boto3, how would you read the file and perform basic data transformations 
# like filtering and aggregation without loading the entire file into memory?
##################################################################################################################################################################

import os
import logging
import boto3
import configparser


# Set up AWS credentials (ensure AWS CLI or environment variables are configured)
# default to be left as blank for aws_access_key_id and aws_secret_access_key
# get_aws_credentials method will obtain the credentials through '~/.aws/credentials'

# insert the region_name to fit to the testing
region_name = ''

config = configparser.RawConfigParser()
aws_credentials_path = '~/.aws/credentials'


# Specify your S3 bucket and file path

# insert the bucket_name and file_key (s3 path to the file) to fit to the testing
bucket_name = ''
file_key = r'demo/ingestion/year=2023/month=09/day=15/xAPI-Edu-Data.csv'

# example query, modify to fit to the testing
SQL_query = 'SELECT * FROM s3object limit 100'

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
    

def bucket_exist(s3,bucket_name):
    buckets = s3.list_buckets()['Buckets']
    for bucket in buckets:
        if bucket['Name'] == bucket_name:
            logging.info(f'This bucket {bucket_name} is found')
        else:
            logging.error(f'This bucket {bucket_name} is not found...')

def file_exist(s3, bucket_name, file_key):
    keys = s3.list_objects_v2(Bucket=bucket_name)['Contents']
    for key in keys:
        if key['Key'] == file_key:
            logging.info(f'{file_key} is found in {bucket_name}')
    file_format_csv(file_key)

def file_format_csv(file_key):
    file_extension = file_key.split('.')[-1]
    if file_extension == 'csv':
        logging.info(f'{file_key} is a csv file')
    else:
        logging.error(f'{file_key} is not csv file')

if __name__=='__main__':

    # 0. connecting to AWS and S3
    aws_access_key_id, aws_secret_access_key = get_aws_credentials(aws_credentials_path)

    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, 
                      aws_secret_access_key=aws_secret_access_key, 
                      region_name=region_name)

    # 1. Check if the bucket and csv file exists
    bucket_exist(s3_client,bucket_name)
    file_exist(s3_client, bucket_name, file_key)

    # 2. Create an S3 select_object_content to read the CSV via SQL expression
    resp = s3_client.select_object_content(
    Bucket=bucket_name,
    Key=file_key,
    ExpressionType='SQL',
    Expression=SQL_query,
    InputSerialization = {'CSV': {"FileHeaderInfo": "Use"}, 'CompressionType': 'NONE'},
    OutputSerialization = {'CSV': {}},
    )
    for event in resp['Payload']:
        if 'Records' in event:
            records = event['Records']['Payload'].decode('utf-8')
            logging.info(records)
        elif 'Stats' in event:
            statsDetails = event['Stats']['Details']
            logging.info("Stats details bytesScanned: ")
            logging.info(statsDetails['BytesScanned'])
            logging.info("Stats details bytesProcessed: ")
            logging.info(statsDetails['BytesProcessed'])
            logging.info("Stats details bytesReturned: ")
            logging.info(statsDetails['BytesReturned'])