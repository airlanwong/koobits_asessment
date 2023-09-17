# Q2)  You need to automate the process of moving data from an S3 bucket to an AWS Redshift cluster using Python.
# Describe the steps you would take to set up the ETL process, 
# and write a Python script to load data from S3 to Redshift.
##################################################################################################################################################################

# There are at least three ways to automate the moving data from S3 to Redshift.
# method 1. use lambda as a automated trigger when a csv file is uploaded in the S3 directory. The lambda will copy the data to redshift

# method 2. lambda can be costly if the file require heavy compution. A local script that is manually trigger to automate 
# the proces of copying the csv file from S3 to redshift.

# method 3. lambda has a compute limit of 15 minutes. AWS glue is another etl tool which could be used for heavy transformation 
# if the processing takes more than 15 minutes. However, glue is costly compared to lambda.



# Assumption: 
# - Assume the script is standalone not linked to other scripts. some of the functions are repeated in other questions. Importing function would be a good practice.
# - Table is already created in redshift
# - Data require small-medium workload (up to 1 million) and in CSV format. Else, Glue will be a better option.
# - This is a batch ETL not streaming (real-time) ETL
# - Lambda and Redshift are in the same VPC
# - IAM role permission is set up

##################################################################################################################################################################
# This python (3.11.4) is adopted to lambda.

# Step 0. lambda listen to S3 event. Once file was upload, the lambda will be triggered using a fake event
# Step 1. Connect to Redshift and S3
# Step 2. check for if bucket and file exist
# Step 3. Create a cursor
# Step 4. Copy query with credentials. This is where you could do basic transformation and filter to the data with copy command (extract and transform)
# Step 5. Commit the transaction (load)
# Step 6. Get the row copied to redshift for checking purposes 
# Step 7. Move the CSV file from demo/ingestion to demo/archives to prevent double extraction, and keep the raw file in archive
# Step 8. Move the object to archive
# Step 9. Close the cursor and connection
##################################################################################################################################################################

import os
import logging
import boto3
import pandas as pd
import configparser
import redshift_connector
import json
import urllib.parse


# Set up AWS credentials (ensure AWS CLI or environment variables are configured)
# default to be left as blank for aws_access_key_id and aws_secret_access_key
# get_aws_credentials method will obtain the credentials through '~/.aws/credentials'
# best practice is to store the credentials in secret manager for store and retrieval, not expose the credentials in the script

# insert the region_name to fit to the testing
region_name = ''

config = configparser.RawConfigParser()
aws_credentials_path = '~/.aws/credentials'


# Specify your S3 bucket and file path
# comment out bucket_name and file_key as lambda_handler will obtain the parameters
# bucket_name = 'demo-alan-wong-s3'
# file_key = r'demo/ingestion/year=2023/month=09/day=15/xAPI-Edu-Data.csv'
# archive_file_key = r'demo/archive/xAPI-Edu-Data.csv'


# Redshift connection parameters
# best practice is to store the credentials in secret manager for store and retrieval, not expose the credentials in the script

# insert the redshift credentials to fit to the testing
redshift_cluster = ''
redshift_host = ''
redshift_port = 5439
redshift_database = ''
redshift_user = ''
redshift_password = ''
iam = ''

# Redshift table
# insert the redshift table to fit to the testing
table = ''

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

def table_exists(cur, table_exist_query):
    cur.execute(table_exist_query)
    if cur.fetchone()[0]:
        logging.info(f"The table '{table}' exists in Redshift.")
    else:
        logging.error(f"The table '{table}' does not exist in Redshift.")

def move_object(s3_client,bucket_name,source_file_key,destination_file_key):\
    # copy the object to destination location
    s3_client.copy_object(
    CopySource={'Bucket': bucket_name, 'Key': source_file_key},
    Bucket=bucket_name,
    Key=destination_file_key
    )

    # delete the object from the source location
    s3_client.delete_object(Bucket=bucket_name, Key=source_file_key)

    logging.info(f'S3 object moved from {source_file_key} to {destination_file_key}')

def count_row_table(cursor,query):
    cursor.execute(query)
    return int(cursor.fetchone()[0])

def row_copied_to_rs(post_count,pre_count,table_rs):
    rows_copied = post_count - pre_count
    logging.info(f'{rows_copied} rows copied to {table_rs}')
    return rows_copied

def lambda_handler(event, context):

    logging.info("Received event: " + json.dumps(event, indent=2))
    # Get the object from the event and show its content type
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    archive_file_key = file_key.replace('ingestion','archive')

    # 0. connecting to AWS S3 
    aws_access_key_id, aws_secret_access_key = get_aws_credentials(aws_credentials_path)

    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, 
                      aws_secret_access_key=aws_secret_access_key, 
                      region_name=region_name)

    # 1. Check if the bucket and csv file exists
    bucket_exist(s3_client,bucket_name)
    file_exist(s3_client, bucket_name, file_key)

    # 2. Connect to redshift assuming that the redshift

    conn = redshift_connector.connect(
        host=redshift_host,
        port=redshift_port,
        database=redshift_database,
        user=redshift_user,
        password=redshift_password
    )

    # 3. Create a cursor
    cur = conn.cursor()
    
    # prepare the queries
    copy_query = f"""
    COPY {table}
    FROM 's3://{bucket_name}/{file_key}'
    FORMAT AS CSV
    IGNOREHEADER 1
    delimiter ','
    access_key_id '{aws_access_key_id}'
    secret_access_key '{aws_secret_access_key}';
    """

    table_exist_query = f"""
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = '{table}'
    );
    """

    table_count_query = f"""
    SELECT count(1)
    FROM {table}
    ;
    """

    try:
        table_exists(cur, table_exist_query)

        pre_row_count = count_row_table(cur,table_count_query)
        
        # 4. Copy query with credentials
        cur.execute(copy_query)
        # 5. Commit the transaction
        conn.commit()
        post_row_count = count_row_table(cur,table_count_query)

        # 6. Get the row copied to redshift for checking purposes
        row_copied_to_rs(post_row_count,pre_row_count,table)
        # 7. Move the CSV file from demo/ingestion to demo/archives to prevent double extraction, and keep the raw file in archive

        # 8. Move the object to archive
        move_object(s3_client,bucket_name, file_key, archive_file_key)

        logging.info("Data loaded from S3 to Redshift successfully.")
    except Exception as e:
        logging.error("Error: Failed to copy data from S3 to Redshift.")
        logging.error(e)
    finally:
        # 9. Close the cursor and connection
        cur.close()
        conn.close()


# uncomment this section for exact lambda usage on AWS
if __name__ == '__main__':
    # mock a fake event to lambda_handler
    # 0. lambda listen to S3 event
    file = open('fake_event.json')
    fake_event = json.load(file)
    logging.info(fake_event)
    lambda_handler(fake_event, 'context')