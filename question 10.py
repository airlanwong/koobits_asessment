# Q10)  You need to analyze a large dataset stored in an S3 bucket using Python and AWS Glue. 
# Write a Python script to run an AWS Glue job that transforms the data and makes it available for analysis.
##################################################################################################################################################################

# IMPORTANT: This is developed in local machine with docker pulling aws-glue-lib. aws-glue-lib is not available in pip install.
# Instruction to enable the running of local glue development is found in how_to_run_glue_locally.txt

# Assumption: 
# - The dataset is only one file and it is in parquet format
# - This is a batch ETL not streaming (real-time) ETL
# - IAM role permission is set up

##################################################################################################################################################################
# This python (3.11.4) is developed with aws-glue-lib.4.0.0.
# The data is from xAPI-Edu-Data.csv in the repo.

# Step 0. set up the s3 and glue_client
# Step 1. start the spark session since to prepare for large data processing
# Step 2. check if the file exist. use spark to read the parquet file
# Step 3. transform the data to make it available for analysis (two option: spark transformation or spark SQL)
# Step 3.1 Set up a sql query to prepare the data for partitioning and analysis
# Step 4. load the transformed data to S3
# Step 5. check if the destination file exist
# Step 6. close the spark session
##################################################################################################################################################################

import logging
import os, boto3
from pyspark.context import SparkContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import configparser


logging.basicConfig(level = logging.INFO)

sql_table = 'source_table'

# 3.1. Set up a sql query to prepare the data for partitioning and analysis
# This query is based on principles of data warehouse and optimise partitioning for athena query to prepare for analysis. 
# fact_guid is used as unique primary key.
# gender is categorical encoded to be stored in a fact_table
# created_date_time and current_version columns are used as a way to manage slowly changing dimension
# year,month and day columns are used for partitioning

sql_query = f"""
    SELECT
        MD5(NVL(Relation,'') || '-' ||
        NVL(raisedhands,'') || '-' ||
        NVL(VisITedResources,'')
        ) as fact_guid, 
        Relation,
        raisedhands,
        case when gender = 'M' then 0
        when gender = 'F' then 1 end as gender,
        raisedhands + VisITedResources AS random_combined_column,
        'Townville Primary School' as source,
        current_date() as created_date_time,
        True as current_version,
        YEAR(current_date()) as year,
        MONTH(current_date()) as month,
        DAY(current_date()) as day
    FROM
        {sql_table}
"""

def get_aws_credentials(aws_credentials_path):
    path = os.path.expanduser(aws_credentials_path)
    config_path = config.read(path)
    if os.path.exists(config_path[0]):
        aws_access_key_id = config.get('default', 'aws_access_key_id')
        aws_secret_access_key = config.get('default', 'aws_secret_access_key')
        return aws_access_key_id, aws_secret_access_key
    else:
        logging.error(f'{config_path[0]} is not a aws config file')

def file_exist(s3, bucket_name, file_key):
    keys = s3.list_objects_v2(Bucket=bucket_name)['Contents']
    for key in keys:
        if key['Key'] == file_key:
            logging.info(f'{file_key} is found in {bucket_name}')

def glue_read_parquet(spark,s3_file_key):
    source_data = spark.read.parquet(s3_file_key, header=True, inferSchema=True)
    logging.info("Original Data:")
    source_data.show(5)
    return source_data

def transformation_source_data(data):
    # This is a example of a transformation. This example is to filter by students who raisedhands more than 60 times.
    transformed_data = data.select("gender", "GradeID", "raisedhands").filter(source_data["raisedhands"] > 60)
    logging.info(f"Transformed Data with student {filter_column} > 60:")
    transformed_data.show(5)
    return transformed_data

def transformation_source_data_via_sql(data,sql_query,table):
    data.createOrReplaceTempView(table)
    transformed_df = spark.sql(sql_query)
    logging.info("Query Results:")
    transformed_df.show()

config = configparser.RawConfigParser()
aws_credentials_path = '~/.aws/credentials'
aws_access_key_id, aws_secret_access_key = get_aws_credentials(aws_credentials_path)
# Insert the region_name and bucket_name for testing
region_name = ''
bucket_name = ''
prefix_key = r'demo/ingestion/year=2023/month=09/day=15'
destination_prefix_key = r'demo/post_ingestion/year=2023/month=09/day=15'
destination_file_key = f'{destination_prefix_key}/transformed_data.parquet'
file_key = fr'{prefix_key}/xAPI-Edu-Data.parquet'
filter_column = 'raisedhands'
s3_path = f's3://{bucket_name}/{file_key}'


if __name__ == '__main__':
    # 0. set up the s3 and glue_client
    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, 
                          aws_secret_access_key=aws_secret_access_key, 
                          region_name=region_name)

    glue_client = boto3.client('glue', aws_access_key_id=aws_access_key_id, 
                          aws_secret_access_key=aws_secret_access_key, 
                          region_name=region_name)
    try:

        # uncomment for actual glue job usage
        # glue_client.start_job_run(JobName = 'my_test_Job')

        # 1. start the spark session since to prepare for large data processing
        # uncomment for actual glue job usage
        # args = getResolvedOptions(sys.argv,['JOB_NAME'])
        sc = SparkContext()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session
        logger = glueContext.get_logger()
        job = Job(glueContext)

        # uncomment for actual glue job usage
        # job.init(args['JOB_NAME'], args)
        
        # 2. check if the file exist. use spark to read the parquet file
        file_exist(s3_client, bucket_name, file_key)
        # need to add in the check in glue_read_parquet 
        source_data = glue_read_parquet(spark,s3_path)

        # 3. transform the data to make it available for analysis (two option: spark transformation or spark SQL)

        # option 1. using spark transformation, usually better performance but harder to understand the transformation steps
        transformed_data = transformation_source_data(source_data)

        # option 2. using spark SQL transformation, it is easier but might be slow in performance
        transformed_data = transformation_source_data_via_sql(source_data,sql_query,sql_table)
        
        # 4. load the transformed data to S3
        # load the transformed data back to S3 in a seperate folder in post_ingestion
        transformed_data.write.parquet(f"s3://{destination_file_key}", mode="overwrite")

        # 5. check if the write-to-s3 file exist
        file_exist(s3_client, bucket_name, destination_file_key)
        # uncomment for actual glue job usage
        # job.commit()
    except Exception as e:
        logging.error("Error: Failed to copy data from S3 to Redshift.")
        logging.error(e)
    # Stop the SparkSession
    finally:
        # 6. close the spark session
        spark.stop()
