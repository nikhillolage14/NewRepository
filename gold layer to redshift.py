import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import json
import psycopg2


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


query = """
COPY dev.public.customer FROM 's3://sqs-file-upload-aws/gold layer/customer_2026-03-19 (1).csv' 
IAM_ROLE 'arn:aws:iam::877231425354:role/Aws-redshift-Admin-Acess'
 FORMAT AS CSV DELIMITER ',' QUOTE '"' IGNOREHEADER 1 REGION AS 'ap-south-1'
"""

def connection_redshit():
    try:
        conn = psycopg2.connect(
            host="redshift-cluster-1.crq26asi4crq.ap-south-1.redshift.amazonaws.com",
            user="awsuser",
            port="5439",
            database="dev",
            password="Admin#123"
        )
        return conn
    except Exception as e:
        print("Connection error:", e)

def dumping_process(conn):
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
    except Exception as e:
        print("Execution error:", e)
    finally:
        conn.close()

def main():
    conn = connection_redshit()
    if conn:
        dumping_process(conn)

if __name__="__main__":
    main()
job.commit()