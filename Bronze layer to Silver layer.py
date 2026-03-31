import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

import json
import boto3

client=boto3.client("s3")
bucketname="s3-client-files"
sourcefolder="bronze layer/"
destinationfolder="silver layer/"

def list_file():
    try:
        response=client.list_objects_v2(Bucket=bucketname,Prefix=sourcefolder)

        files=[]

        if "Contents" in response:
            for obj in response['Contents']:
                key=obj['Key']

                if key!=sourcefolder:
                    files.append(key)
        return files

    except Exception as e :
        print(e)

def copy_file(files):
    try:
        copy_file=[]

        for file in files:
            filename=file.split("/")[-1]

            destination_key=destinationfolder+filename

            copy_source={
                "Bucket":bucketname ,
                "Key":file
            }

            response=client.copy_object(
                Bucket=bucketname,
                CopySource=copy_source,
                Key=destination_key
            )
            copy_file.append(file)
        return copy_file

    except Exception as e:
        print(e)

def file_delete(files):
    try:
        for file_key in files:
            if file_key!=sourcefolder:
                client.delete_object(Bucket=bucketname,
                Key=file_key)
    except Exception as e:
        print(e)
def main():
    files=list_file()
    copy_file(files)
    file_delete(files)

if __name__=="main":
    main()

job.commit()