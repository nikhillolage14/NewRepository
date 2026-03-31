import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import json
import boto3


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

client=boto3.client("s3")
bucketname="s3-client-files"
sourcefolder="landing-zone/"
dest_folder="bronze layer/"

def file_list():
    try:
        response=client.list_objects_v2(Bucket=bucketname,
        Prefix=sourcefolder)

        files=[]

        if 'Contents' in response:
            for obj in response["Contents"]:
                key=obj['Key']

                if key !=sourcefolder:
                    files.append(key)
        return files

    except Exception as e:
        print(e)
        
def copy_file(files):
    try:
        copy_files=[]

        for file in files:
            filename=file.split("/")[-1]

            destination_key=dest_folder+filename

            copy_source={
                "Bucket":bucketname,
                "Key":file
            }

            response=client.copy_object(Bucket=bucketname,
            CopySource=copy_source,
            Key=destination_key)
        return copy_files

    except Exception as e:
        print(e)

def delete_file(files):
    try:

        for file_key in files:
            if file_key != sourcefolder:
                client.delete_object(Bucket=bucketname,
                Key=file_key)
    except Exception as e:
        print(e)

def main():
    files=file_list()
    copy_file(files)
    delete_file(files)

if __name__=="__main__":
    main()

job.commit()


