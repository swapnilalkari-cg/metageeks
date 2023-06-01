import sys
import json
import traceback
import boto3
import pymysql
import datetime
import ast
import requests
import urllib.request
import boto3
import json
import time
import uuid
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import date_format
from datetime import datetime
from pyspark.sql.functions import current_timestamp

session = boto3.session.Session()
S3Client = boto3.client('s3')
args = getResolvedOptions(sys.argv, ["JOB_NAME","bucket_name","object_name","data_provider","fileId","sequenceNumber","fileInstanceId"])
sc = SparkContext()
glueContext = GlueContext(sc)
# spark = glueContext.spark_session
spark = glueContext.spark_session.builder \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").getOrCreate()
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

x  = datetime.now()
yy = x.strftime("%Y")
mm = x.strftime("%m")
dd = x.strftime("%d")
date = (f"{yy}-{mm}-{dd}")
time = str(datetime.now()).split(" ")[1].split(":")[0]

file_name = args["object_name"]
fileName = file_name.rstrip(".csv")

response_API = requests.get(f'http://dataprovider5-env.eba-bjm63ugz.us-east-2.elasticbeanstalk.com/api/getFileSchemaDetails/{args["fileId"]}')
data = response_API.text
query_results = json.loads(data)

source = schema = {}
header = delimiter = path = bucket = ""
                            
source = query_results["fileDataSrcTgtLoc"]
schema = query_results["fileDataSrcSchema"]

bucket = source['s3Bucket']
delimiter = schema['fieldDelimiter']
header = schema['isHeaderRowPresent']
fileId = query_results["fileDataSrcId"]

glue = boto3.client('glue')
client_kinesis = boto3.client('kinesis')
   

def getS3Filename(fileName):
    get_last_modified = lambda obj: int(obj['LastModified'].strftime('%s'))
    s3 = boto3.client('s3')
    bucket = 'aws-glue-parquet-target'
    date = datetime.now().strftime("%Y-%m-%d")
    prefix = '{0}{1}/Date={2}/'.format(args['data_provider'],fileName,date)
    objs = s3.list_objects_v2(Bucket=bucket,Prefix = prefix)['Contents']
    last_added = [obj['Key'] for obj in sorted(objs, key=get_last_modified,reverse=True)][0]
    print(last_added[len(prefix):])
    return last_added[len(prefix):] 

def writeRowsfunc(fileName,datasetName):
    date = datetime.now().strftime("%Y-%m-%d")
    DF_write = spark.read.parquet('s3a://aws-glue-parquet-target/{0}{1}/Date={2}/{3}'.format(args['data_provider'],fileName,date,datasetName))
    writtenRows = DF_write.count()
    return writtenRows
    
try:
    type_job = 'ingestion.job.started'
    content = '''{
         "eventType": "%s",
         "eventSource": "%s",
         "eventTime": "%s",
         "eventData":{
          "fileID" : "%s", "fileInstanceID" : "%s", "datasetName" : "%s"
          },
         "eventSequence" : "%s"
      }''' % (type_job, args["JOB_NAME"], datetime.now().strftime('%Y-%m-%d %H:%M:%S'), fileId, args["fileInstanceId"], args["object_name"], args["sequenceNumber"])

    response = client_kinesis.put_record(StreamName = 'iaas_kinesis_ingestion_stream', Data=content, PartitionKey = str(uuid.uuid4()))
    print(response)


    try:
        df = spark.read.format('csv').option('header', header).option('inferSchema', 'true').option("delimiter", delimiter).load("s3a://{0}/{1}{2}/{3}".format(args["bucket_name"], args["data_provider"], date, args["object_name"]))
        rows_read = df.count()
        if rows_read == 0:
            raise Exception("No data found in the input file")

        df1 = df.withColumn("CurrentDate", current_timestamp())
        df2 = df1.withColumn("Date", date_format("CurrentDate", "yyyy-MM-dd"))
        df2.write.format('delta').partitionBy("Date").mode("append").save(f"s3a://aws-glue-parquet-target/{args['data_provider']}{fileName}")
        datasetName = getS3Filename(fileName)
        rowsRead = df2.count()
        print("rowsread = ", rowsRead)
        rowsWritten = writeRowsfunc(fileName, datasetName)
        type_job = 'ingestion.job.succeeded'
        content = '''{
             "eventType": "%s",
             "eventSource": "%s",
             "eventTime": "%s",
             "eventData":{
              "fileID" : "%s", "fileInstanceID" : "%s", "datasetName" : "%s", "numsOfRowsRead" : "%s", "numsOfRowsWritten":"%s"
              },
             "eventSequence" : "%s"
          }''' % (type_job, args["object_name"], datetime.now().strftime('%Y-%m-%d %H:%M:%S'), fileId, args["fileInstanceId"], datasetName, rowsRead, rowsWritten, args["sequenceNumber"])


    except Exception as e:
        type_job = 'ingestion.job.fileNotFound'
        datasetName = ''
        errorMessage = 'File Not Found!' 
        error_message = traceback.format_exc()
        print(error_message)
        m = error_message.split()
        print(m)
        str1 = ''
        for i in m:
            str1 = str1 +' '+ i
        print(str1)
        error_msg = str1.replace('"',"'")
        content='''{
             "eventType": "%s",
             "eventSource": "%s",
             "eventTime": "%s",
             "eventData":{
              "fileID" : "%s", "fileInstanceID" : "%s", "datasetName" : "%s", "errorMessage" : "%s"
              },
             "eventSequence" : "%s"
          }'''%(type_job,args["object_name"],datetime.now().strftime('%Y-%m-%d %H:%M:%S'),fileId,args["fileInstanceId"],datasetName,error_msg,args["sequenceNumber"])

    response = client_kinesis.put_record(StreamName = 'iaas_kinesis_ingestion_stream', Data=content, PartitionKey = str(uuid.uuid4()))
    print(response)

    client = boto3.client('s3')
    event_path=f'event-payloads/ingestion-events/{fileName}_{datetime.now()}.json'
    client.put_object(Bucket='file-data-source-bucket', Key=event_path, Body=content)

    url = "http://eventservice-env.eba-2bajizrw.us-east-2.elasticbeanstalk.com/api/addNewEvent"

    headers = {
        "Content-Type": "application/json",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept": "*/*"
    }

    content_json = json.loads(content)
    response = requests.post(url, json=content_json, headers=headers)

    print(response.text)
    
except Exception as e:
    type_job = 'ingestion.job.failed'
    datasetName = '' 
    error_message = traceback.format_exc()
    print(error_message)
    m = error_message.split()
    print(m)
    str1 = ''
    for i in m:
        str1 = str1 +' '+ i
    print(str1)
    error_msg = str1.replace('"',"'")
    content='''{
         "eventType": "%s",
         "eventSource": "%s",
         "eventTime": "%s",
         "eventData":{
          "fileID" : "%s", "fileInstanceID" : "%s", "datasetName" : "%s", "errorMessage" : "%s"
          },
         "eventSequence" : "%s"
      }'''%(type_job,args["object_name"],datetime.now().strftime('%Y-%m-%d %H:%M:%S'),fileId,args["fileInstanceId"],datasetName,error_msg,args["sequenceNumber"])
    
    response = client_kinesis.put_record(StreamName = 'iaas_kinesis_ingestion_stream', Data=content, PartitionKey = str(uuid.uuid4()))
    print(response)

job.commit()