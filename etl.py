import requests
import pandas as pd
from pandas import concat
import boto3
import io
import datetime
from dotenv import dotenv_values
from util import load_data,get_database_conn
#from boto.s3.connection import S3Connection
#from boto.s3.key import Key

config = dict (dotenv_values(".env"))

Access_key = config.get('Access_key')
Secret_key = config.get('Secret_key')

def extract():
    countries =['Canada','UK','America']
    final_job_df = pd.DataFrame()
    
    for country in countries:
        url = "https://jsearch.p.rapidapi.com/search"
        querystring = {"query":f"Data Engineer and Data Analysts Jobs in {country}","page":"1","num_pages":"1"}
        headers = {
        "X-RapidAPI-Key": "461d132365msh10a556a9aa20dcep1aa381jsn705ba5796aa9",
        "X-RapidAPI-Host": "jsearch.p.rapidapi.com"
        }
        response = requests.request("GET", url, headers=headers, params=querystring)
        print(response.json())
        job_list = pd.DataFrame(response.json()['data'])
        #job_list = job_list[['employer_website','job_id', 'job_employment_type', 'job_title','job_apply_link','job_description', 'job_city', 'job_country','job_posted_at_timestamp', 'employer_company_type']]
        print(job_list.head(5))
        final_job_df = pd.concat([final_job_df,job_list])

    final_job_df.to_json('job_list.json',orient='records')

#connecting to S3 Bucket to upload JSON document
#aws_connection = S3Connection (Access_key,Secret_key)
#bucket = aws_connection.get_bucket('jobscapstone')
#k = Key(bucket)
#k.key = 'raw/job_list_raw.json'
#k.set_contents_from_filename('job_list.json')

def transform():
    bucket,filename = "jobscapstone" , "raw/jobstest.json"
    required_cols = ['employer_website','job_id', 'job_employment_type', 'job_title','job_apply_link','job_description' ,'job_city', 'job_country','job_posted_at_timestamp', 'employer_company_type']

    s3_client = boto3.client('s3',
        aws_access_key_id=Access_key,
        aws_secret_access_key=Secret_key,
    )

    with open("job_list.json","rb") as f:
        data = f.read()
    # define the settings of the Bucket
    response = s3_client.put_object(
        Body= data,
        Bucket = "jobscapstone",
        Key=filename
    )

    obj = s3_client.get_object(Bucket = bucket, Key = filename)
    data = obj['Body'].read()
    print(type(data))
    df1 = pd.read_json(io.BytesIO(data))
    df1 = df1.replace('\n',' ', regex= True)
    df1 = df1[required_cols]
    df1.to_csv('jobs.csv',index=False)
    print(df1[required_cols])
    s3_client.delete_object(Bucket = bucket, Key = filename)


    with open("jobs.csv","rb") as f:
        data = f.read()
    # define the settings of the Bucket
    response = s3_client.put_object(
        Body= data,
        Bucket = "jobscapstone",
        Key=f"transformed/output_{str(datetime.date.today())}.csv"
    )
    
#load_data('data_engineer_jobs')




