import boto3
import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator

with DAG(
    dag_id = 'museum_extract',
    start_date = datetime.datetime(2022, 6, 23),
    schedule_interval='0 0 * * 0',
    dagrun_timeout = datetime.timedelta(minutes=60)
) as dag:

    @task
    def museum_extract():
        root_bucket = 'smithsonian-nasm-landing'
        my_bucket = 's3://smithsonian-nasm-landing/metadata/'
        bucket_landing = 'smithsonian-nasm-landing'
        access_key = 'AKIAXEOCLRHQLPQEBTR6'
        secret_key = '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'
        
        client = boto3.client('s3',
                              aws_access_key_id = access_key,
                              aws_secret_access_key = secret_key,
                              region_name = 'us-east-1')

        resource = boto3.resource('s3',
                                  aws_access_key_id = access_key,
                                  aws_secret_access_key = secret_key,
                                  region_name = 'us-east-1')
        
        session = boto3.Session(aws_access_key_id=access_key, 
                                aws_secret_access_key=secret_key)        
        s3 = session.resource('s3')
        
        source_bucket_name = 'smithsonian-open-access'
        target_bucket_name = 'smithsonian-nasm-landing'
        source_bucket = s3.Bucket(source_bucket_name)
        target_bucket = s3.Bucket(target_bucket_name)

        source_folder = "metadata/edan/nasm"
        target_folder = "metadata"

        for obj in source_bucket.objects.filter(Prefix=source_folder):
            source_filename = (obj.key).split('/')[-1]
            folder_name = (obj.key).split('/')[-2]
            if source_filename != 'index.txt' and folder_name == 'nasm':
                copy_source = {
                    'Bucket': source_bucket_name,
                    'Key': obj.key
                }
                target_filename = "{}/{}".format(target_folder, source_filename)
                s3.meta.client.copy(copy_source, target_bucket_name, target_filename)
                
    museum_extract()