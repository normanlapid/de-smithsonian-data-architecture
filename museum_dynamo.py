import boto3
import datetime
import pandas as pd
import re
import json
from io import StringIO
from sqlalchemy import create_engine
from decimal import Decimal 

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

with DAG(
    dag_id = 'museum_dynamo',
    start_date = datetime.datetime(2022, 6, 23),
    schedule_interval='0 3 * * 0',
    dagrun_timeout = datetime.timedelta(minutes=60)
) as dag:

    delete_table = BashOperator(
        task_id='delete_table',
        bash_command='aws dynamodb delete-table --table-name museum')
    
    pause = BashOperator(
        task_id='pause',
        bash_command='sleep 5')

    create_table = BashOperator(
        task_id='create_table',
        bash_command='aws dynamodb create-table \
                        --table-name museum \
                        --attribute-definitions \
                            AttributeName=identifier,AttributeType=S \
                            AttributeName=media_url,AttributeType=S \
                        --key-schema \
                            AttributeName=identifier,KeyType=HASH \
                            AttributeName=media_url,KeyType=RANGE \
                        --billing-mode PAY_PER_REQUEST')

    @task
    def museum_dynamo():
        ## Reading the source files from the Smithsonian Open Data
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

        my_bucket = s3.Bucket(bucket_landing)
        keys = []
        for my_bucket_object in my_bucket.objects.all():
            keys.append(my_bucket_object.key)
        r = re.compile(".*.txt")
        newlist = list(filter(r.match, keys))
        txt_str = ''
        for filename in newlist:
            txt_obj = client.get_object(Bucket=root_bucket, Key = filename)
            txt = txt_obj['Body'].read().decode('utf-8')
            txt_str = txt_str + txt
        jsonData = json.loads(json.dumps(txt_str))
        df_nasm = pd.read_json(StringIO(jsonData), lines=True)
        
        main_table_columns = ['id', 'unitCode', 'type', 'url', 'hash',
                              'docSignature', 'lastTimeUpdated', 'title']
        main_table = df_nasm.drop(df_nasm.columns.difference(main_table_columns),
                                  axis=1)
        main_table.rename(columns = {'unitCode':'unitcode',
                                     'docSignature':'docsignature',
                                     'lastTimeUpdated':'lasttimeupdated'},
                          inplace = True)
        content = df_nasm['content']
        df_content = pd.json_normalize(content, max_level=0)
        df_ft = pd.json_normalize(df_content['freetext'], max_level=0)
        credit_line = pd.json_normalize(df_ft['creditLine'].explode())
        credit_line.rename(columns = {'content':'credit_line'}, inplace = True)
        data_source = pd.json_normalize(df_ft['dataSource'].explode())
        data_source.rename(columns = {'content':'data_source'}, inplace = True)
        obj_right = pd.json_normalize(df_ft['objectRights'].explode())
        obj_right.rename(columns = {'content':'restriction_rights'}, inplace = True)
        identifier = pd.json_normalize(df_ft['identifier'].explode())
        identifier.rename(columns = {'content':'identifier'}, inplace = True)
        item_description = pd.concat([df_nasm, credit_line, data_source,
                                      obj_right, identifier], axis=1)
        item_description_columns = ['identifier', 'id', 'credit_line',
                                    'data_source', 'restriction_rights']
        item_description = item_description.drop(item_description.columns.
                                                 difference(item_description_columns),
                                                 axis=1)
        item_description['credit_line'] = (item_description['credit_line'].
                                           map(lambda x: ' ' if pd.isna(x) else x))
        df_NASM = pd.concat([main_table, item_description], axis=1)
        df_NASM['credit_line'] = df_NASM['credit_line'].str.replace("'", '')
        df_NASM['title'] = df_NASM['title'].str.replace("'", '')
        df_notes = df_ft['notes'].explode()
        df_notes = pd.concat([df_notes, identifier], axis=1)
        df_notes = df_notes.reset_index(drop=True)
        obj_type = pd.json_normalize(df_notes['notes'])
        obj_type.rename(columns = {'label':'object_description_type',
                                   'content':'object_description'}, inplace=True)
        obj_types = pd.concat([df_notes, obj_type], axis=1)
        obj_types = obj_types.reset_index()
        obj_types.rename(columns = {'index':'obj_id'}, inplace=True)
        obj_type_column = ['obj_id', 'identifier', 'object_description_type',
                           'object_description']
        obj_type_final = obj_types.drop(obj_types.columns.difference(obj_type_column),
                                        axis=1)
        obj_type_final = obj_type_final.dropna()
        df_NASM_1 = pd.merge(df_NASM, obj_type_final, how="inner", on=["identifier"])
        df_phys = df_ft['physicalDescription'].explode()
        df_phys = pd.concat([df_phys, identifier], axis=1)
        df_phys = df_phys.reset_index(drop=True)
        phys = pd.json_normalize(df_ft['physicalDescription'].explode())
        phys.rename(columns = {'label':'description_type', 'content':'description'},
                    inplace=True)
        phys_descs = pd.concat([df_phys, phys], axis=1)
        phys_desc = phys_descs.reset_index()
        phys_desc.rename(columns = {'index':'pd_id'}, inplace = True)
        phys_desc_col = ['pd_id', 'identifier', 'description_type', 'description']
        phys_desc = phys_desc.drop(phys_desc.columns.difference(phys_desc_col), axis=1)
        df_NASM_2 = pd.merge(df_NASM_1, phys_desc, how="inner", on=["identifier"])
        df_NASM_2 = df_NASM_2.drop_duplicates(subset=['pd_id'])
        df_NASM_2 = df_NASM_2.reset_index(drop=True)
        df_name = df_ft['name'].explode()
        df_name = pd.concat([df_name, identifier], axis=1)
        df_name = df_name.reset_index(drop=True)
        names = pd.json_normalize(df_ft['name'].explode())
        names.rename(columns = {'label':'type', 'content':'description'}, inplace=True)
        names_1 = pd.concat([df_name, names], axis=1)
        names_1 = names_1.reset_index()
        names_1.rename(columns = {'index':'history_id'}, inplace = True)
        names_1_col = ['history_id', 'identifier', 'type', 'description']
        names_1 = names_1.drop(names_1.columns.difference(names_1_col), axis=1)
        names_1.dropna(inplace=True)
        df_NASM_3 = pd.merge(df_NASM_2, names_1, how="inner", on=["identifier"])
        df_NASM_3 = df_NASM_3.reset_index(drop=True)
        df_NASM_3 = df_NASM_3.drop(columns=['object_description', 'description_x'])
        df_NASM_3['description_y'] = df_NASM_3['description_y'].str.replace("'", '')
        df_NASM_3 = df_NASM_3.loc[:,~df_NASM_3.columns.duplicated()].copy()
        
        dfc = df_content.copy()
        df_dnr = pd.json_normalize(dfc['descriptiveNonRepeating'], max_level=0)
        online_media = pd.json_normalize(df_dnr['online_media'])
        media = online_media['media'].explode()
        media = pd.merge(media, main_table, left_index=True, right_index=True)
        media = media.reset_index(drop=True)
        medias = pd.json_normalize(online_media['media'].explode())
        medias_1 = pd.concat([media, medias], axis=1)
        medias_1 = medias_1.reset_index()
        medias_1.rename(columns = {'index':'media_id', 'idsId':'idsid'}, inplace = True)
        medias_1_col = ['media_id', 'id', 'type', 'guid', 'idsid', 'thumbnail', 'content']
        medias_2 = medias_1.drop(medias_1.columns.difference(medias_1_col), axis=1)
        medias_2.dropna(inplace=True)
        medias_3 = medias_1.rename(columns = {'url':'url_x'})
        resource = medias_3['resources'].explode()
        resource = pd.merge(resource, medias_3, left_index=True, right_index=True)
        resource = resource.reset_index(drop=True)
        resources = pd.json_normalize(medias['resources'].explode())
        resources_1 = pd.concat([resource, resources], axis=1)
        resources_1 = resources_1.reset_index()
        resources_1.rename(columns = {'index':'resource_id', 'idsId':'idsid'}, inplace = True)
        resources_1_col = ['resource_id', 'media_id', 'label', 'width', 'height', 'url']
        resources_2 = resources_1.drop(resources_1.columns.difference(resources_1_col), axis=1)
        resources_2.dropna(inplace=True)
        df_media = pd.merge(medias_2, resources_2, how="inner", on=["media_id"])
        df_media = df_media.reset_index(drop=True)
        df_media = df_media.reset_index()
        df_media.rename(columns = {'index':'media_id', 'media_id':'resources_id',
                                   'url':'media_url'}, inplace=True)
        df_media = df_media.drop(columns=['type', 'resource_id'])

        df_complete = df_media.merge(df_NASM_3, on='id', how='inner')
        df_complete = df_complete.drop(columns=['media_id', 'resources_id'])
        df_complete = df_complete.drop_duplicates(subset=['media_url'])
        
        nasm_dict = df_complete.to_dict(orient='records')
        
        dynamodb = boto3.resource('dynamodb')
        museum = dynamodb.Table('museum')
        client = boto3.client('dynamodb')
        
        for row in nasm_dict:
            client.execute_statement(
            Statement=f"""INSERT INTO museum VALUE {str(row)}"""
            )


    delete_table >> pause >> create_table >> museum_dynamo()