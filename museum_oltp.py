import boto3
import datetime
import pandas as pd
import re
import json
from io import StringIO
from sqlalchemy import create_engine

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

with DAG(
    dag_id = 'museum_oltp',
    start_date = datetime.datetime(2022, 6, 23),
    schedule_interval='0 1 * * 0',
    dagrun_timeout = datetime.timedelta(minutes=60)
) as dag:

    create_tables = PostgresOperator(
        task_id='create_tables',
        postgres_conn_id='museum_oltp',
        sql=[
        """DROP TABLE IF EXISTS main_table CASCADE;
        CREATE TABLE main_table (
            id TEXT PRIMARY KEY,
            unitcode TEXT NOT NULL,
            type TEXT NOT NULL,
            url TEXT NOT NULL,
            hash TEXT NOT NULL,
            docsignature TEXT NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            lasttimeupdated BIGINT NOT NULL
        );

        DROP TABLE IF EXISTS item_description CASCADE;
        CREATE TABLE IF NOT EXISTS item_description (
            identifier TEXT PRIMARY KEY,
            id TEXT REFERENCES main_table,
            credit_line TEXT,
            data_source TEXT NOT NULL,
            restriction_rights TEXT NOT NULL
        );

        DROP TABLE IF EXISTS obj_type CASCADE;
        CREATE TABLE IF NOT EXISTS obj_type (
            obj_id BIGINT PRIMARY KEY,
            identifier TEXT REFERENCES item_description,
            object_description_type TEXT,
        object_description TEXT NOT NULL
        );

        DROP TABLE IF EXISTS physical_description CASCADE;
        CREATE TABLE IF NOT EXISTS physical_description (
            pd_id BIGINT PRIMARY KEY,
            identifier TEXT REFERENCES item_description,
            description_type TEXT NOT NULL,
            description TEXT NOT NULL
        );

        DROP TABLE IF EXISTS history CASCADE;
        CREATE TABLE IF NOT EXISTS history (
            history_id BIGINT PRIMARY KEY,
            identifier TEXT REFERENCES item_description,
            type TEXT NOT NULL,
            description TEXT NOT NULL
        );

        DROP TABLE IF EXISTS media CASCADE;
        CREATE TABLE IF NOT EXISTS media (
            media_id BIGINT PRIMARY KEY,
            id TEXT REFERENCES main_table,
            type TEXT NOT NULL,
            guid TEXT NOT NULL,
            idsid TEXT NOT NULL,
            thumbnail TEXT NOT NULL,
            content TEXT NOT NULL
        );

        DROP TABLE IF EXISTS media_resource CASCADE;
        CREATE TABLE IF NOT EXISTS media_resource (
            resource_id BIGINT PRIMARY KEY,
            media_id BIGINT REFERENCES media,
            label TEXT NOT NULL,
            width INTEGER NOT NULL,
            height INTEGER NOT NULL,
            url TEXT NOT NULL
        );

        DROP TABLE IF EXISTS origin CASCADE;
        CREATE TABLE IF NOT EXISTS origin (
            id TEXT REFERENCES main_table,
            location_type TEXT NOT NULL,
            location TEXT NOT NULL
        );"""]
        )
    
    @task
    def museum_oltp():
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
        
        ## Prepare the tables
        # main_table
        main_table_columns = ['id', 'unitCode', 'type', 'url', 'hash',
                              'docSignature', 'timestamp', 'lastTimeUpdated']
        main_table = df_nasm.drop(df_nasm.columns.difference(main_table_columns), axis=1)
        main_table.rename(columns = {'unitCode':'unitcode', 'docSignature':'docsignature',
                                     'lastTimeUpdated':'lasttimeupdated'},
                          inplace = True)

        # item_description
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
        item_description = pd.concat([df_nasm, credit_line, data_source, obj_right, identifier],
                                     axis=1)
        item_description_columns = ['identifier', 'id', 'credit_line',
                                    'data_source', 'restriction_rights']
        item_description = (item_description.drop(item_description.columns.
                                                  difference(item_description_columns),
                                                  axis=1))
        item_description['credit_line'] = (item_description['credit_line'].
                                           map(lambda x: ' ' if pd.isna(x) else x))

        # obj_type
        df_notes = df_ft['notes'].explode()
        df_notes = pd.concat([df_notes, identifier], axis=1)
        df_notes = df_notes.reset_index(drop=True)
        obj_type = pd.json_normalize(df_notes['notes'])
        obj_type.rename(columns = {'label':'object_description_type',
                                   'content':'object_description'}, inplace = True)
        obj_types = pd.concat([df_notes, obj_type], axis=1)
        obj_types = obj_types.reset_index()
        obj_types.rename(columns = {'index':'obj_id'}, inplace = True)
        obj_type_column = ['obj_id', 'identifier', 'object_description_type', 'object_description']
        obj_type_final = obj_types.drop(obj_types.columns.difference(obj_type_column),
                                        axis=1)
        obj_type_final = obj_type_final.dropna()
        
        # physical_description
        df_phys = df_ft['physicalDescription'].explode()
        df_phys = pd.concat([df_phys, identifier], axis=1)
        df_phys = df_phys.reset_index(drop=True)
        phys = pd.json_normalize(df_ft['physicalDescription'].explode())
        phys.rename(columns = {'label':'description_type', 'content':'description'},
                    inplace = True)
        phys_descs = pd.concat([df_phys, phys], axis=1)
        phys_desc = phys_descs.reset_index()
        phys_desc.rename(columns = {'index':'pd_id'}, inplace = True)
        phys_desc_col = ['pd_id', 'identifier', 'description_type', 'description']
        phys_desc = phys_desc.drop(phys_desc.columns.difference(phys_desc_col), axis=1)

        # history
        df_name = df_ft['name'].explode()
        df_name = pd.concat([df_name, identifier], axis=1)
        df_name = df_name.reset_index(drop=True)
        names = pd.json_normalize(df_ft['name'].explode())
        names.rename(columns = {'label':'type', 'content':'description'}, inplace = True)
        names_1 = pd.concat([df_name, names], axis=1)
        names_1 = names_1.reset_index()
        names_1.rename(columns = {'index':'history_id'}, inplace = True)
        names_1_col = ['history_id', 'identifier', 'type', 'description']
        names_1 = names_1.drop(names_1.columns.difference(names_1_col), axis=1)
        names_1.dropna(inplace=True)
        
        # media
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
        
        # media_resource
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
        
        # origin
        df_is = pd.json_normalize(dfc['indexedStructured'], max_level=0)
        geoloc = pd.json_normalize(pd.json_normalize(df_is['geoLocation'])[0])
        geoloc.rename(columns = {'L2.type':'location_type', 'L2.content':'location'}, inplace = True)
        loc = pd.concat([geoloc, main_table], axis=1)
        loc_col = ['id', 'location_type', 'location']
        location = loc.drop(loc.columns.difference(loc_col), axis=1)
        location.dropna(inplace=True)

        ## Setup hooks and connection objects
#         oltp_hook = PostgresHook(postgres_conn_id='museum_oltp')
#         oltp_conn = oltp_hook.get_conn()
        conn = create_engine('postgresql://nasm_user:nasm_museum@sampledb.c9ujnzf6jgs2.'
                             'us-east-1.rds.amazonaws.com:5432/museum_nasm')
        
        ## Replace existing tables
        ## Write dataframes to Postgres DB tables
        main_table.to_sql('main_table', con=conn, if_exists='append', index=False)
        item_description.to_sql('item_description', con=conn, if_exists='append', index=False)
        obj_type_final.to_sql('obj_type', con=conn, if_exists='append', index=False)
        phys_desc.to_sql('physical_description', con=conn, if_exists='append', index=False)
        names_1.to_sql('history', con=conn, if_exists='append', index=False)
        medias_2.to_sql('media', con=conn, if_exists='append', index=False)
        resources_2.to_sql('media_resource', con=conn, if_exists='append', index=False)
        location.to_sql('origin', con=conn, if_exists='append', index=False)
        
    create_tables >> museum_oltp()