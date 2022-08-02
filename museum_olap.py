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
    dag_id = 'museum_olap',
    start_date = datetime.datetime(2022, 6, 23),
    schedule_interval='0 2 * * 0',
    dagrun_timeout = datetime.timedelta(minutes=60)
) as dag:
    
    create_tables = RedshiftSQLOperator(
        task_id='create_tables',
        redshift_conn_id='museum_olap',
        sql=[
        """CREATE TABLE IF NOT EXISTS dim_media (
            media_id INTEGER NOT NULL,
            thumbnail TEXT,
            idsID TEXT,
            guid TEXT,
            type TEXT
        );""",
            
        """CREATE TABLE IF NOT EXISTS dim_media_resource (
            resources_id INTEGER NOT NULL,
            media_id INTEGER,
            width FLOAT,
            height FLOAT,
            label TEXT,
            url TEXT,
            dimensions TEXT
        );""",

        """CREATE TABLE IF NOT EXISTS dim_physical_desc (
            phys_desc_id INTEGER NOT NULL,
            label TEXT,
            content TEXT
        );""",

        """CREATE TABLE IF NOT EXISTS dim_manufacturer (
            manufacturer_id INTEGER NOT NULL,
            label TEXT,
            content TEXT
        );""",

        """CREATE TABLE IF NOT EXISTS dim_setname (
            setname_id INTEGER NOT NULL,
            label TEXT,
            content TEXT
        );""",

        """CREATE TABLE IF NOT EXISTS dim_screditline(
            creditline_id INTEGER NOT NULL,
            label TEXT,
            content TEXT
        );""",

        """CREATE TABLE IF NOT EXISTS dim_notes(
            notes_id INTEGER NOT NULL,
            label TEXT,
            content TEXT
        );""",

        """CREATE TABLE IF NOT EXISTS dim_invty_desc (
            invty_desc_id INTEGER NOT NULL,
            title TEXT,
            object_type TEXT,
            place TEXT,
            usage_flag TEXT
        );""",

        """CREATE TABLE IF NOT EXISTS fact_artifact(
            id TEXT,
            main_id INTEGER,
            url TEXT,
            hash TEXT,
            docsignature TEXT,
            timestamp TIMESTAMP,
            lastTimeUpdated FLOAT,
            title TEXT,
            media_id INTEGER,
            resources_id INTEGER,
            phys_desc_id INTEGER,
            manufacturer_id INTEGER,
            setname_id INTEGER,
            creditline_id INTEGER,
            notes_id INTEGER,
            invty_desc_id INTEGER
        )
        
        DISTSTYLE KEY DISTKEY(id)
        SORTKEY (main_id, invty_desc_id, notes_id);"""]
        )
    
    @task
    def museum_olap():
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
        rs = df_nasm.copy()
        main = rs[['id', 'content']]
        main = pd.json_normalize(main['content'], max_level=0)
        main = pd.concat([main, rs], axis=1)
        main.reset_index(inplace=True)
        main.rename(columns={'index':'main_idx'}, inplace=True)
        
        # media
        df_dnr = pd.json_normalize(main['descriptiveNonRepeating'], max_level=0)
        online_media = pd.DataFrame(df_dnr['online_media'])
        online_media = pd.json_normalize(online_media['online_media'])
        online_media = pd.DataFrame(online_media['media'].explode())
        online_media.reset_index(inplace=True)
        online_media.rename(columns={'index':'main_id'}, inplace=True)
    
        media = pd.concat([online_media, pd.json_normalize(online_media['media'])],
                          axis=1).reset_index()
        media.rename(columns={'index':'media_id'}, inplace=True)
        media = media[['media_id', 'main_id', 'thumbnail', 'idsId', 'guid', 'type']]
        media = media.drop_duplicates()
        media = media[media.notna().all(axis=1)]
        
        # media_resource (res)
        online_media = pd.json_normalize(df_dnr['online_media'])
        med = pd.json_normalize(online_media['media'].explode())
        res = pd.concat([media, pd.json_normalize(med['resources'].explode())],
                        axis=1).reset_index()
        res.rename(columns={'index':'resources_id'}, inplace=True)
        res = res[['resources_id', 'media_id', 'main_id', 'width',
                   'height', 'label', 'url', 'dimensions']]
        res = res.drop_duplicates()
        res = res[res.notna().all(axis=1)]
        
        # phys_desc
        df_ft = pd.json_normalize(main['freetext'], max_level=0)
        pdes = pd.DataFrame(df_ft['physicalDescription'])
        pdes = pd.DataFrame(pdes['physicalDescription'].explode())
        pdes.reset_index(inplace=True)
        pdes.rename(columns={'index':'main_id'}, inplace=True)
        phys_desc = pd.concat([pdes, pd.json_normalize(pdes['physicalDescription'])],
                              axis=1).reset_index()
        phys_desc.rename(columns={'index':'phys_desc_id'}, inplace=True)
        phys_desc = phys_desc[['phys_desc_id', 'main_id', 'label', 'content']]
        phys_desc.drop_duplicates()
        phys_desc = phys_desc[phys_desc.notna().all(axis=1)]
        
        # manufacturer
        df_ft = pd.json_normalize(main['freetext'], max_level=0)
        name = pd.DataFrame(df_ft['name'])
        name = pd.DataFrame(name['name'].explode())
        name.reset_index(inplace=True)
        name.rename(columns={'index':'main_id'}, inplace=True)
        manufacturer = pd.concat([name, pd.json_normalize(name['name'])],
                                 axis=1).reset_index()
        manufacturer.rename(columns={'index':'manufacturer_id'}, inplace=True)
        manufacturer = manufacturer[['manufacturer_id', 'main_id', 'label', 'content']]
        manufacturer.drop_duplicates()
        manufacturer = manufacturer[manufacturer.notna().all(axis=1)]
        
        # set_name
        df_ft = pd.json_normalize(main['freetext'], max_level=0)
        sn = pd.DataFrame(df_ft['setName'])
        sn = pd.DataFrame(sn['setName'].explode())
        sn.reset_index(inplace=True)
        sn.rename(columns={'index':'main_id'}, inplace=True)
        
        setname = pd.concat([sn, pd.json_normalize(sn['setName'])], axis=1).reset_index()
        setname.rename(columns={'index':'setname_id'}, inplace=True)
        setname = setname[['setname_id', 'main_id', 'label', 'content']]
        setname.drop_duplicates()
        setname = setname[setname.notna().all(axis=1)]
        
        # credit_line
        df_ft = pd.json_normalize(main['freetext'], max_level=0)
        cl = pd.DataFrame(df_ft['creditLine'])
        cl = pd.DataFrame(cl['creditLine'].explode())
        cl.reset_index(inplace=True)
        cl.rename(columns={'index':'main_id'}, inplace=True)
        creditline = pd.concat([cl, pd.json_normalize(cl['creditLine'])],
                               axis=1).reset_index()
        creditline.rename(columns={'index':'creditline_id'}, inplace=True)
        creditline = creditline[['creditline_id', 'main_id', 'label', 'content']]
        creditline.drop_duplicates()
        creditline = creditline[creditline.notna().all(axis=1)]
        
        # notes
        df_ft = pd.json_normalize(main['freetext'], max_level=0)
        nt = pd.DataFrame(df_ft['notes'])
        nt = pd.DataFrame(nt['notes'].explode())
        nt.reset_index(inplace=True)
        nt.rename(columns={'index':'main_id'}, inplace=True)
        notes = pd.concat([nt, pd.json_normalize(nt['notes'])],
                          axis=1).reset_index()
        notes.rename(columns={'index':'notes_id'}, inplace=True)
        notes = notes[['notes_id', 'main_id', 'label', 'content']]
        notes.drop_duplicates()
        notes = notes[notes.notna().all(axis=1)]
        
        # invty_desc
        df_is = pd.json_normalize(main['indexedStructured'], max_level=0)
        invty = pd.concat([main['main_idx'], main['title'], df_is['object_type'].str.get(0),
                           df_is['place'].str.get(0), df_is['usage_flag'].str.get(0)], axis=1)
        invty = invty.reset_index()
        invty.rename(columns={'index':'invty_desc_id', 'main_idx':'main_id'}, inplace=True)
        invty = invty[invty.notna().all(axis=1)]
        invty.drop_duplicates(subset='invty_desc_id', keep='first')
        
        # fact_table
        fact = main[['id', 'main_idx', 'url', 'hash', 'docSignature',
                     'timestamp', 'lastTimeUpdated', 'title']]
        fact = fact.rename(columns={'main_idx':'main_id'})
        
        rs = res[['resources_id', 'main_id']]
        med = media[['media_id', 'main_id']]
        pdesc = phys_desc[['phys_desc_id', 'main_id']]
        mfg = manufacturer[['manufacturer_id', 'main_id']]
        sn = setname [['setname_id', 'main_id']]
        cl2 = creditline[['creditline_id', 'main_id']]
        nt = notes[['notes_id', 'main_id']]
        inv = invty[['invty_desc_id', 'main_id']]
        
        med_rs = med.merge(rs, on='main_id', how='inner')
        pdesc_mfg = pdesc.merge(mfg, on='main_id', how='inner')
        pdesc_mfg_sn = pdesc_mfg.merge(sn, on='main_id', how='inner')
        pdesc_mfg_sn_cl2 = pdesc_mfg_sn.merge(cl2, on='main_id', how='inner')
        pdesc_mfg_sn_cl2_notes = pdesc_mfg_sn_cl2.merge(nt, on='main_id', how='inner')
        pdesc_mfg_sn_cl2_notes_inv = pdesc_mfg_sn_cl2_notes.merge(inv, on='main_id', how='inner')
        all_dims = pdesc_mfg_sn_cl2_notes_inv.merge(med_rs, on='main_id', how='inner')
        fact_mm = fact.merge(all_dims, on='main_id', how='inner')
        
        media = media.drop('main_id', axis=1)
        res = res.drop('main_id', axis=1)
        phys_desc = phys_desc.drop('main_id', axis=1)
        manufacturer = manufacturer.drop('main_id', axis=1)
        setname = setname.drop('main_id', axis=1)
        creditline = creditline.drop('main_id', axis=1)
        notes = notes.drop('main_id', axis=1)
        invty = invty.drop('main_id', axis=1)
        
        phys_desc['content'] = phys_desc['content'].apply(lambda x: x[:250])
        notes['content'] = notes['content'].apply(lambda x: x[:250])
        
        ## Setup hooks and connection objects
        olap_hook = RedshiftSQLHook(redshift_conn_id='music_olap')
        conn = olap_hook.get_sqlalchemy_engine()
        
        ## Write dataframes to Redshift DB tables
        media.to_sql('dim_media', conn, if_exists='replace', index=False)
        res.to_sql('dim_media_resources', conn, if_exists='replace', index=False)
        phys_desc.to_sql('dim_physical_desc', conn, if_exists='replace', index=False)
        manufacturer.to_sql('dim_manufacturer', conn, if_exists='replace', index=False)
        setname.to_sql('dim_setname', conn, if_exists='replace', index=False)
        creditline.to_sql('dim_creditline', conn, if_exists='replace', index=False)
        notes.to_sql('dim_notes', conn, if_exists='replace', index=False)
        invty.to_sql('dim_invty_desc', conn, if_exists='replace', index=False)
        # fact_mm.to_sql('fact_artifact', conn, if_exists='replace', index=False)

    create_tables >> museum_olap()