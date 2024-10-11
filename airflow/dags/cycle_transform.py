import pandas as pd
import os
import time
import logging
import boto3

from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

BUCKET_NAME = "tfl-cycle"
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")

def get_wanted_files_dates(bucket_name:str, yyyymm:str):
    session = boto3.Session(
    aws_access_key_id='xxx',
    aws_secret_access_key='xxx',
    region_name='eu-west-3'
)
    ressources = session.resource('s3')
    bucket = ressources.Bucket('tfl-cycle')

    get_fnames = [i.split("/")[-1] for i in [obj.key for obj in bucket.objects.all()]]
    get_dates = [i[:7] for i in get_fnames]

    file_list = []
    for f_date, f_names in zip(get_dates, get_fnames) :
        if f_date.startswith(yyyymm):
            file_list.append(f'bronze/{f_names}')

    return [
        {
        "filename": f'./data/{file}',
        "object_name": file
        }
    for file in file_list]

def prepare_data(current_date:str, **context):
    ti=context['ti']
    file_list=ti.xcom_pull(task_ids='get_files_names')

    for file in file_list:
        fi = file["filename"].split("/")[-1]
        logging.info(f'Opening the following file: ./data/bronze/{fi}')
        logging.info(os.path.getsize(f'./data/bronze/{fi}'))
        df = pd.read_parquet(f'./data/bronze/{fi}')
        is_old_format="Rental Id" in df.columns

        df = df.rename(columns={'Rental Id': 'Number',
                                    'Start Date': 'Start date',
                                    'End Date': 'End date',
                                    'StartStation Name': 'Start station',
                                    'Start Station Name': 'Start station',
                                    'Duration_Seconds': 'Total duration',
                                    'EndStation Name': 'End station',
                                    'End Station Name': 'End station',
                                    'Duration': 'Total duration',
                                    'Bike Id': 'Bike number'
                                    })
        if not is_old_format:
            df = df.drop(['Bike model','Total duration'], axis=1)
            df = df.rename(columns={"Total duration (ms)":"Total duration"})
            df['Total duration'] = df['Total duration'].map(lambda x: x*0.001)
        cols_to_keep=['Number', 'Start date', 'Start station',
       'End date', 'End station', 'Bike number', 'Total duration']
        df=df[cols_to_keep]
        df.dropna(inplace=True)
        df['Total duration'] = df['Total duration'].map(lambda x: int(x))
        df['Bike number'] = df['Bike number'].map(lambda x: int(x))

        df["date"] = current_date

        df.to_parquet(f'./data/silver/{fi}')

def download_from_s3(bucket_name:str):

    session = boto3.Session(
    aws_access_key_id='xxx',
    aws_secret_access_key='xxx',
    region_name='eu-west-3'
)
    ressources = session.resource('s3')
    bucket = ressources.Bucket(bucket_name)

    for obj in bucket.objects.all():
        local_file = os.path.join(f"./data/bronze", os.path.basename(obj.key))
        bucket.download_file(obj.key, local_file)

def upload_to_s3(bucket_name):

    session = boto3.Session(
    aws_access_key_id='xxx',
    aws_secret_access_key='xxx',
    region_name='eu-west-3'
)
    s3 = session.client('s3')

    path='./data/silver'
    for file in os.listdir(f"{AIRFLOW_HOME}/data/silver"):
        s3.upload_file(
            os.path.join(path,file),
            bucket_name,
            f'silver/{file}'
        )

with DAG(
    "transform",
    start_date=datetime(2015, 1, 1, tzinfo=timezone.utc),
    schedule_interval='@monthly',
    default_args={"depends_on_past": True},
    catchup=True,
) as dag:

    current_year_month = '{{ds_nodash[:6]}}'
    current_date = '{{ds_nodash}}'
    gcp_con_id='google_conn'

    get_files_names_task = PythonOperator(
        task_id="get_files_names",
        python_callable=get_wanted_files_dates,
        op_kwargs={"bucket_name": BUCKET_NAME,
                   "yyyymm":current_year_month}
    )

    download_from_s3_task = PythonOperator(
        task_id="download_from_s3",
        python_callable=download_from_s3,
        op_kwargs={"bucket_name":BUCKET_NAME
        }
    )

    prepare_data_task = PythonOperator(
        task_id="prepare_data",
        python_callable=prepare_data,
        op_kwargs={"current_date":current_date},
        provide_context=True,
        retries=3
    )

    upload_to_s3_task = PythonOperator(
        task_id="upload_to_s3_silver",
        python_callable=upload_to_s3,
        op_kwargs={"bucket_name": BUCKET_NAME},
        retries=3
    )

    get_files_names_task >> download_from_s3_task >> prepare_data_task >> upload_to_s3_task
