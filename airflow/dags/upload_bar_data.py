import os
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

from datetime import datetime
from google.cloud import storage

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = "bar_crawl_data"
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
URL_TEMPLATE = 'https://data.cityofnewyork.us/api/views/43nn-pn8j/rows.csv?accessType=DOWNLOAD'
# path_to_local_home = "/Users/sthakur/Documents/GitHub/personal/nyc_bar_crawl/data"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
dataset_file = 'DOHMH_New_York_City_Restaurant_Inspection_Results.csv' # /Users/sthakur/Documents/GitHub/personal/nyc bar crawl/data
parquet_file = dataset_file.replace('.csv', '.parquet')
OUTPUT_FILE = 'bar_crawl_data'
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

def upload_to_gcs(bucket, object_name, local_file):

    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    print("Source file:",src_file)
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))

# DAG START

bar_data_workflow = DAG(
    "bar_data_dag",
    schedule_interval="0 6 2 * *", #on the sixth 
    start_date=datetime(2022,8,16),
    end_date=datetime(2022,8,31)
)

with bar_data_workflow:

    entry_task = BashOperator(
        task_id="entry_task",
        # bash_command=f"curl -sSL {URL_TEMPLATE} > {path_to_local_home}/{OUTPUT_FILE_TEMPLATE_FP}"
        # bash_command='echo "{{ execution_date.strftime(\'%Y-%m-%d\') }}"'
        bash_command='echo entering the dag'
    )

    download_data_task = BashOperator(
        task_id='download_data_task',
        bash_command=f"curl -sSL {URL_TEMPLATE} > {path_to_local_home}/{dataset_file}"
        # bash_command='echo "{{ execution_date.strftime(\'%Y_%m\') }}" '
        # bash_command=f'echo "{URL_TEMPLATE}<-->{path_to_local_home}/{OUTPUT_FILE_TEMPLATE_FP}"'
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{dataset_file}",
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        # bash_command='echo "{{ execution_date.strftime(\'%Y-%m\') }}"'
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{OUTPUT_FILE}",
            "local_file": f"{path_to_local_home}/{parquet_file}",
        },
    )

entry_task >> download_data_task >> format_to_parquet_task >> local_to_gcs_task

# DAG END