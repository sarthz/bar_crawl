import os

from airflow import DAG
from datetime import datetime
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from google.cloud import storage


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = "bar_crawl_data"
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'bar_crawl_data')

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="move_gcs_bar_crawl_data_to_bq_workflow",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dcamp'],
) as dag:

        gcs_to_bq_external_task = BigQueryCreateExternalTableOperator(
            task_id=f"bigquery_external_table_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"nyc_bar_crawl_data",
                },
                "externalDataConfiguration": {
                    "autodetect": True,
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"gs://{BUCKET}/raw/*"],
                },
            },
        )

        CREATE_PARTITION_TABLE_QUERY=f"""    
            CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.nyc_bar_crawl_data_partitoned
            PARTITION BY DATE(record_date) AS
            SELECT * FROM {BIGQUERY_DATASET}.nyc_bar_crawl_data;
        """

        bq_external_to_partition_table_task = BigQueryInsertJobOperator(
            task_id=f"insert_query_job_bar_crawl",
            configuration={
                "query": {
                    "query": CREATE_PARTITION_TABLE_QUERY,
                    "useLegacySql": False,
                }
            },
        )

gcs_to_bq_external_task >> bq_external_to_partition_table_task