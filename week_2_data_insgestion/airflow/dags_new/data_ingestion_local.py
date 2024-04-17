import os

from datetime import datetime

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingest_script import ingest_callable

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_DATABASE = os.getenv('PG_DATABASE')
PG_PORT = os.getenv('PG_PORT')

local_workflow = DAG(
    "LocalIngestionDag",
    schedule_interval="0 0 1 * *",
    start_date = datetime(2021, 1, 1),
    end_date = datetime(2021, 7, 31)
)

dataset_file = 'yellow_tripdata_2021-01'
url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'

URL_PREFIX = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
URL_TEMPLATE = URL_PREFIX + "/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz"
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + "/output_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz"
OUTPUT_FILE_TEMPLATE_CSV = AIRFLOW_HOME + "/output_{{ execution_date.strftime(\'%Y-%m\') }}.csv"
TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'

with local_workflow:
    
    wget_task = BashOperator(
        task_id = 'wget',
        bash_command=f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}'
    )

    extract_dataset_task = BashOperator(
    task_id="extract_dataset_task",
    bash_command = f"gzip -f -d {OUTPUT_FILE_TEMPLATE}"
    )

    ingest_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=ingest_callable,
        op_kwargs=dict(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            db=PG_DATABASE,
            table_name=TABLE_NAME_TEMPLATE,
            csv_file=OUTPUT_FILE_TEMPLATE_CSV
        )
    )

    wget_task >> extract_dataset_task >> ingest_task
    