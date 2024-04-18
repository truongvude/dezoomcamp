import os
import logging


from datetime import datetime



from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

homework_q4_workflow = DAG(
    "DataIngestionHW4",
    schedule_interval = "0 0 1 * *",
    start_date = datetime(2024, 4, 1)
)


airflow_home = os.environ.get("airflow_home", "/opt/airflow/")
dataset_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/"
dataset_file = "taxi_zone_lookup.csv"
parquet_file = dataset_file.replace(".csv", ".parquet")
url_template = dataset_url + dataset_file

def format_to_parquet(src_file):
    if not src_file.endswith(".csv"):
        logging.error("Can only accept source file in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace(".csv", ".parquet"))

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


with homework_q4_workflow:
    download_dataset_task = BashOperator(
        task_id = "download_dataset_task",
        bash_command = f"curl -ssL {url_template} > {airflow_home}/{dataset_file}",
    )

    format_to_parquet_task = PythonOperator(
        task_id = "format_to_parquet_task",
        python_callable = format_to_parquet,
        op_kwargs={"src_file": f"{airflow_home}/{dataset_file}",
        }          
    )

    upload_to_gcs_task = PythonOperator(
        task_id = "upload_to_gcs_task",
        python_callable =upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{parquet_file}",
            "local_file": f"{airflow_home}/{parquet_file}"},
    )

    remove_files_task = BashOperator(
        task_id = "remove_files_task",
        bash_command = f"rm {airflow_home}/{dataset_file} {airflow_home}/{parquet_file}"
    )

    download_dataset_task >> format_to_parquet_task >> upload_to_gcs_task >> remove_files_task