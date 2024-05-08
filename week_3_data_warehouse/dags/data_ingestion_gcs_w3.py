import os
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from google.cloud import storage

from datetime import datetime

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

airflow_home = os.environ.get("airflow_home", "/opt/airflow/")
url_frefix = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
parquet_file = "green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"
url_template = url_frefix + parquet_file

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

w3_workflow = DAG(
    "DataIngestion",
    schedule_interval = "0 0 1 * *",
    start_date = datetime(2022, 1, 1),
    end_date = datetime(2022, 12, 31)

)

with w3_workflow:
    download_dataset_task = BashOperator(
        task_id = "download_dataset_task",
        bash_command = f"curl -ssL {url_template} > {airflow_home}/{parquet_file}"
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
        task_id = "remove_file_task",
        bash_command = f"rm {airflow_home}/{parquet_file}"
    )

    download_dataset_task >> upload_to_gcs_task >> remove_files_task