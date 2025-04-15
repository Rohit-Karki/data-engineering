"""
File Compression DAG - Monitors a shared folder for new files, compresses them,
and sends an email with file specifications.
This DAG uses an event-driven approach to detect new file uploads to a shared folder,
compress the files using gzip, and send notification emails with file details.
"""

from airflow import DAG
from airflow.sensor.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.providers.google.drive.sensors.drive import GoogleDriveFileSensor
from airflow.providers.google.drive.hooks.drive import GoogleDriveHook
from airflow.providers.email.operators.email import EmailOperator
from airflow.models import Variable


from airflow_operators import GoogleDriveDownloadOperator
from airflow_operators import GoogleDriveUploadOperator
from airflow_operators import ZipOperator
from airflow_sensors import GoogleDriveSensor

from datetime import datetime, timedelta
import os
import gzip
import shutil
import tempfile
import json


WORKFLOW_DAG_ID = 'compression_workflow_dag'

# start/end times are datetime objects
# here we start execution on Jan 1st, 2017
WORKFLOW_START_DATE = datetime(2025, 4, 15)

# schedule/retry intervals are timedelta objects
# here we execute the DAGs tasks every day
WORKFLOW_SCHEDULE_INTERVAL = timedelta(1)

# default arguments are applied by default to all tasks
# in the DAG
WORKFLOW_DEFAULT_ARGS = {
    'owner': 'Rohit',
    'depends_on_past': False,
    'start_date': WORKFLOW_START_DATE,
    'email': ['rohitkarki804@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

# initialize the DAG

dag = DAG(
    dag_id=WORKFLOW_DAG_ID,
    start_date=WORKFLOW_START_DATE,
    schedule_interval=WORKFLOW_SCHEDULE_INTERVAL,
    default_args=WORKFLOW_DEFAULT_ARGS,
)


# Create the DAG
with DAG(
    'Google Drive',
    default_args=WORKFLOW_DEFAULT_ARGS,
    description='Monitor Google Drive folder, zip new files, and upload back to Drive',
    schedule_interval=timedelta(minutes=30),
    catchup=False,
) as dag:

    # Add necessary imports for MediaIoBaseDownload
    from googleapiclient.http import MediaIoBaseDownload

    # Monitor Google Drive folder for new files
    monitor_drive = GoogleDriveSensor(
        task_id='monitor_drive_folder',
        folder_id='/airflow_uploads',
        service_account_json='./credentials.json',
        poke_interval=300,  # Check every 5 minutes
        timeout=60 * 60 * 12,  # Timeout after 12 hours
        mode='poke'
    )

    # Download the detected file
    download_file = GoogleDriveDownloadOperator(
        task_id='download_file',
        service_account_json='./credentials.json',
        # file_id will be pulled from XCom
    )

    # Task 3: Zip the downloaded file
    zip_file = ZipOperator(
        task_id='zip_file',
        # path_to_file_to_zip will be pulled from XCom
        # path_to_save_zip will be generated automatically
    )

    # Task 4: Upload the zipped file back to Google Drive
    upload_zip = GoogleDriveUploadOperator(
        task_id='upload_zip',
        destination_folder_id='airflow_zipped_folders',
        service_account_json='./credentials.json',
        # source_path will be pulled from XCom
    )

    # Set task dependencies
    monitor_drive >> download_file >> zip_file >> upload_zip
