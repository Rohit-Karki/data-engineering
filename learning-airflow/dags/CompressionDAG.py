import os
from airflow import Dataset, DAG
from pendulum import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from minio import Minio
from minio.error import S3Error
from datetime import datetime, timedelta
from LocalFileSensor import LocalFileSensor
from ZipOperator import ZipOperator

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'file_compression_and_email_workflow',
    default_args=default_args,
    description='Monitor folder for files and process them',
    # schedule_interval=timedelta(minutes=1),  # Check every 1 minutes
    catchup=False,
)


def check_fileName_in_minio(**kwargs):
    folder_path = '/usr/local/airflow/files'
    os.makedirs(folder_path, exist_ok=True)

    # Get filename from webhook trigger
    filename = kwargs['dag_run'].conf.get('filename')
    print(f"📂 Received filename from conf: {filename}")

    # MinIO client setup
    client = Minio(
        "minio:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )

    bucket_name = "mybucket"
    local_file_path = os.path.join(folder_path, os.path.basename(filename))

    # Download the file
    try:
        client.fget_object(bucket_name, filename, local_file_path)
        print(f"✅ File downloaded to {local_file_path}")
    except Exception as e:
        print(f"❌ Error downloading file from MinIO: {e}")
        raise
    # Push to XCom
    kwargs['ti'].xcom_push(key='file_path', value=local_file_path)

    return local_file_path


def upload_zip_to_minio(**context):
    local_zip_path = context['ti'].xcom_pull(
        task_ids='process_task', key='zip_file_path')
    print(f"{local_zip_path}")
    minio_client = Minio(
        "minio:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )

    bucket_name = "processed-files"
    object_name = local_zip_path.split("/")[-1]  # just the file name

    # Ensure bucket and upload
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)

    minio_client.fput_object(bucket_name, object_name, local_zip_path)

    # Optionally push to XCom
    context['ti'].xcom_push(key='zipped_file_url',
                            value=f"s3://{bucket_name}/{object_name}")
    print(f"s3://{bucket_name}/{object_name}")


check_dir_task = PythonOperator(
    task_id='check_directory',
    python_callable=check_fileName_in_minio,
    dag=dag
)

# Set up the processing task
process_task = ZipOperator(
    task_id='process_task',
    dag=dag
)

# Upload the zipped file to Minio
upload_to_minio = PythonOperator(
    task_id='upload_to_minio',
    python_callable=upload_zip_to_minio,
    dag=dag
)

# send_email = EmailOperator(
#     task_id='send_email',
#     to='rohitkarki804@gmail.com',
#     subject='Zipped and Unzipped File Paths',
#     html_content="""
#     <h3>Files Processed Successfully</h3>
#     <p><strong>Unzipped File Path:</strong> {{ ti.xcom_pull(task_ids='check_directory', key='file_path') }}</p>
#     <p><strong>Zipped File Path:</strong> {{ ti.xcom_pull(task_ids='process_task', key='zip_file_path') }}</p>
#     """,
#     dag=dag
# )

# Define task dependencies
check_dir_task >> process_task >> upload_to_minio
