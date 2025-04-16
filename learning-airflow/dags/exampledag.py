import os
from airflow import Dataset, DAG
from airflow.decorators import dag, task
from pendulum import datetime
from airflow.operators.python import PythonOperator
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
    'file_processing_workflow',
    default_args=default_args,
    description='Monitor folder for files and process them',
    schedule_interval=timedelta(minutes=5),  # Check every 5 minutes
    catchup=False,
)

# Define the path to monitor
folder_path='/usr/local/airflow'  # inside Docker

def check_directory(**kwargs):
    import os
    directory = folder_path
    files = os.listdir(directory)
    print(f"Files in directory: {files}")
    return files

check_dir_task = PythonOperator(
    task_id='check_directory',
    python_callable=check_directory,
    dag=dag
)

# Function to process the file
def process_file(**context):
    file_path = context['ti'].xcom_pull(task_ids='wait_for_file', key='file_path')
    print(f"Processing file: {file_path}")
    
    # Your processing logic here
    
    # Optionally, move the file to a 'processed' folder
    processed_dir = os.path.join(os.path.dirname(file_path), 'processed')
    os.makedirs(processed_dir, exist_ok=True)
    new_path = os.path.join(processed_dir, os.path.basename(file_path))
    os.rename(file_path, new_path)
    
    return f"Processed and moved file to {new_path}"

# Set up the FileSensor to wait for a file
wait_for_file = LocalFileSensor(
    task_id='wait_for_file',
    directory_path=folder_path + '/',  # Use wildcard to detect any file
    # fs_conn_id='fs_default',  # Connection to the filesystem
    poke_interval=30,         # Check every 30 seconds
    timeout=60 * 30,          # Timeout after 30 minutes
    mode='poke',              # Use poke mode to continuously check
    dag=dag,
)


# # Set up the processing task
# process_task = PythonOperator(
#     task_id='process_file',
#     python_callable=process_file,
#     provide_context=True,
#     dag=dag,
# )

# Set up the processing task
process_task = ZipOperator(
    dag=dag
)

# Define task dependencies
check_dir_task >> wait_for_file >> process_task