import os
import logging
from datetime import datetime, timedelta
from airflow.models import BaseOperator, TaskInstance
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.dates import days_ago
from googleapiclient.discovery import build
from zipfile import ZipFile
import tempfile

class LocalFileSensor(BaseSensorOperator):
    """
    Sensor that monitors a local file system folder for new files.
    
    :param directory_path: Local directory path to monitor
    :type directory_path: string
    :param last_modified_time: Only detect files modified after this time (defaults to 1 hour ago)
    :type last_modified_time: datetime
    :param file_extension: Optional file extension to filter by (e.g., '.pdf')
    :type file_extension: string
    """
    template_fields = ('directory_path', 'file_extension')
    
    def __init__(
        self,
        directory_path,
        last_modified_time=None,
        file_extension=None,
        *args, **kwargs):
        super(LocalFileSensor, self).__init__(*args, **kwargs)
        self.directory_path = directory_path
        self.last_modified_time = last_modified_time or datetime.utcnow() - \
            timedelta(hours=1)
        self.file_extension = file_extension
    
    def poke(self, context):
        """Check if there are new files in the specified directory."""
        import os
        from datetime import datetime
        
        logging.info(f"Checking for new files in directory: {self.directory_path}")
        
        # Check if directory exists
        if not os.path.exists(self.directory_path):
            logging.error(f"Directory does not exist: {self.directory_path}")
            return False
        
        # Convert timestamp to Unix time for comparison
        time_threshold = self.last_modified_time.timestamp()
        
        new_files = []
        
        # Walk through directory
        for file_name in os.listdir(self.directory_path):
            file_path = os.path.join(self.directory_path, file_name)
            
            # Skip directories
            if os.path.isdir(file_path):
                continue
                
            # Apply file extension filter if specified
            if self.file_extension and not file_name.endswith(self.file_extension):
                continue
                
            # Check modified time
            mod_time = os.path.getmtime(file_path)
            if mod_time > time_threshold:
                file_size = os.path.getsize(file_path)
                new_files.append({
                    'name': file_name,
                    'path': file_path,
                    'size': file_size,
                    'modifiedTime': datetime.fromtimestamp(mod_time).isoformat(),
                })
        
        if not new_files:
            logging.info("No new files detected")
            return False
            
        logging.info(f"Detected {len(new_files)} new file(s)")
        
        # Push file information to XCom for downstream tasks
        context['ti'].xcom_push(key='detected_files', value=new_files)
        
        # Push the first file's details as individual XComs for easy access
        if new_files:
            context['ti'].xcom_push(key='file_name', value=new_files[0]['name'])
            context['ti'].xcom_push(key='file_path', value=new_files[0]['path'])
            context['ti'].xcom_push(key='file_size', value=new_files[0]['size'])
            context['ti'].xcom_push(key='modified_time', value=new_files[0]['modifiedTime'])
            
        return True