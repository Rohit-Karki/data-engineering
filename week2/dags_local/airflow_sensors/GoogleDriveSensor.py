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


class GoogleDriveSensor(BaseSensorOperator):
    """
    Sensor that monitors a Google Drive folder for new files.
    
    :param folder_id: Google Drive folder ID to monitor
    :type folder_id: string
    :param service_account_json: Path to Google service account credentials JSON file
    :type service_account_json: string
    :param last_modified_time: Only detect files modified after this time (defaults to 1 hour ago)
    :type last_modified_time: datetime
    :param file_extension: Optional file extension to filter by (e.g., '.pdf')
    :type file_extension: string
    """

    template_fields = ('folder_id', 'service_account_json', 'file_extension')

    @apply_defaults
    def __init__(
            self,
            folder_id,
            service_account_json,
            last_modified_time=None,
            file_extension=None,
            *args, **kwargs):
        super(GoogleDriveSensor, self).__init__(*args, **kwargs)
        self.folder_id = folder_id
        self.service_account_json = service_account_json
        self.last_modified_time = last_modified_time or datetime.utcnow() - \
            timedelta(hours=1)
        self.file_extension = file_extension

    def _get_drive_service(self):
        """Create and return an authorized Google Drive service instance."""
        scopes = ['https://www.googleapis.com/auth/drive.readonly']
        credentials = service_account.Credentials.from_service_account_file(
            self.service_account_json, scopes=scopes)

        service = build('drive', 'v3', credentials=credentials)
        return service

    def poke(self, context):
        """Check if there are new files in the specified Google Drive folder."""
        logging.info(
            f"Checking for new files in Google Drive folder: {self.folder_id}")

        service = self._get_drive_service()

        # Format the time for Google Drive API query
        time_threshold = self.last_modified_time.strftime('%Y-%m-%dT%H:%M:%S')

        # Build the query
        query = f"'{self.folder_id}' in parents and modifiedTime > '{time_threshold}'"

        # Add file extension filter if specified
        if self.file_extension:
            query += f" and name contains '{self.file_extension}'"

        query += " and trashed = false"

        # Execute the query
        results = service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name, mimeType, webViewLink, modifiedTime)'
        ).execute()

        files = results.get('files', [])

        if not files:
            logging.info("No new files detected")
            return False

        logging.info(f"Detected {len(files)} new file(s)")

        # Push file information to XCom for downstream tasks
        context['ti'].xcom_push(key='detected_files', value=files)

        # Push the first file's details as individual XComs for easy access
        if files:
            context['ti'].xcom_push(key='file_id', value=files[0]['id'])
            context['ti'].xcom_push(key='file_name', value=files[0]['name'])
            context['ti'].xcom_push(
                key='file_type', value=files[0]['mimeType'])
            context['ti'].xcom_push(
                key='file_link', value=files[0].get('webViewLink', ''))

        return True