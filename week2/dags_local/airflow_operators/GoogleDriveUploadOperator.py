from airflow.models import BaseOperator, TaskInstance
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account

class GoogleDriveUploadOperator(BaseOperator):
    """
    An operator which uploads files to Google Drive.

    :param source_path: Full path to the file you want to upload
    :type source_path: string
    :param destination_folder_id: Google Drive folder ID where to upload the file
    :type destination_folder_id: string  
    :param service_account_json: Path to Google service account credentials JSON file
    :type service_account_json: string
    """

    template_fields = ('source_path', 'destination_folder_id',
                       'service_account_json')

    @apply_defaults
    def __init__(
            self,
            source_path=None,
            destination_folder_id=None,
            service_account_json=None,
            *args, **kwargs):
        super(GoogleDriveUploadOperator, self).__init__(*args, **kwargs)
        self.source_path = source_path
        self.destination_folder_id = destination_folder_id
        self.service_account_json = service_account_json

    def _get_drive_service(self):
        """Create and return an authorized Google Drive service instance."""
        scopes = ['https://www.googleapis.com/auth/drive']
        credentials = service_account.Credentials.from_service_account_file(
            self.service_account_json, scopes=scopes)

        service = build('drive', 'v3', credentials=credentials)
        return service

    def execute(self, context):
        # Get the zip file path from XCom if not provided
        if not self.source_path:
            self.source_path = context['ti'].xcom_pull(key='zip_file_path')

            if not self.source_path:
                raise ValueError("No source path provided or found in XCom")

        # Make sure source path exists
        if not os.path.exists(self.source_path):
            raise FileNotFoundError(
                f"Source path does not exist: {self.source_path}")

        file_name = os.path.basename(self.source_path)
        logging.info(
            f"Uploading {file_name} to Google Drive folder: {self.destination_folder_id}")

        service = self._get_drive_service()

        # Prepare file metadata
        file_metadata = {
            'name': file_name,
            'parents': [self.destination_folder_id]
        }

        # Determine MIME type for ZIP files
        mime_type = 'application/zip'

        # Upload the file
        media = MediaFileUpload(
            self.source_path, mimetype=mime_type, resumable=True)

        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, name, webViewLink'
        ).execute()

        logging.info(
            f"File uploaded: {file.get('name')} (ID: {file.get('id')})")
        logging.info(f"Web view link: {file.get('webViewLink')}")

        # Push the uploaded file info to XCom
        context['ti'].xcom_push(key='uploaded_file_id', value=file.get('id'))
        context['ti'].xcom_push(key='uploaded_file_link',
                                value=file.get('webViewLink'))

        return file.get('id')
