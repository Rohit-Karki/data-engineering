
class GoogleDriveDownloadOperator(BaseOperator):
    """
    Operator that downloads a file from Google Drive.

    :param file_id: Google Drive file ID to download (can be passed via XCom)
    :type file_id: string
    :param destination_path: Local path where to save the downloaded file
    :type destination_path: string
    :param service_account_json: Path to Google service account credentials JSON file
    :type service_account_json: string
    """

    template_fields = ('file_id', 'destination_path', 'service_account_json')

    @apply_defaults
    def __init__(
            self,
            file_id=None,
            destination_path=None,
            service_account_json=None,
            *args, **kwargs):
        super(GoogleDriveDownloadOperator, self).__init__(*args, **kwargs)
        self.file_id = file_id
        self.destination_path = destination_path
        self.service_account_json = service_account_json

    def _get_drive_service(self):
        """Create and return an authorized Google Drive service instance."""
        scopes = ['https://www.googleapis.com/auth/drive.readonly']
        credentials = service_account.Credentials.from_service_account_file(
            self.service_account_json, scopes=scopes)

        service = build('drive', 'v3', credentials=credentials)
        return service

    def execute(self, context):
        # Get file_id from XCom if not provided
        if not self.file_id:
            self.file_id = context['ti'].xcom_pull(
                task_ids=context['ti'].task_id, key='file_id')
            if not self.file_id:
                raise ValueError("No file_id provided or found in XCom")

        # Get file name from XCom if available
        file_name = context['ti'].xcom_pull(
            task_ids=context['ti'].task_id, key='file_name')

        service = self._get_drive_service()

        # Get file metadata if we don't have the name
        if not file_name:
            file_metadata = service.files().get(fileId=self.file_id).execute()
            file_name = file_metadata['name']

        # Set destination path if not provided
        if not self.destination_path:
            temp_dir = tempfile.gettempdir()
            self.destination_path = os.path.join(temp_dir, file_name)

        logging.info(
            f"Downloading file '{file_name}' (ID: {self.file_id}) to {self.destination_path}")

        # Download the file
        request = service.files().get_media(fileId=self.file_id)

        with open(self.destination_path, 'wb') as f:
            # Stream the download to avoid loading large files into memory
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                logging.info(
                    f"Download progress: {int(status.progress() * 100)}%")

        logging.info(
            f"File downloaded successfully to {self.destination_path}")

        # Push the download path to XCom for downstream tasks
        context['ti'].xcom_push(
            key='downloaded_file_path', value=self.destination_path)

        return self.destination_path
