import math
import os
import tempfile
import logging
from zipfile import ZipFile
from airflow.models import BaseOperator
from airflow.utils import apply_defaults


class ZipOperator(BaseOperator):
    """
    An operator which takes in a path to a file and zips the contents to a location you define.

    :param path_to_file_to_zip: Full path to the file you want to Zip
    :type path_to_file_to_zip: string
    :param path_to_save_zip: Full path to where you want to save the Zip file
    :type path_to_save_zip: string
    """

    template_fields = ('path_to_file_to_zip', 'path_to_save_zip')

    @apply_defaults
    def __init__(
            self,
            path_to_file_to_zip=None,
            path_to_save_zip=None,
            *args, **kwargs):
        super(ZipOperator, self).__init__(*args, **kwargs)
        self.path_to_file_to_zip = path_to_file_to_zip
        self.path_to_save_zip = path_to_save_zip

    def execute(self, context):
        logging.info("Executing ZipOperator.execute(context)")        
        print(f"Processing file: {file_path}")       

        # Get the path from XCom if not provided
        if not self.path_to_file_to_zip:
            self.path_to_file_to_zip = context['ti'].xcom_pull(task_ids='wait_for_file', key='file_path')
            

        # Generate zip path if not provided
        if not self.path_to_save_zip:
            folder_path = os.path.dirname(self.path_to_file_to_zip)
            file_name = os.path.basename(self.path_to_file_to_zip)
            base_name = os.path.splitext(file_name)[0]
            self.path_to_save_zip = os.path.join(folder_path, f"{base_name}.zip")
        
        
        logging.info(f"Path to the File to Zip: {self.path_to_file_to_zip}")
        logging.info(f"Path to save the Zip File: {self.path_to_save_zip}")

        dir_path_to_file_to_zip = os.path.dirname(
            os.path.abspath(self.path_to_file_to_zip))
        file_to_zip_name = os.path.basename(self.path_to_file_to_zip)

        # Create the zip file
        with ZipFile(self.path_to_save_zip, 'w') as zip_file:
            is_file = os.path.isfile(self.path_to_file_to_zip)

            if is_file:
                logging.info(f"Writing '{file_to_zip_name}' to zip file")
                zip_file.write(self.path_to_file_to_zip,
                               arcname=file_to_zip_name)
            else:  # is folder
                for dirname, subdirs, files in os.walk(self.path_to_file_to_zip):
                    zip_file.write(dirname, os.path.relpath(
                        dirname, os.path.join(self.path_to_file_to_zip, '..')))
                    for filename in files:
                        file_path = os.path.join(dirname, filename)
                        arcname = os.path.join(os.path.relpath(
                            dirname, os.path.join(self.path_to_file_to_zip, '..')), filename)
                        logging.info(
                            f"Writing '{file_path}' to zip file as '{arcname}'")
                        zip_file.write(file_path, arcname=arcname)

        logging.info(
            f"Zip file created successfully at {self.path_to_save_zip}")

        # Push the zip file path to XCom for downstream tasks
        context['ti'].xcom_push(key='zip_file_path',
                                value=self.path_to_save_zip)

        return self.path_to_save_zip
