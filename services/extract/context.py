from google.cloud import bigquery, storage
from google.oauth2 import service_account

import os
import logging

os.environ['PATH_TO_CREDENTIALS'] = 'path/to/your/service-account-file.json'
os.environ['GCS_BUCKET_NAME'] = 'your-gcs-bucket-name'
os.environ['GCP_PROJECT_ID'] = 'your-gcp-project-id'

logger = logging.getLogger()
logger.setLevel(logging.INFO)
class Context:
    def __init__(self):
        self.logger = logger
        # self.__credential_path = os.getenv("PATH_TO_CREDENTIALS")
        # self.__credentials = service_account.Credentials.from_service_account_file(self.__credential_path)
        # self.__project_id = os.getenv("GCP_PROJECT_ID")
        # self.bigquery = self.__get_bigquery()
        # self.storage = self.__get_storage()
        
    def __get_storage(self):
        try:
            client = storage.Client(credentials=self.__credentials, project=self.__project_id)
            return client
        except Exception as e:
            self.logger.error(f"Error getting GCS client: {e}")
            return None
        
    def __get_bigquery(self):
        try:
            client = bigquery.Client(credentials=self.__credentials, project=self.__project_id)
            return client
        except Exception as e:
            self.logger.error(f"Error getting BigQuery client: {e}")
            return None
