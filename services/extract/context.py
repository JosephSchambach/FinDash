from google.cloud import bigquery, storage
from google.oauth2 import service_account
from supabase import create_client, Client

import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
class Context:
    def __init__(self):
        self.logger = logger
        self.storage = self._get_supabase()
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

    def _get_supabase(self):
        try: 
            url = os.getenv("SUPABASE_STORAGE_URL")
            key = os.getenv("SUPABASE_KEY")
            client: Client = create_client(url, key)
            return client
        except Exception as e:
            self.logger.error(f"Error getting Supabase client: {e}")
            return None