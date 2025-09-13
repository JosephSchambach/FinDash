from datetime import datetime
import pyarrow as pa
import pandas as pd
import os

class DataLoader:
    def __init__(self, context):
        self.context = context
    
    def load(self, file_path, bucket_name):
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        now = datetime.now().strftime("%Y%m%d_%H%M%S")
        storage_file_path = f"stock_prices/{now}.parquet"
        try:
            with open(file_path, 'rb') as file:
                response = self.context.storage.storage.from_('raw-stock-data').upload(path=storage_file_path, file=file, file_options={"upsert": "true"})
            return True
        except Exception as e:
            self.context.logger.error(f"Error uploading file to GCS: {e}")
            return False

    def drop(self, file_path):
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        try:
            os.remove(file_path)
        except Exception as e:
            self.context.logger.error(f"Error deleting file: {e}")
