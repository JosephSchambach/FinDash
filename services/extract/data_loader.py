from google.cloud import storage
import pyarrow as pa
import pandas as pd

class DataLoader:
    def __init__(self, context, data, bucket_name):
        self.context = context
        self.data = data
        self.bucket_name = bucket_name
        self.client = storage.Client()
    
    def load(self, filename):
        pass
    
    def drop(self, filename):
        pass
