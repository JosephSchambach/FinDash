import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

class DataExtractor:
    def __init__(self, context):
        self.context = context
        self.file = ""
        self.dataframe = pd.DataFrame() 
    
    def alphavantage_extract(self, symbol):
        pass

    def yfinance_extract(self, symbol):
        pass
    
    def append(self, raw_data):
        pass
    
    def save(self):
        table = pa.Table.from_pandas(self.dataframe)
        pq.write_table(table, 'extracted_data.parquet')
        self.file = 'extracted_data.parquet'
