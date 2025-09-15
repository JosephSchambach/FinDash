import os 

working_dir = os.path.dirname(os.path.abspath(__file__))

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yfinance as yf
import requests as req
from alpha_vantage_api import AlphaVantage
from models import parse_yfinance, validated_data


class DataExtractor:
    def __init__(self, context):
        self.context = context
        self.alpha_vantage = AlphaVantage(self.context.logger)
        self.file_path = os.path.join(working_dir, 'extracted_data.parquet')
        self.dataframe_list = []
        self.dataframe = pd.DataFrame() 
    
    def alphavantage_extract(self, symbol, period, function):
        data = self.alpha_vantage.fetch_stock_price(symbol, period, function)
        return data

    def yfinance_extract(self, symbol, period, function):
        try:
            data = yf.download(tickers=symbol, period=period)
            data = parse_yfinance(data, symbol, self.context.logger)
            df = validated_data(data)
            return df
        except Exception as e:
            self.context.logger.error(f"Error extracting data from yfinance: {e}")
            return pd.DataFrame()

    def append(self, raw_data):
        if isinstance(raw_data, pd.DataFrame) and not raw_data.empty:
            try:
                self.dataframe_list.append(raw_data)
            except Exception as e:
                self.context.logger.error(f"Error appending data: {e}")
        else:
            self.context.logger.warning("Received empty or invalid DataFrame, skipping append.")

    def save(self):
        try:
            self.dataframe = pd.concat(self.dataframe_list, axis=0, ignore_index=True)
            table = pa.Table.from_pandas(self.dataframe)
            pq.write_table(table, self.file_path)
        except Exception as e:
            self.context.logger.error(f"Error saving data: {e}")