import os 

working_dir = os.path.dirname(os.path.abspath(__file__))

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yfinance as yf
import requests as req
from alpha_vantage_api import AlphaVantage


class DataExtractor:
    def __init__(self, context):
        self.context = context
        self.alpha_vantage = AlphaVantage(self.context.logger)
        self.file_path = os.path.join(working_dir, 'extracted_data.parquet')
        self.dataframe = pd.DataFrame() 
    
    def alphavantage_extract(self, symbol, period):
        data = self.alpha_vantage.fetch_stock_price(symbol, period)
        return data

    def yfinance_extract(self, symbol, period):
        try:
            data = yf.download(tickers=symbol, period=period)
            data.columns = data.columns.to_flat_index()
            lookup = {
                ("Open", symbol.upper()): "open",
                ("High", symbol.upper()): "high",
                ("Low", symbol.upper()): "low",
                ("Close", symbol.upper()): "close",
                ("Volume", symbol.upper()): "volume"
            }
            data = data.rename(columns=lookup)
            data['symbol'] = symbol.upper()
            if "Price" in data.columns:
                data = data.drop(columns=["Price"])
            return data
        except Exception as e:
            self.context.logger.error(f"Error extracting data from yfinance: {e}")
            return pd.DataFrame()

    def append(self, raw_data):
        try:
            self.dataframe = pd.concat([self.dataframe, raw_data], axis=0, ignore_index=True)
        except Exception as e:
            self.context.logger.error(f"Error appending data: {e}")

    def save(self):
        mapping = {
            "open": float,
            "close": float,
            "high": float,
            "low": float,
            "volume": float
        }
        cleaned_data = self.dataframe.astype(mapping)
        table = pa.Table.from_pandas(cleaned_data)
        pq.write_table(table, self.file_path)
