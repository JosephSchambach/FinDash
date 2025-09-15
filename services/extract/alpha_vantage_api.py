import requests
import pandas as pd
from models import parse_alpha_vantage, validated_data
import os

class AlphaVantage:
    def __init__(self, logger): 
        self.logger = logger 
        self.api_key = os.getenv("ALPHAVANTAGE_API_KEY")
        self.base_url = os.getenv("ALPHAVANTAGE_BASE_URL")
        
    def fetch_stock_price(self, symbol, interval, function):
        url = f"{self.base_url}function={function}&symbol={symbol}&interval={interval}&apikey={self.api_key}"
        try:
            response = requests.get(url)
            data = response.json()
            if f"Time Series ({interval})" in data:
                data = data[f"Time Series ({interval})"]
                data = parse_alpha_vantage(data, symbol, self.logger)
                df = validated_data(data)
                return df
            self.logger.warning(f"Unexpected data format for {symbol}: {interval}")
            return data
        except Exception as e:
            self.logger.error(f"Error fetching stock price: {e}")
            return {}