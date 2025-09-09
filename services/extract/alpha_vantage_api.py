from dotenv import load_dotenv
import requests
import pandas as pd
import os

load_dotenv()

class AlphaVantage:
    def __init__(self, logger): 
        self.logger = logger 
        self.api_key = os.getenv("ALPHAVANTAGE_API_KEY")
        self.base_url = os.getenv("ALPHAVANTAGE_BASE_URL")
        
    def fetch_stock_price(self, symbol, interval):
        url = f"{self.base_url}function=TIME_SERIES_INTRADAY&symbol={symbol}&interval={interval}&apikey={self.api_key}"
        try:
            response = requests.get(url)
            data = response.json()
            if f"Time Series ({interval})" in data:
                df = pd.DataFrame.from_dict(data[f"Time Series ({interval})"], orient="index")
                df.index = pd.to_datetime(df.index)
                df = df.rename(columns={
                    "1. open": "open",
                    "2. high": "high",
                    "3. low": "low",
                    "4. close": "close",
                    "5. volume": "volume"
                })
                df['symbol'] = symbol.upper()
                return df
            return data
        except Exception as e:
            self.logger.error(f"Error fetching stock price: {e}")
            return {"error": str(e)}