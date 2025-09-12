from pydantic import BaseModel, Field
from datetime import datetime
from typing import List
import pandas as pd

class StockData(BaseModel):
    symbol: str
    timestamp: datetime
    open: float = Field(alias="open")
    high: float = Field(alias="high")
    low: float = Field(alias="low")
    close: float = Field(alias="close")
    volume: int

def parse_alpha_vantage(time_series: dict, symbol: str) -> List[StockData]:
    records = []
    for ts, values in time_series.items():
        records.append(
            StockData(
                symbol=symbol,
                timestamp=datetime.strptime(ts, "%Y-%m-%d %H:%M:%S"),
                open=round(float(values["1. open"]), 2),
                high=round(float(values["2. high"]), 2),
                low=round(float(values["3. low"]), 2),
                close=round(float(values["4. close"]), 2),
                volume=int(values["5. volume"])
            )
        )
    return records

def parse_yfinance(df: pd.DataFrame, symbol: str) -> List[StockData]:
    records = []
    for idx, row in df.iterrows():
        records.append(
            StockData(
                symbol=symbol,
                timestamp=idx.to_pydatetime(),
                open=round(float(row["Open"]) if not hasattr(row["Open"], "iloc") else float(row["Open"].iloc[0]), 2),
                high=round(float(row["High"]) if not hasattr(row["High"], "iloc") else float(row["High"].iloc[0]), 2),
                low=round(float(row["Low"]) if not hasattr(row["Low"], "iloc") else float(row["Low"].iloc[0]), 2),
                close=round(float(row["Close"]) if not hasattr(row["Close"], "iloc") else float(row["Close"].iloc[0]), 2),
                volume=int(row["Volume"]) if not hasattr(row["Volume"], "iloc") else int(row["Volume"].iloc[0])

            )
        )
    return records

def validated_data(data: List[StockData]) -> pd.DataFrame:
    return pd.DataFrame([item.dict() for item in data])