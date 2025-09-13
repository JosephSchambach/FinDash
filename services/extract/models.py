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

def parse_alpha_vantage(time_series: dict, symbol: str, logger) -> List[StockData]:
    if time_series is None or not isinstance(time_series, dict):
        logger.warning(f"Invalid time series data for symbol: {symbol}")
        return []
    records = []
    for ts, values in time_series.items():
        try:
            open = round(float(values["1. open"]), 2)
            high = round(float(values["2. high"]), 2)
            low = round(float(values["3. low"]), 2)
            close = round(float(values["4. close"]), 2)
            volume = int(values["5. volume"])
        except KeyError as e:
            logger.error(f"Missing key {e} in Alpha Vantage data")
            continue
        except ValueError as e:
            logger.error(f"Value error {e} in Alpha Vantage data")
            continue
        records.append(
            StockData(
                symbol=symbol,
                timestamp=datetime.strptime(ts, "%Y-%m-%d %H:%M:%S"),
                open=open,
                high=high,
                low=low,
                close=close,
                volume=volume
            )
        )
    return records

def parse_yfinance(df: pd.DataFrame, symbol: str, logger) -> List[StockData]:
    if df.empty:
        logger.warning(f"No data returned from yfinance for symbol: {symbol}")
        return []
    def parse_row(row):
        return StockData(
            symbol=symbol,
            timestamp=row.name,
            open=round(float(row["Open"].iloc[0]), 2),
            high=round(float(row["High"].iloc[0]), 2),
            low=round(float(row["Low"].iloc[0]), 2),
            close=round(float(row["Close"].iloc[0]), 2),
            volume=int(row["Volume"].iloc[0])
        )
    records = df.apply(parse_row, axis=1).tolist()
    return records

def validated_data(data: List[StockData]) -> pd.DataFrame:
    return pd.DataFrame([item.dict() for item in data])