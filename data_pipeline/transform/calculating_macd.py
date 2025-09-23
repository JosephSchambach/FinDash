from google.cloud import bigquery
import pandas as pd

client = bigquery.Client()
query = """
SELECT
    date,
    open,
    high,
    low,
    close,
    volume
FROM
    `your_project.your_dataset.your_table`
ORDER BY
    date
"""
df = client.query(query).to_dataframe()

import talib

# Example: Calculate RSI
df['RSI'] = talib.RSI(df['close'], timeperiod=14)

macd, macdsignal, macdhist = talib.MACD(df['close'], fastperiod=12, slowperiod=26, signalperiod=9)
df['MACD'] = macd
df['MACD_Signal'] = macdsignal
df['MACD_Hist'] = macdhist

table_id = "your_project.your_dataset.your_results_table"
client.load_table_from_dataframe(df, table_id).result()