from google.cloud import bigquery
import pandas as pd
import talib
import numpy as np

# Initialize BigQuery client
client = bigquery.Client()

# SQL query with corrected syntax for BigQuery
sql_query = """
WITH sma AS (
  SELECT 
    symbol, 
    recorded_time, 
    close,
    volume,
    AVG(close) OVER (
      PARTITION BY symbol 
      ORDER BY recorded_time 
      ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS twenty_mvng_avg,
    AVG(close) OVER (
      PARTITION BY symbol 
      ORDER BY recorded_time 
      ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
    ) AS fifty_mvng_avg,
    AVG(close) OVER (
      PARTITION BY symbol 
      ORDER BY recorded_time 
      ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
    ) AS two_hundred_mvng_avg
  FROM silver.stock_data
), 
std_dev AS (
  SELECT *,
    STDDEV(close) OVER (
      PARTITION BY symbol 
      ORDER BY recorded_time 
      ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS std_dev
  FROM sma
), 
bollinger AS (
  SELECT *,
    twenty_mvng_avg + (2 * std_dev) AS upper_bound,
    twenty_mvng_avg - (2 * std_dev) AS lower_bound
  FROM std_dev
), 
diff_data AS (
  SELECT *, 
    close - LAG(close, 1) OVER (PARTITION BY symbol ORDER BY recorded_time) AS close_diff
  FROM bollinger
), 
agg_data AS (
  SELECT *, 
    CASE WHEN close_diff >= 0 THEN close_diff ELSE 0 END AS gain,
    CASE WHEN close_diff < 0 THEN ABS(close_diff) ELSE 0 END AS loss
  FROM diff_data
), 
rsi_sma AS (
  SELECT *,
    AVG(gain) OVER (
      PARTITION BY symbol 
      ORDER BY recorded_time 
      ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ) AS avg_gain,
    AVG(loss) OVER (
      PARTITION BY symbol 
      ORDER BY recorded_time 
      ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    ) AS avg_loss
  FROM agg_data
), 
sql_rsi AS (
  SELECT *,
    CASE 
      WHEN avg_loss = 0 THEN 100 
      ELSE 100 - (100 / (1 + (avg_gain / avg_loss))) 
    END AS sql_rsi
  FROM rsi_sma
),
price_vol AS (
  SELECT *,
    close * volume AS total
  FROM sql_rsi
),
agg_price_vol AS (
  SELECT *,
    SUM(total) OVER (PARTITION BY symbol ORDER BY recorded_time ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS total_price,
    SUM(volume) OVER (PARTITION BY symbol ORDER BY recorded_time ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS total_volume
  FROM price_vol
), sql_vwap AS (
  SELECT *,
    total_price / total_volume AS vwap
  FROM agg_price_vol
)
SELECT * 
FROM sql_vwap
ORDER BY symbol, recorded_time;
"""

# Execute the SQL query and load into DataFrame
print("Executing SQL query...")
df = client.query(sql_query).to_dataframe()

print(f"Retrieved {len(df)} rows from BigQuery")

# Add TA-Lib indicators
print("Calculating TA-Lib indicators...")

# Initialize new columns
df['talib_rsi'] = np.nan
df['macd'] = np.nan
df['macd_signal'] = np.nan
df['macd_hist'] = np.nan

# Calculate indicators for each symbol separately
for symbol in df['symbol'].unique():
    print(f"Processing {symbol}...")
    symbol_mask = df['symbol'] == symbol
    symbol_data = df[symbol_mask].copy().sort_values('recorded_time')
    
    # Calculate TA-Lib RSI
    if len(symbol_data) >= 14:
        symbol_data['talib_rsi'] = talib.RSI(symbol_data['close'].values, timeperiod=14)
    
    # Calculate MACD
    if len(symbol_data) >= 26:
        macd, macd_signal, macd_hist = talib.MACD(
            symbol_data['close'].values, 
            fastperiod=12, 
            slowperiod=26, 
            signalperiod=9
        )
        symbol_data['macd'] = macd
        symbol_data['macd_signal'] = macd_signal
        symbol_data['macd_hist'] = macd_hist
    
    # Update the main dataframe
    df.loc[symbol_mask, ['talib_rsi', 'macd', 'macd_signal', 'macd_hist']] = symbol_data[['talib_rsi', 'macd', 'macd_signal', 'macd_hist']].values

# Optional: Compare SQL RSI vs TA-Lib RSI
df['rsi_difference'] = df['sql_rsi'] - df['talib_rsi']

# Prepare final dataset for gold layer
final_columns = [
    'symbol', 'recorded_time', 'close',
    'twenty_mvng_avg', 'fifty_mvng_avg', 'two_hundred_mvng_avg',
    'upper_bound', 'lower_bound',  # Bollinger Bands
    'sql_rsi', 'talib_rsi',  # RSI from both methods
    'macd', 'macd_signal', 'macd_hist'  # MACD indicators
]

gold_df = df[final_columns].copy()

# Create the gold table
print("Writing to gold.stock_data_w_indicators...")

# Configure the load job
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",  # Overwrite existing data
    schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    autodetect=True
)

# Load to BigQuery gold layer
table_id = "gold.stock_data_w_indicators"
job = client.load_table_from_dataframe(gold_df, table_id, job_config=job_config)
result = job.result()  # Wait for the job to complete

print(f"Successfully loaded {len(gold_df)} rows to {table_id}")

# Display sample results
print("\nSample of final data:")
print(gold_df.head(10))