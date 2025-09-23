-- BigQuery Create Bronze
CREATE EXTERNAL TABLE `findash.bronze.stock_data`
(
  symbol STRING,
  timestamp TIMESTAMP,
  open FLOAT64,
  close FLOAT64,
  high FLOAT64,
  low FLOAT64,
  volume INT64
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://your-bucket/path/to/parquet/files/*.parquet'],
  max_staleness = INTERVAL 1 HOUR
);

-- BigQuery Create Silver
CREATE TABLE `findash.silver.stock_data`
(
  symbol STRING,
  open FLOAT64,
  close FLOAT64,
  high FLOAT64,
  low FLOAT64,
  volume INT64,
  id STRING,
  recorded_time TIMESTAMP,
  last_updated TIMESTAMP
)
PARTITION BY DATE(recorded_time)
OPTIONS (
  description = 'Silver table for stock data with hashed id and timestamps'
);