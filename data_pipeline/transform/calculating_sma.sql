select symbol, recorded_time, close,
  AVG(close) OVER (PARTITION BY symbol ORDER BY recorded_time ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS fifty_mvng_avg,
  AVG(close) OVER (PARTITION BY symbol ORDER BY recorded_time ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS two_hundred_mvng_avg
FROM silver.stock_data