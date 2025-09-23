WITH sma AS (
  SELECT symbol, recorded_time, close,
    AVG(close) OVER (PARTITION BY symbol ORDER BY recorded_time ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS mvng_avg
  FROM silver.stock_data
), sma_stdv AS (
  SELECT *,
    2 * STDDEV(close) OVER (PARTITION BY symbol ORDER BY recorded_time ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS std_dev_w_mltplr
  FROM sma
)
SELECT *,
  mvng_avg + std_dev_w_mltplr AS upper_bound,
  mvng_avg - std_dev_w_mltplr AS lower_bound
FROM sma_stdv