WITH lag_data AS (
  SELECT
    symbol,
    recorded_time,
    close,
    LAG(close, 1) OVER (PARTITION BY symbol ORDER BY recorded_time) AS prev_close
  FROM `silver.stock_data`
),
diff_data AS (
  SELECT
    *,
    close - prev_close AS close_diff
  FROM lag_data
),
agg_data AS (
  SELECT
    *,
    CASE WHEN close_diff >= 0 THEN close_diff ELSE 0 END AS gain,
    CASE WHEN close_diff < 0 THEN ABS(close_diff) ELSE 0 END AS loss
  FROM diff_data
),
rsi_sma AS (
  SELECT
    *,
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
)
SELECT
  *,
  CASE WHEN avg_loss = 0 THEN 100
       ELSE 100 - (100 / (1 + (avg_gain / avg_loss)))
  END AS rsi
FROM rsi_sma
ORDER BY recorded_time;
