-- BRONZE TO SILVER INGESTION WORKFLOW

-- # supabase code
-- insert into silver.stock_data
-- select
--   sd.symbol,
--   sd.open,
--   sd.close,
--   sd.high,
--   sd.low,
--   sd.volume,
--   sd.symbol || ' ' || sd.timestamp::TEXT as id, -- hash this in GCP
--   cast(sd."timestamp" as timestamp) as recorded_time,
--   current_timestamp as last_updated
-- from
--   bronze.stock_data as sd
-- on conflict (sd.id) do nothing;

-- # BigQuery code
MERGE INTO `findash.silver.stock_data` AS target
USING (
  SELECT
    symbol,
    open,
    close,
    high,
    low,
    volume,
    TO_HEX(SHA256(symbol || ' ' || CAST(timestamp AS STRING))) AS id,
    CAST(timestamp AS TIMESTAMP) AS recorded_time,
    CURRENT_TIMESTAMP() AS last_updated
  FROM
    `findash.bronze.stock_data`
) AS source
ON target.id = source.id
WHEN NOT MATCHED THEN
  INSERT (symbol, open, close, high, low, volume, id, recorded_time, last_updated)
  VALUES (symbol, open, close, high, low, volume, id, recorded_time, last_updated);