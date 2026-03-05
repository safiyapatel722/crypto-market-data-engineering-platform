MERGE `curated_market_data.crypto_daily_metrics` T
USING (

  WITH daily_agg AS (
    SELECT
      coin_id,
      DATE(event_time) AS date,
      AVG(price_usd) AS avg_price,
      MAX(price_usd) AS max_price,
      MIN(price_usd) AS min_price,
      SUM(total_volume) AS total_volume,
      AVG(market_cap) AS avg_market_cap
    FROM `staging_market_data.crypto_price_timeseries`
    GROUP BY coin_id, DATE(event_time)
  ),

  with_lag AS (
    SELECT
      *,
      LAG(avg_price) OVER (
        PARTITION BY coin_id
        ORDER BY date
      ) AS prev_avg_price
    FROM daily_agg
  )

  SELECT
    coin_id,
    date,
    avg_price,
    max_price,
    min_price,
    total_volume,
    avg_market_cap,
    SAFE_DIVIDE(avg_price - prev_avg_price, prev_avg_price) * 100
      AS daily_price_change_pct
  FROM with_lag

) S

ON T.coin_id = S.coin_id
AND T.date = S.date

WHEN MATCHED THEN UPDATE SET
  avg_price = S.avg_price,
  max_price = S.max_price,
  min_price = S.min_price,
  total_volume = S.total_volume,
  avg_market_cap = S.avg_market_cap,
  daily_price_change_pct = S.daily_price_change_pct

WHEN NOT MATCHED THEN INSERT (
  coin_id,
  date,
  avg_price,
  max_price,
  min_price,
  total_volume,
  avg_market_cap,
  daily_price_change_pct
)
VALUES (
  S.coin_id,
  S.date,
  S.avg_price,
  S.max_price,
  S.min_price,
  S.total_volume,
  S.avg_market_cap,
  S.daily_price_change_pct
);