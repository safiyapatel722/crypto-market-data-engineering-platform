MERGE `staging_market_data.crypto_price_timeseries` T
USING (

  WITH extracted AS (

    SELECT
      r.coin_id,
      r.ingestion_time,
      JSON_QUERY_ARRAY(r.payload, "$.prices") AS prices,
      JSON_QUERY_ARRAY(r.payload, "$.market_caps") AS market_caps,
      JSON_QUERY_ARRAY(r.payload, "$.total_volumes") AS total_volumes
    FROM `raw_market_data.crypto_api_responses` r
    WHERE DATE(r.ingestion_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)

  )

  SELECT * EXCEPT(rn)
  FROM (

    SELECT
      coin_id,
      TIMESTAMP_MILLIS(CAST(JSON_VALUE(p, "$[0]") AS INT64)) AS event_time,
      CAST(JSON_VALUE(p, "$[1]") AS FLOAT64) AS price_usd,
      CAST(JSON_VALUE(m, "$[1]") AS FLOAT64) AS market_cap,
      CAST(JSON_VALUE(v, "$[1]") AS FLOAT64) AS total_volume,
      ingestion_time,

      ROW_NUMBER() OVER (
        PARTITION BY coin_id,
        TIMESTAMP_MILLIS(CAST(JSON_VALUE(p, "$[0]") AS INT64))
        ORDER BY ingestion_time DESC
      ) AS rn

    FROM extracted,
    UNNEST(prices) AS p WITH OFFSET idx
    JOIN UNNEST(market_caps) AS m WITH OFFSET idx2
      ON idx = idx2
    JOIN UNNEST(total_volumes) AS v WITH OFFSET idx3
      ON idx = idx3

  )
  WHERE rn = 1

) S

ON T.coin_id = S.coin_id
AND T.event_time = S.event_time

WHEN MATCHED THEN
  UPDATE SET
    price_usd = S.price_usd,
    market_cap = S.market_cap,
    total_volume = S.total_volume,
    ingestion_time = S.ingestion_time

WHEN NOT MATCHED THEN
  INSERT (
    coin_id,
    event_time,
    price_usd,
    market_cap,
    total_volume,
    ingestion_time
  )
  VALUES (
    S.coin_id,
    S.event_time,
    S.price_usd,
    S.market_cap,
    S.total_volume,
    S.ingestion_time
  );