WITH btc_volatility AS (
  SELECT
    v.bar_date AS observation_date,
    v.product_id,
    v.volatility_30d AS btc_volatility_30d
  FROM market_macro.gld_market.dp_crypto_volatility_1d AS v
  WHERE v.product_id = 'BTC-USD'
),
eurusd_base AS (
  SELECT
    m.observation_date,
    m.value AS eurusd_rate,
    LAG(m.value) OVER (ORDER BY m.observation_date) AS prev_eurusd_rate
  FROM market_macro.gld_macro.dp_macro_indicators AS m
  WHERE m.indicator_id = 'ECB_FX_REF_EUR_USD'
),
eurusd_returns AS (
  SELECT
    observation_date,
    eurusd_rate,
    CASE
      WHEN prev_eurusd_rate IS NULL OR prev_eurusd_rate <= 0 OR eurusd_rate <= 0 THEN NULL
      ELSE LN(eurusd_rate / prev_eurusd_rate)
    END AS eurusd_log_return_1d
  FROM eurusd_base
),
eurusd_volatility AS (
  SELECT
    observation_date,
    eurusd_rate,
    eurusd_log_return_1d,
    STDDEV_SAMP(eurusd_log_return_1d) OVER (
      ORDER BY observation_date
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS eurusd_volatility_30d
  FROM eurusd_returns
),
joined_volatility AS (
  SELECT
    b.observation_date,
    b.product_id,
    b.btc_volatility_30d,
    e.eurusd_rate,
    e.eurusd_log_return_1d,
    e.eurusd_volatility_30d
  FROM btc_volatility AS b
  INNER JOIN eurusd_volatility AS e
    ON b.observation_date = e.observation_date
),
normalized_volatility AS (
  SELECT
    observation_date,
    product_id,
    btc_volatility_30d,
    eurusd_rate,
    eurusd_log_return_1d,
    eurusd_volatility_30d,
    (btc_volatility_30d - AVG(btc_volatility_30d) OVER (
      ORDER BY observation_date
      ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    )) / NULLIF(STDDEV_SAMP(btc_volatility_30d) OVER (
      ORDER BY observation_date
      ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    ), 0.0) AS btc_volatility_30d_zscore_90d,
    (eurusd_volatility_30d - AVG(eurusd_volatility_30d) OVER (
      ORDER BY observation_date
      ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    )) / NULLIF(STDDEV_SAMP(eurusd_volatility_30d) OVER (
      ORDER BY observation_date
      ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    ), 0.0) AS eurusd_volatility_30d_zscore_90d
  FROM joined_volatility
)
SELECT
  observation_date,
  product_id,
  btc_volatility_30d,
  eurusd_rate,
  eurusd_log_return_1d,
  eurusd_volatility_30d,
  btc_volatility_30d_zscore_90d,
  eurusd_volatility_30d_zscore_90d
FROM normalized_volatility
ORDER BY observation_date;
