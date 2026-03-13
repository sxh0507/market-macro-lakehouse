WITH btc_features AS (
  SELECT
    c.feature_date,
    c.product_id,
    c.log_return_1d AS crypto_log_return_1d,
    c.fill_policy,
    c.macro_features
  FROM market_macro.gld_cross.dp_crypto_macro_features_1d AS c
  WHERE c.product_id = 'BTC-USD'
),
macro_long AS (
  SELECT
    f.feature_date,
    f.product_id,
    f.crypto_log_return_1d,
    f.fill_policy,
    kv.key AS indicator_id,
    kv.value AS macro_value
  FROM btc_features AS f
  LATERAL VIEW EXPLODE(MAP_ENTRIES(f.macro_features)) exploded AS kv
  WHERE kv.key IN (
    'ECB_FX_REF_EUR_USD',
    'FRED_FEDFUNDS',
    'FRED_CPIAUCSL'
  )
),
macro_labeled AS (
  SELECT
    feature_date,
    product_id,
    crypto_log_return_1d,
    fill_policy,
    indicator_id,
    CASE
      WHEN indicator_id = 'ECB_FX_REF_EUR_USD' THEN 'fx_ref_rate'
      WHEN indicator_id = 'FRED_FEDFUNDS' THEN 'policy_rate'
      WHEN indicator_id = 'FRED_CPIAUCSL' THEN 'inflation'
      ELSE 'other'
    END AS indicator_group,
    macro_value
  FROM macro_long
  WHERE crypto_log_return_1d IS NOT NULL
    AND macro_value IS NOT NULL
),
rolling_stats AS (
  SELECT
    feature_date,
    product_id,
    indicator_id,
    indicator_group,
    macro_value,
    crypto_log_return_1d,
    fill_policy,
    COUNT(*) OVER w AS window_count,
    SUM(crypto_log_return_1d) OVER w AS sum_x,
    SUM(macro_value) OVER w AS sum_y,
    SUM(crypto_log_return_1d * crypto_log_return_1d) OVER w AS sum_x2,
    SUM(macro_value * macro_value) OVER w AS sum_y2,
    SUM(crypto_log_return_1d * macro_value) OVER w AS sum_xy
  FROM macro_labeled
  WINDOW w AS (
    PARTITION BY product_id, indicator_id
    ORDER BY feature_date
    ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
  )
)
SELECT
  feature_date,
  product_id,
  indicator_id,
  indicator_group,
  macro_value,
  crypto_log_return_1d,
  CASE
    WHEN window_count < 30 THEN NULL
    WHEN ((window_count * sum_x2 - sum_x * sum_x) <= 0)
      OR ((window_count * sum_y2 - sum_y * sum_y) <= 0) THEN NULL
    ELSE
      (window_count * sum_xy - sum_x * sum_y)
      / SQRT(
        (window_count * sum_x2 - sum_x * sum_x)
        * (window_count * sum_y2 - sum_y * sum_y)
      )
  END AS rolling_corr_90d,
  fill_policy,
  false AS backtest_safe
FROM rolling_stats
ORDER BY indicator_id, feature_date;
