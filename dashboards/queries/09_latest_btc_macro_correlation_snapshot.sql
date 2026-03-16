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
      WHEN indicator_id = 'ECB_FX_REF_EUR_USD' THEN 'EUR/USD'
      WHEN indicator_id = 'FRED_FEDFUNDS' THEN 'Fed Funds'
      WHEN indicator_id = 'FRED_CPIAUCSL' THEN 'US CPI'
      ELSE indicator_id
    END AS indicator_label,
    CASE
      WHEN indicator_id = 'ECB_FX_REF_EUR_USD' THEN 'fx_ref_rate'
      WHEN indicator_id = 'FRED_FEDFUNDS' THEN 'policy_rate'
      WHEN indicator_id = 'FRED_CPIAUCSL' THEN 'inflation'
      ELSE 'other'
    END AS indicator_group,
    CASE
      WHEN indicator_id = 'ECB_FX_REF_EUR_USD' THEN 1
      WHEN indicator_id = 'FRED_FEDFUNDS' THEN 2
      WHEN indicator_id = 'FRED_CPIAUCSL' THEN 3
      ELSE 99
    END AS sort_order,
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
    indicator_label,
    indicator_group,
    sort_order,
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
),
rolling_corr AS (
  SELECT
    feature_date,
    product_id,
    indicator_id,
    indicator_label,
    indicator_group,
    sort_order,
    fill_policy,
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
    END AS rolling_corr_90d
  FROM rolling_stats
),
latest_snapshot AS (
  SELECT
    indicator_id,
    MAX(feature_date) AS latest_feature_date
  FROM rolling_corr
  WHERE rolling_corr_90d IS NOT NULL
  GROUP BY indicator_id
)
SELECT
  c.latest_feature_date AS feature_date,
  r.product_id,
  r.indicator_id,
  r.indicator_label,
  r.indicator_group,
  r.sort_order,
  r.rolling_corr_90d,
  r.fill_policy,
  false AS backtest_safe
FROM rolling_corr AS r
INNER JOIN latest_snapshot AS c
  ON r.indicator_id = c.indicator_id
 AND r.feature_date = c.latest_feature_date
ORDER BY r.sort_order, r.indicator_label;
