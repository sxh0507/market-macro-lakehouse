WITH btc_cross AS (
  SELECT
    c.feature_date,
    c.product_id,
    c.log_return_1d,
    c.fill_policy,
    c.macro_features['FRED_FEDFUNDS'] AS fedfunds_value,
    c.macro_features['FRED_CPIAUCSL'] AS cpi_value
  FROM market_macro.gld_cross.dp_crypto_macro_features_1d AS c
  WHERE c.product_id = 'BTC-USD'
),
btc_dual_axis AS (
  SELECT
    feature_date,
    product_id,
    log_return_1d AS btc_log_return_1d,
    SUM(COALESCE(log_return_1d, 0.0)) OVER (
      ORDER BY feature_date
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS btc_cum_log_return_30d,
    EXP(SUM(COALESCE(log_return_1d, 0.0)) OVER (
      ORDER BY feature_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )) * 100.0 AS btc_price_index_100,
    fedfunds_value,
    cpi_value,
    fill_policy,
    false AS backtest_safe
  FROM btc_cross
)
SELECT
  feature_date,
  product_id,
  btc_log_return_1d,
  btc_cum_log_return_30d,
  btc_price_index_100,
  fedfunds_value,
  cpi_value,
  fill_policy,
  backtest_safe
FROM btc_dual_axis
ORDER BY feature_date;
