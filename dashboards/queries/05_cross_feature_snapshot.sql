WITH latest_feature_date AS (
  SELECT MAX(feature_date) AS latest_feature_date
  FROM market_macro.gld_cross.dp_crypto_macro_features_1d
)
SELECT
  c.feature_date,
  c.product_id,
  c.base_asset,
  c.quote_currency,
  c.simple_return_1d,
  c.log_return_1d,
  c.volatility_7d,
  c.volatility_30d,
  c.fill_policy,
  c.macro_features
FROM market_macro.gld_cross.dp_crypto_macro_features_1d AS c
INNER JOIN latest_feature_date AS d
  ON c.feature_date = d.latest_feature_date
ORDER BY c.product_id;
