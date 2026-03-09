WITH latest_market_date AS (
  SELECT MAX(bar_date) AS latest_bar_date
  FROM market_macro.gld_market.dp_crypto_returns_1d
)
SELECT
  r.bar_date,
  r.product_id,
  r.base_asset,
  r.quote_currency,
  r.close,
  r.simple_return_1d,
  r.log_return_1d,
  v.volatility_7d,
  v.volatility_30d,
  v.volatility_90d
FROM market_macro.gld_market.dp_crypto_returns_1d AS r
LEFT JOIN market_macro.gld_market.dp_crypto_volatility_1d AS v
  ON r.product_id = v.product_id
 AND r.bar_date = v.bar_date
INNER JOIN latest_market_date AS d
  ON r.bar_date = d.latest_bar_date
ORDER BY r.product_id;
