WITH latest_market_date AS (
  SELECT MAX(bar_date) AS latest_bar_date
  FROM market_macro.gld_market.dp_crypto_returns_1d
),
daily_returns AS (
  SELECT
    r.bar_date,
    r.product_id,
    r.base_asset,
    r.quote_currency,
    r.close,
    r.simple_return_1d,
    r.log_return_1d,
    ROUND(r.simple_return_1d * 100, 2) AS simple_return_1d_pct
  FROM market_macro.gld_market.dp_crypto_returns_1d AS r
  INNER JOIN latest_market_date AS d
    ON r.bar_date = d.latest_bar_date
  WHERE r.simple_return_1d IS NOT NULL
),
gainers AS (
  SELECT
    bar_date,
    product_id,
    base_asset,
    quote_currency,
    close,
    simple_return_1d,
    log_return_1d,
    simple_return_1d_pct,
    ROW_NUMBER() OVER (
      ORDER BY simple_return_1d DESC, product_id
    ) AS rank_in_group
  FROM daily_returns
),
losers AS (
  SELECT
    bar_date,
    product_id,
    base_asset,
    quote_currency,
    close,
    simple_return_1d,
    log_return_1d,
    simple_return_1d_pct,
    ROW_NUMBER() OVER (
      ORDER BY simple_return_1d ASC, product_id
    ) AS rank_in_group
  FROM daily_returns
),
selected_movers AS (
  SELECT
    bar_date,
    CASE
      WHEN rank_in_group = 1 THEN 'Top Gainer'
      ELSE '2nd Gainer'
    END AS card_title,
    'gainer' AS mover_group,
    CASE
      WHEN rank_in_group = 1 THEN 1
      ELSE 2
    END AS sort_order,
    product_id,
    base_asset,
    quote_currency,
    close,
    simple_return_1d,
    log_return_1d,
    simple_return_1d_pct
  FROM gainers
  WHERE rank_in_group <= 2

  UNION ALL

  SELECT
    bar_date,
    CASE
      WHEN rank_in_group = 1 THEN 'Top Loser'
      ELSE '2nd Loser'
    END AS card_title,
    'loser' AS mover_group,
    CASE
      WHEN rank_in_group = 1 THEN 3
      ELSE 4
    END AS sort_order,
    product_id,
    base_asset,
    quote_currency,
    close,
    simple_return_1d,
    log_return_1d,
    simple_return_1d_pct
  FROM losers
  WHERE rank_in_group <= 2
)
SELECT
  bar_date,
  card_title,
  mover_group,
  sort_order,
  product_id,
  base_asset,
  quote_currency,
  close,
  simple_return_1d,
  log_return_1d,
  simple_return_1d_pct,
  CASE
    WHEN simple_return_1d > 0 THEN 'positive'
    WHEN simple_return_1d < 0 THEN 'negative'
    ELSE 'flat'
  END AS sentiment,
  FORMAT_STRING('%s %+.2f%%', base_asset, simple_return_1d_pct) AS kpi_label
FROM selected_movers
ORDER BY sort_order;
