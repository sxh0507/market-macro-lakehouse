WITH latest_signals AS (
  SELECT
    indicator_id,
    observation_date,
    value,
    unit,
    frequency,
    ROW_NUMBER() OVER (
      PARTITION BY indicator_id
      ORDER BY observation_date DESC
    ) AS rn
  FROM market_macro.gld_macro.dp_macro_indicators
  WHERE indicator_id IN (
    'FRED_CPIAUCSL',
    'FRED_FEDFUNDS',
    'ECB_FX_REF_EUR_USD'
  )
),
eurusd_latest AS (
  SELECT
    observation_date,
    value,
    LAG(value) OVER (
      ORDER BY observation_date
    ) AS previous_value,
    ROW_NUMBER() OVER (
      ORDER BY observation_date DESC
    ) AS rn
  FROM market_macro.gld_macro.dp_macro_indicators
  WHERE indicator_id = 'ECB_FX_REF_EUR_USD'
),
macro_cards AS (
  SELECT
    1 AS sort_order,
    'Latest US CPI' AS card_title,
    'US CPI' AS signal_name,
    observation_date,
    value AS signal_value,
    unit,
    frequency,
    'fred' AS source_system,
    FORMAT_NUMBER(value, 1) AS signal_value_display
  FROM latest_signals
  WHERE indicator_id = 'FRED_CPIAUCSL'
    AND rn = 1

  UNION ALL

  SELECT
    2 AS sort_order,
    'Latest Fed Funds' AS card_title,
    'Fed Funds' AS signal_name,
    observation_date,
    value AS signal_value,
    unit,
    frequency,
    'fred' AS source_system,
    FORMAT_NUMBER(value, 2) AS signal_value_display
  FROM latest_signals
  WHERE indicator_id = 'FRED_FEDFUNDS'
    AND rn = 1

  UNION ALL

  SELECT
    3 AS sort_order,
    'Latest EUR/USD Move' AS card_title,
    'EUR/USD Daily Change' AS signal_name,
    observation_date,
    ((value - previous_value) / previous_value) * 100 AS signal_value,
    '% change' AS unit,
    'D' AS frequency,
    'ecb' AS source_system,
    FORMAT_STRING('%+.2f%%', ((value - previous_value) / previous_value) * 100) AS signal_value_display
  FROM eurusd_latest
  WHERE rn = 1
    AND previous_value IS NOT NULL
)
SELECT
  sort_order,
  card_title,
  signal_name,
  observation_date,
  signal_value,
  signal_value_display,
  unit,
  frequency,
  source_system
FROM macro_cards
ORDER BY sort_order;
