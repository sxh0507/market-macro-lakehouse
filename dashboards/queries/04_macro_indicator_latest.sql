WITH latest_macro_date AS (
  SELECT
    indicator_id,
    MAX(observation_date) AS latest_observation_date
  FROM market_macro.gld_macro.dp_macro_indicators
  GROUP BY indicator_id
)
SELECT
  m.source_system,
  m.indicator_id,
  m.indicator_group,
  m.observation_date,
  m.value,
  m.unit,
  m.frequency,
  m.is_official,
  m.derivation_method
FROM market_macro.gld_macro.dp_macro_indicators AS m
INNER JOIN latest_macro_date AS d
  ON m.indicator_id = d.indicator_id
 AND m.observation_date = d.latest_observation_date
ORDER BY
  m.source_system,
  m.indicator_id;
