SELECT
  pipeline_name,
  source_name,
  target_table,
  watermark_value,
  watermark_type,
  status,
  last_success_at,
  updated_at
FROM market_macro.obs.obs_ingestion_state
ORDER BY
  source_name,
  pipeline_name;
