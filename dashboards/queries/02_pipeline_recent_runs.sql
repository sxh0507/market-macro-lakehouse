SELECT
  run_id,
  pipeline_name,
  layer,
  source_name,
  target_table,
  status,
  rows_read,
  rows_written,
  started_at,
  completed_at,
  error_message
FROM market_macro.obs.obs_pipeline_run_log
ORDER BY started_at DESC
LIMIT 100;
