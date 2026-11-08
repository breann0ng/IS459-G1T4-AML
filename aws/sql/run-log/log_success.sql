UPDATE analytics.pipeline_runs
SET status = 'SUCCEEDED', finished_at = current_timestamp
WHERE run_id = '{run_id}';
