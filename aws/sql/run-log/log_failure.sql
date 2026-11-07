-- {error} is the Step Functions error name (e.g. States.TaskFailed); the full cause is in the execution history.
UPDATE analytics.pipeline_runs
SET status = 'FAILED', finished_at = current_timestamp, error = '{error}'
WHERE run_id = '{run_id}';
