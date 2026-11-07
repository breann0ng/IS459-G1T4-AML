-- aml-daily: one row per run. run_id = the Step Functions execution name.
INSERT INTO analytics.pipeline_runs (run_id, pipeline, dataset, date_from, date_to, status, started_at)
VALUES ('{run_id}', 'aml-daily', '{dataset}', DATE '{date}', DATE '{date}', 'RUNNING', current_timestamp);
