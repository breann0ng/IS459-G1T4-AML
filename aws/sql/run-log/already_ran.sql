-- aml-daily, first step: is this day already running or done? 0 = go ahead.
-- Step Functions fills {dataset} and {date}.
SELECT count(*) AS n
FROM analytics.pipeline_runs
WHERE pipeline = 'aml-daily' AND dataset = '{dataset}' AND date_to = DATE '{date}'
  AND status IN ('RUNNING', 'SUCCEEDED');
