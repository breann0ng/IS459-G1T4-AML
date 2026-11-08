-- The last day aml-daily finished, per dataset (what to replay next = the day after).
SELECT dataset, max(date_to) AS last_day
FROM analytics.pipeline_runs
WHERE pipeline = 'aml-daily' AND status = 'SUCCEEDED'
GROUP BY dataset;
