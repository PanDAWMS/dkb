-- select output datasets for all tasks
-- for time period
-- P.S. mininum value of t_production_task.timestamp is 2014-03-04 14:53:51.33390
SELECT
  t.taskid,
  jd.datasetname as output_datasets
FROM
  t_production_task t
  LEFT JOIN
  ATLAS_PANDA.jedi_datasets jd
  ON jd.jeditaskid = t.taskid
WHERE
  jd.type IN ('output')
  AND t.timestamp > to_date('01-01-2016 00:00:00', 'dd-mm-yyyy hh24:mi:ss')
  AND t.timestamp <= to_date('01-02-2016 01:00:00', 'dd-mm-yyyy hh24:mi:ss');