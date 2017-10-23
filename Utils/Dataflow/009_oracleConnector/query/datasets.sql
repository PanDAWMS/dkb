-- select input and output datasets for all tasks
-- for time period
-- P.S. mininum value of t_production_task.timestamp is 2014-03-04 14:53:51.33390
SELECT
  t.taskid,
  jd.datasetname,
  jd.type
FROM
  t_production_task t
  JOIN
  ATLAS_PANDA.jedi_datasets jd
  ON jd.jeditaskid = t.taskid
WHERE
  jd.type IN ('input', 'output') AND
  t.timestamp > to_date('%s', 'dd-mm-yyyy hh24:mi:ss') AND
  t.timestamp <= to_date('%s', 'dd-mm-yyyy hh24:mi:ss')
ORDER BY t.taskid;