-- select input and output datasets for all tasks
-- for time period
-- P.S. mininum value of t_production_task.timestamp is 12-03-2014 14:53:51.33390
SELECT
  t.taskid,
  jd.datasetname,
  jd.type
FROM
  t_production_task t
  JOIN t_prodmanager_request r
  ON t.pr_id = r.pr_id
  JOIN
  ATLAS_PANDA.jedi_datasets jd
  ON jd.jeditaskid = t.taskid
  LEFT JOIN t_task tt
  ON t.taskid = tt.taskid
WHERE
  jd.type IN ('output') AND
      t.timestamp > to_date('%s', 'dd-mm-yyyy hh24:mi:ss') AND
      t.timestamp <= to_date('%s', 'dd-mm-yyyy hh24:mi:ss') AND
      t.pr_id > 300
ORDER BY t.taskid;