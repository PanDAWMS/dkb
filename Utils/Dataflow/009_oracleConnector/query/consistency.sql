-- Select taskid and timestamp of all tasks for specified period of time
  SELECT DISTINCT
    t.taskid,
    TO_CHAR(t.timestamp, 'dd-mm-yyyy hh24:mi:ss')           AS task_timestamp
  FROM
    ATLAS_DEFT.t_production_task t
  WHERE
    t.timestamp > :start_date AND
    t.timestamp <= :end_date AND
    t.pr_id %(production_or_analysis_cond)s 300
