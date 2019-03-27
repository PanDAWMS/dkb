-- Select all tasks for specified period of time
-- Query tables:
------------------------------------------------
-- ATLAS_DEFT.t_production_task
-- ATLAS_DEFT.t_production_step
-- ATLAS_DEFT.t_step_template
-- ATLAS_DEFT.t_ht_to_task
-- ATLAS_DEFT.t_hashtag
-- ATLAS_PANDA.jedi_datasets
--
-- All fields:
-- architecture, campaign, cloud, conditions_tags, core_count, description, end_time,
-- energy_gev, evgen_job_opts, geometry_version, hashtag_list, job_config, physics_list, processed_events,
-- phys_group, project, pr_id, requested_events, run_number, site, start_time, step_name, status, subcampaign,
-- taskid, taskname, task_timestamp,  ticket_id, trans_home, trans_path, trans_uses, trigger_config, user_name, vo,
-- n_files_per_job, n_events_per_job, n_files_to_be_used,

-- RESTRICTIONS:
-- 1. taskID must be more than 4 000 000 OR from the date > 12-03-2014
-- 2. we collecting only PRODUCTION tasks OR only ANALYSIS tasks
--    ('pr_id > 300' or 'pr_id = 300')
  SELECT DISTINCT
    t.taskid,
    TO_CHAR(t.timestamp, 'dd-mm-yyyy hh24:mi:ss')           AS task_timestamp
  FROM
    ATLAS_DEFT.t_production_task t
  WHERE
    t.timestamp > :start_date AND
    t.timestamp <= :end_date AND
    t.pr_id %(production_or_analysis_cond)s 300
