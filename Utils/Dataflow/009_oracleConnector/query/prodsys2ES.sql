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
-- taskid, taskname, task_timestamp,  ticket_id, trans_home, trans_path, trans_uses, trigger_config, user_name, vo

-- RESTRICTIONS:
-- 1. taskID must be more than 4 000 000 OR from the date > 12-03-2014
-- 2. we collecting only PRODUCTION tasks
with tasks as (
    SELECT
      t.campaign,
      t.taskid,
      t.step_id,
      t.taskname,
      TO_CHAR(t.timestamp, 'dd-mm-yyyy hh24:mi:ss')           AS task_timestamp,
      NVL(TO_CHAR(t.start_time, 'dd-mm-yyyy hh24:mi:ss'), '') AS start_time,
      NVL(TO_CHAR(t.endtime, 'dd-mm-yyyy hh24:mi:ss'), '')    AS end_time,
      t.subcampaign,
      t.project,
      t.phys_group,
      t.status,
      t.pr_id,
      t.username as user_name,
      t.primary_input,
      t.ctag,
      t.output_formats,
      s_t.step_name,
      r.description,
      r.energy_gev,
      LISTAGG(hashtag.hashtag, ', ')
        WITHIN GROUP (
          ORDER BY t.taskid) AS hashtag_list
    FROM
      ATLAS_DEFT.t_production_task t
      JOIN ATLAS_DEFT.t_prodmanager_request r
        ON t.pr_id = r.pr_id
      JOIN ATLAS_DEFT.t_production_step s
        ON t.step_id = s.step_id
      JOIN ATLAS_DEFT.t_step_template s_t
        ON s.step_t_id = s_t.step_t_id
      LEFT JOIN ATLAS_DEFT.t_ht_to_task ht_t
        ON t.taskid = ht_t.taskid
      LEFT JOIN ATLAS_DEFT.t_hashtag hashtag
        ON hashtag.ht_id = ht_t.ht_id
    WHERE
      t.timestamp > to_date('%s', 'dd-mm-yyyy hh24:mi:ss') AND
      t.timestamp <= to_date('%s', 'dd-mm-yyyy hh24:mi:ss') AND
      t.pr_id > 300
    GROUP BY
        t.campaign,
        t.taskid,
        t.step_id,
        t.taskname,
        TO_CHAR(t.timestamp, 'dd-mm-yyyy hh24:mi:ss'),
        NVL(TO_CHAR(t.start_time, 'dd-mm-yyyy hh24:mi:ss'), ''),
        NVL(TO_CHAR(t.endtime, 'dd-mm-yyyy hh24:mi:ss'), ''),
        t.subcampaign,
        t.project,
        t.phys_group,
        t.status,
        t.pr_id,
        t.primary_input,
        t.ctag,
        t.output_formats,
        t.username,
        s_t.step_name,
        r.description,
        r.energy_gev),
  tasks_t_task as (
      SELECT
        t.campaign,
        t.subcampaign,
        t.phys_group,
        t.project,
        t.pr_id,
        t.step_name,
        t.status,
        t.taskid,
        t.taskname,
        t.task_timestamp,
        t.start_time,
        t.end_time,
        t.hashtag_list,
        t.description,
        t.energy_gev,
        t.user_name,
        t.primary_input,
        t.ctag,
        t.output_formats,
        to_char(NVL(substr(regexp_substr(tt.jedi_task_parameters, '"architecture": "(.[^",])+'),
                           regexp_instr(
                               regexp_substr(tt.jedi_task_parameters, '"architecture": "(.[^",])+'),
                               '(": ")+',
                               1,
                               1,
                               1
                           )
                    ), '')) AS architecture,
        to_char(NVL(substr(regexp_substr(tt.jedi_task_parameters, '"coreCount": [0-9\.]+'),
                           regexp_instr(
                               regexp_substr(tt.jedi_task_parameters, '"coreCount": [0-9\.]+'),
                               '(": )+',
                               1,
                               1,
                               1
                           )
                    ), '')) AS core_count,
        to_char(NVL(substr(regexp_substr(tt.jedi_task_parameters, '"\-\-conditionsTag \\"default:[a-zA-Z0-9_\-]+[^\""]'),
                           regexp_instr(
                               regexp_substr(tt.jedi_task_parameters,
                                             '"\-\-conditionsTag \\"default:[a-zA-Z0-9_\-]+[^\""]'),
                               '(:)+',
                               1,
                               1,
                               1
                           )
                    ), '')) AS conditions_tags,
        to_char(NVL(substr(regexp_substr(tt.jedi_task_parameters, '"\-\-geometryVersion=\\"default:[a-zA-Z0-9_\-]+[^\""]'),
                           regexp_instr(
                               regexp_substr(tt.jedi_task_parameters,
                                             '"\-\-geometryVersion=\\"default:[a-zA-Z0-9_\-]+[^\""]'),
                               '(:)+',
                               1,
                               1,
                               1
                           )
                    ), '')) AS geometry_version,
        to_char(NVL(substr(regexp_substr(tt.jedi_task_parameters, '"ticketID": "(.[^",])+'),
                           regexp_instr(
                               regexp_substr(tt.jedi_task_parameters, '"ticketID": "(.[^",])+'),
                               '(": ")+',
                               1,
                               1,
                               1
                           )
                    ), '')) AS ticket_id,
        to_char(NVL(substr(
          regexp_substr(tt.jedi_task_parameters, '"transHome": "[a-zA-Z0-9_\.\-]+[^"]'),
            regexp_instr(
                regexp_substr(tt.jedi_task_parameters, '"transHome": "[a-zA-Z0-9_\.\-]+[^"]'),
                '(": ")+',
                1,
                1,
                1
            )
          ), '')) as trans_home,
        to_char(NVL(substr(regexp_substr(tt.jedi_task_parameters, '"transPath": "(.[^",])+'),
                           regexp_instr(
                               regexp_substr(tt.jedi_task_parameters, '"transPath": "(.[^",])+'),
                               '(": ")+',
                               1,
                               1,
                               1
                           )
                    ), '')) AS trans_path,
        to_char(NVL(substr(regexp_substr(tt.jedi_task_parameters, '"transUses": "(.[^",])+'),
                           regexp_instr(
                               regexp_substr(tt.jedi_task_parameters, '"transUses": "(.[^",])+'),
                               '(": ")+',
                               1,
                               1,
                               1
                           )
                    ), '')) AS trans_uses,
        to_char(NVL(substr(regexp_substr(tt.jedi_task_parameters, '"vo": "[a-zA-Z0-9_\-\.]+[^"]'),
                           regexp_instr(
                               regexp_substr(tt.jedi_task_parameters, '"vo": "[a-zA-Z0-9_\-\.]+[^"]'),
                               '(": ")+',
                               1,
                               1,
                               1
                           )
                    ), '')) AS vo,
        to_char(NVL(substr(regexp_substr(tt.jedi_task_parameters, '"--runNumber=[0-9]+[^"]'),
                           regexp_instr(
                               regexp_substr(tt.jedi_task_parameters, '"--runNumber=[0-9]+[^"]'),
                               '(=)+',
                               1,
                               1,
                               1
                           )
                    ), '')) AS run_number,
        to_char(NVL(substr(regexp_substr(tt.jedi_task_parameters, '"--triggerConfig=\\"(.[^\""])+'),
                           regexp_instr(
                               regexp_substr(tt.jedi_task_parameters, '"--triggerConfig=\\"(.[^\""])+'),
                               '(=\\")+',
                               1,
                               1,
                               1
                           )
                    ), '')) AS trigger_config,
        to_char(NVL(substr(regexp_substr(tt.jedi_task_parameters, '"--jobConfig=\\"(.[^\""])+'),
                     regexp_instr(
                         regexp_substr(tt.jedi_task_parameters, '"--jobConfig=\\"(.[^\""])+'),
                         '(=\\")+',
                         1,
                         1,
                         1
                     )
              ), '')) AS job_config,
        to_char(NVL(substr(regexp_substr(tt.jedi_task_parameters, '"--evgenJobOpts=\\"(.[^\""])+'),
                     regexp_instr(
                         regexp_substr(tt.jedi_task_parameters, '"--evgenJobOpts=\\"(.[^\""])+'),
                         '(=\\")+',
                         1,
                         1,
                         1
                     )
              ), '')) AS evgen_job_opts,
        to_char(NVL(substr(regexp_substr(tt.jedi_task_parameters, '"cloud": "(.[^",])+'),
                           regexp_instr(
                               regexp_substr(tt.jedi_task_parameters, '"cloud": "(.[^",])+'),
                               '(": ")+',
                               1,
                               1,
                               1
                           )
                    ), '')) AS cloud,
        to_char(NVL(substr(regexp_substr(tt.jedi_task_parameters, '"site": "(.[^",])+'),
                           regexp_instr(
                               regexp_substr(tt.jedi_task_parameters, '"site": "(.[^",])+'),
                               '(": ")+',
                               1,
                               1,
                               1
                           )
                    ), '')) AS site
      FROM
        tasks t LEFT JOIN t_task tt
          ON t.taskid = tt.taskid
  )
  SELECT
    t.campaign,
    t.subcampaign,
    t.phys_group,
    t.project,
    t.pr_id,
    t.step_name,
    t.status,
    t.taskid,
    t.taskname,
    t.task_timestamp,
    t.start_time,
    t.end_time,
    t.hashtag_list,
    t.description,
    t.energy_gev,
    t.architecture,
    t.core_count,
    t.conditions_tags,
    t.geometry_version,
    t.ticket_id,
    t.trans_home,
    t.trans_path,
    t.trans_uses,
    t.user_name,
    t.vo,
    t.run_number,
    t.trigger_config,
    t.job_config,
    t.evgen_job_opts,
    t.cloud,
    t.site,
    t.primary_input,
    t.ctag,
    t.output_formats,
    sum(jd.nevents) AS requested_events,
    sum(jd.neventsused) AS processed_events
  FROM tasks_t_task t
    LEFT JOIN ATLAS_PANDA.jedi_datasets jd
      ON jd.jeditaskid = t.taskid
  WHERE jd.type IN ('input')
        AND jd.masterid IS NULL
  GROUP by
    t.campaign,
    t.subcampaign,
    t.phys_group,
    t.project,
    t.pr_id,
    t.step_name,
    t.status,
    t.taskid,
    t.taskname,
    t.task_timestamp,
    t.start_time,
    t.end_time,
    t.hashtag_list,
    t.description,
    t.energy_gev,
    t.architecture,
    t.core_count,
    t.conditions_tags,
    t.geometry_version,
    t.ticket_id,
    t.trans_home,
    t.trans_path,
    t.trans_uses,
    t.user_name,
    t.vo,
    t.run_number,
    t.trigger_config,
    t.job_config,
    t.evgen_job_opts,
    t.cloud,
    t.site,
    t.primary_input,
    t.ctag,
    t.output_formats
  ORDER BY
    t.taskid;