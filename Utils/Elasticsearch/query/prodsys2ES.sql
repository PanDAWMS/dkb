-- Select all tasks for specified period of time
-- Query tables:
------------------------------------------------
-- ATLAS_DEFT.t_production_task
-- ATLAS_DEFT.t_production_step
-- ATLAS_DEFT.t_step_template
-- ATLAS_DEFT.t_ht_to_task
-- ATLAS_DEFT.t_hashtag
-- ATLAS_DEFT.t_production_tag
-- ATLAS_PANDA.jedi_datasets
--
-- All fields:
-- architecture, campaign, cloud, conditions_tags, core_count, db_release, description, end_time,
-- energy_gev, evgen_job_opts, geometry_version, hashtag_list, job_config, physics_list, processed_events,
-- phys_group, project, pr_id, requested_events, run_number, site, start_time, step_name, status, subcampaign,
-- tag_name, tag_ecm_energy, tag_description,taskid, taskname, task_timestamp, task_type, ticket_id, trans_home,
-- trans_path, trans_uses, trf_release, trigger_config, user_name, vo
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
      t.timestamp > to_date('05-05-2015 00:00:00', 'dd-mm-yyyy hh24:mi:ss')
      AND t.timestamp <= to_date('06-05-2015 00:00:00', 'dd-mm-yyyy hh24:mi:ss')
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
        to_char(NVL(substr(regexp_substr(jedi_task_parameters, '"architecture": "(.[^",])+'),
                           regexp_instr(
                               regexp_substr(jedi_task_parameters, '"architecture": "(.[^",])+'),
                               '(": ")+',
                               1,
                               1,
                               1
                           )
                    ), '')) AS architecture,
        to_char(NVL(substr(regexp_substr(jedi_task_parameters, '"coreCount": [0-9\.]+'),
                           regexp_instr(
                               regexp_substr(jedi_task_parameters, '"coreCount": [0-9\.]+'),
                               '(": )+',
                               1,
                               1,
                               1
                           )
                    ), '')) AS core_count,
        to_char(NVL(substr(regexp_substr(jedi_task_parameters, '"\-\-conditionsTag \\"default:[a-zA-Z0-9_\-]+[^\""]'),
                           regexp_instr(
                               regexp_substr(jedi_task_parameters,
                                             '"\-\-conditionsTag \\"default:[a-zA-Z0-9_\-]+[^\""]'),
                               '(:)+',
                               1,
                               1,
                               1
                           )
                    ), '')) AS conditions_tags,
        to_char(NVL(substr(regexp_substr(jedi_task_parameters, '"\-\-geometryVersion=\\"default:[a-zA-Z0-9_\-]+[^\""]'),
                           regexp_instr(
                               regexp_substr(jedi_task_parameters,
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
        to_char(NVL(substr(regexp_substr(tt.jedi_task_parameters, '"userName": "(.[^",])+'),
                           regexp_instr(
                               regexp_substr(tt.jedi_task_parameters, '"userName": "(.[^",])+'),
                               '(": ")+',
                               1,
                               1,
                               1
                           )
                    ), '')) AS user_name,
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
                    ), '')) AS site,
        to_char(NVL(substr(regexp_substr(tt.jedi_task_parameters, '"taskType": "(.[^",])+'),
                           regexp_instr(
                               regexp_substr(tt.jedi_task_parameters, '"taskType": "(.[^",])+'),
                               '(": ")+',
                               1,
                               1,
                               1
                           )
                    ), '')) AS task_type
      FROM
        tasks t LEFT JOIN t_task tt
          ON t.taskid = tt.taskid
  ),
  tasks_tags as (
select
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
        t.cloud,
        t.site,
        t.task_type,
        (CASE t.job_config WHEN '' THEN
                    to_char(NVL(substr(
          regexp_substr(tag_parameters, '"JobConfig": "[a-zA-Z0-9_\.\-\:]+[^"]'),
            regexp_instr(
                regexp_substr(tag_parameters, '"JobConfig": "[a-zA-Z0-9_\.\-\:]+[^"]'),
                '(": ")+',
                1,
                1,
                1
            )
          ), ''))
        END) as job_config,
        (CASE t.evgen_job_opts WHEN '' THEN
          to_char(NVL(substr(
          regexp_substr(tag_parameters, '"EvgenJobOpts": "[a-zA-Z0-9_\.\-]+[^"]'),
            regexp_instr(
                regexp_substr(tag_parameters, '"EvgenJobOpts": "[a-zA-Z0-9_\.\-]+[^"]'),
                '(": ")+',
                1,
                1,
                1
            )
          ), ''))
          END) as evgen_job_opts,
        (CASE t.conditions_tags WHEN '' THEN
          to_char(NVL(regexp_replace(substr(
        regexp_substr(tag_parameters, '"(ConditionsTag)|(conditionsTag)": "[a-zA-Z0-9_":\.\-]+[^"]'),
          regexp_instr(
              regexp_substr(tag_parameters, '"(ConditionsTag)|(conditionsTag)": "[a-zA-Z0-9_":\.\-]+[^"]'),
              '(": ")+',
              1,
              1,
              1
          )
        ), '"|\\|,|default:','') , ''))
          END) as conditions_tags,
        (CASE t.geometry_version
        WHEN ''
          THEN
          to_char(NVL(regexp_replace(substr(
        regexp_substr(tag_parameters, '"(Geometry)|(geometryVersion)": "[a-zA-Z0-9_":\.\-]+[^"]'),
          regexp_instr(
              regexp_substr(tag_parameters, '"(Geometry)|(geometryVersion)": "[a-zA-Z0-9_":\.\-]+[^"]'),
              '(": ")+',
              1,
              1,
              1
          )
        ),'"|\\|,|default:',''), '')) END) as geometry_version,
        t.ticket_id,
        (CASE t.trans_home WHEN '' THEN
          to_char(NVL(substr(
          regexp_substr(tag_parameters, '"SWReleaseCache": "[a-zA-Z0-9_\.\-]+[^"]'),
            regexp_instr(
                regexp_substr(tag_parameters, '"SWReleaseCache": "[a-zA-Z0-9_\.\-]+[^"]'),
                '(": ")+',
                1,
                1,
                1
            )
          ), ''))
          END) as trans_home,
        t.trans_uses,
        t.user_name,
        t.vo,
        (CASE t.run_number WHEN '' THEN
          to_char(NVL(substr(
        regexp_substr(tag_parameters, '"DataRunNumber": "[0-9]+[^"]'),
          regexp_instr(
              regexp_substr(tag_parameters, '"DataRunNumber": "[0-9]+[^"]'),
              '(": ")+',
              1,
              1,
              1
          )
        ), ''))
        END) as run_number,
        t.trigger_config,
        tag.name as tag_name,
        tag.trf_release,
        to_char(NVL(substr(
          regexp_substr(tag_parameters, '"DBRelease": "[a-zA-Z0-9_\.\-]+[^"]'),
            regexp_instr(
                regexp_substr(tag_parameters, '"DBRelease": "[a-zA-Z0-9_\.\-]+[^"]'),
                '(": ")+',
                1,
                1,
                1
            )
          ), '')) as db_release,
        (CASE trans_path WHEN '' THEN
          to_char(NVL(substr(
          regexp_substr(tag_parameters, '"(Transformation)|(transformation)": "[a-zA-Z0-9_\.\-]+[^"]'),
            regexp_instr(
                regexp_substr(tag_parameters, '"(Transformation)|(transformation)": "[a-zA-Z0-9_\.\-]+[^"]'),
                '(": ")+',
                1,
                1,
                1
            )
          ), ''))
          END) as trans_path,
          to_char(NVL(substr(
          regexp_substr(tag_parameters, '"EcmEnergy": "[a-zA-Z0-9_\.\-]+[^"]'),
            regexp_instr(
                regexp_substr(tag_parameters, '"EcmEnergy": "[a-zA-Z0-9_\.\-]+[^"]'),
                '(": ")+',
                1,
                1,
                1
            )
          ), '')) as tag_ecm_energy,
          to_char(NVL(substr(
          regexp_substr(tag_parameters, '"PhysicsList": "[a-zA-Z0-9_\.\-]+[^"]'),
            regexp_instr(
                regexp_substr(tag_parameters, '"PhysicsList": "[a-zA-Z0-9_\.\-]+[^"]'),
                '(": ")+',
                1,
                1,
                1
            )
          ), '')) as physics_list,
          to_char(NVL(substr(
          regexp_substr(tag_parameters, '"description": "[a-zA-Z0-9_\.\-]+[^"]'),
            regexp_instr(
                regexp_substr(tag_parameters, '"description": "[a-zA-Z0-9_\.\-]+[^"]'),
                '(": ")+',
                1,
                1,
                1
            )
          ), '')) as tag_description,
          to_char(NVL(substr(
          regexp_substr(tag_parameters, '"baseRelease": "[0-9\.]+[^"]'),
            regexp_instr(
                regexp_substr(tag_parameters, '"baseRelease": "[0-9\.]+[^"]'),
                '(": ")+',
                1,
                1,
                1
            )
          ), '')) as base_release
from tasks_t_task t
  LEFT JOIN
     ATLAS_DEFT.t_production_tag tag
ON
  tag.name = trim(regexp_substr(trim(regexp_substr(t.taskname, '[^.]+',1,5)), '[^_]*$'))
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
        t.cloud,
        t.site,
        t.task_type,
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
        t.tag_name,
        t.trf_release,
        t.db_release,
        t.job_config,
        t.evgen_job_opts,
        t.tag_ecm_energy,
        t.physics_list,
        t.tag_description,
        jd.nevents AS requested_events,
        jd.neventsused AS processed_events
      FROM tasks_tags t
        LEFT JOIN ATLAS_PANDA.jedi_datasets jd
          ON jd.jeditaskid = t.taskid
      WHERE jd.type IN ('input')
            AND jd.masterid IS NULL;
