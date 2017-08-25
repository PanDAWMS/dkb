-- QUERY FROM DEFT/JEDI FOR ELASTICSEARCH EVENTS SUMMARY FOR MC16 CAMPAIGN

-- 1. (mc16_tasks) get all tasks for mc16 campaign, including some REQUEST params and restricted by GOOD status
-- 2. (task_hashtags) get lists of hashtags for all mc16 tasks, defining step_names by step_id
-- 3. FINALLY:
--    JOIN with ATLAS_PANDA.jedi_datasets to obtain events numbers
--    nevents - number of input events
--    neventsused - number of processed events
--    JOIN with t_task to obtain such parameters as:
--    atlas geometry, conditions tags, software release
with mc16_tasks as (
    select
      t.campaign,
      t.taskid,
      t.step_id,
      t.taskname,
      TO_CHAR(t.timestamp, 'dd-mm-yyyy hh24:mi:ss') as task_timestamp,
      NVL(TO_CHAR(t.start_time, 'dd-mm-yyyy hh24:mi:ss'), '') as start_time,
      NVL(TO_CHAR(t.endtime, 'dd-mm-yyyy hh24:mi:ss'), '') as end_time,
      t.subcampaign,
      t.project,
      t.phys_group,
      t.status,
      t.pr_id,
      r.description,
      r.energy_gev
    from
      ATLAS_DEFT.t_production_task t,
      ATLAS_DEFT.t_prodmanager_request r
    where
      lower(t.campaign) = 'mc16'
      and t.pr_id = r.pr_id
),
  task_hashtags as (
      SELECT
        t.campaign,
        t.subcampaign,
        t.phys_group,
        t.project,
        t.pr_id,
        s_t.step_name,
        t.status,
        t.taskid,
        t.taskname,
        t.task_timestamp,
        t.start_time,
        t.end_time,
        t.description,
        t.energy_gev,
        LISTAGG(hashtag.hashtag, ', ')
        WITHIN GROUP (
          ORDER BY t.taskid) AS hashtag_list
      FROM
        mc16_tasks t,
        ATLAS_DEFT.t_ht_to_task ht_t,
        ATLAS_DEFT.t_hashtag hashtag,
        ATLAS_DEFT.t_production_step s,
        ATLAS_DEFT.t_step_template s_t
      WHERE
        t.taskid = ht_t.taskid
        and hashtag.ht_id = ht_t.ht_id
        and t.step_id = s.step_id
        and s.step_t_id = s_t.step_t_id
      GROUP BY
        t.campaign,
        t.subcampaign,
        t.phys_group,
        t.project,
        t.pr_id,
        s_t.step_name,
        t.status,
        t.taskid,
        t.taskname,
        t.task_timestamp,
        t.start_time,
        t.end_time,
        t.description,
        t.energy_gev
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
        to_char(NVL(substr(regexp_substr(jedi_task_parameters, '"architecture": "(.[^",])+'),
               regexp_instr(
                   regexp_substr(jedi_task_parameters, '"architecture": "(.[^",])+'),
                   '(": ")+',
                   1,
                   1,
                   1
               )
        ),'')) as architecture,
        to_char(NVL(substr(regexp_substr(jedi_task_parameters, '"coreCount": [0-9\.]+'),
               regexp_instr(
                   regexp_substr(jedi_task_parameters, '"coreCount": [0-9\.]+'),
                   '(": )+',
                   1,
                   1,
                   1
               )
        ),'')) as core_count,
        to_char(NVL(substr(regexp_substr(jedi_task_parameters, '"\-\-conditionsTag \\"default:[a-zA-Z0-9_\-]+[^\""]'),
               regexp_instr(
                   regexp_substr(jedi_task_parameters, '"\-\-conditionsTag \\"default:[a-zA-Z0-9_\-]+[^\""]'),
                   '(:)+',
                   1,
                   1,
                   1
              )
       ),'')) as conditions_tags,
        to_char(NVL(substr(regexp_substr(jedi_task_parameters, '"\-\-geometryVersion=\\"default:[a-zA-Z0-9_\-]+[^\""]'),
               regexp_instr(
                   regexp_substr(jedi_task_parameters, '"\-\-geometryVersion=\\"default:[a-zA-Z0-9_\-]+[^\""]'),
                   '(:)+',
                   1,
                   1,
                   1
              )
       ),'')) as geometry_version,
        to_char(NVL(substr(regexp_substr(tt.jedi_task_parameters, '"ticketID": "(.[^",])+'),
                 regexp_instr(
                     regexp_substr(tt.jedi_task_parameters, '"ticketID": "(.[^",])+'),
                     '(": ")+',
                     1,
                     1,
                     1
                 )
        ),'')) as ticket_id,
        to_char(NVL(substr(regexp_substr(tt.jedi_task_parameters, '"transHome": "(.[^",])+'),
                  regexp_instr(
                      regexp_substr(tt.jedi_task_parameters, '"transHome": "(.[^",])+'),
                      '(": ")+',
                      1,
                      1,
                      1
                  )
        ),'')) as trans_home,
        to_char(NVL(substr(regexp_substr(tt.jedi_task_parameters, '"transPath": "(.[^",])+'),
                     regexp_instr(
                         regexp_substr(tt.jedi_task_parameters, '"transPath": "(.[^",])+'),
                         '(": ")+',
                         1,
                         1,
                         1
                     )
        ),'')) as trans_path,
        to_char(NVL(substr(regexp_substr(tt.jedi_task_parameters, '"transUses": "(.[^",])+'),
                     regexp_instr(
                         regexp_substr(tt.jedi_task_parameters, '"transUses": "(.[^",])+'),
                         '(": ")+',
                         1,
                         1,
                         1
                     )
        ),'')) as trans_uses,
        to_char(NVL(substr(regexp_substr(tt.jedi_task_parameters, '"userName": "(.[^",])+'),
                     regexp_instr(
                         regexp_substr(tt.jedi_task_parameters, '"userName": "(.[^",])+'),
                         '(": ")+',
                         1,
                         1,
                         1
                     )
        ),'')) as user_name,
        to_char(NVL(substr(regexp_substr(tt.jedi_task_parameters, '"vo": "[a-zA-Z0-9_\-\.]+[^"]'),
                     regexp_instr(
                         regexp_substr(tt.jedi_task_parameters, '"vo": "[a-zA-Z0-9_\-\.]+[^"]'),
                         '(": ")+',
                         1,
                         1,
                         1
                     )
        ),'')) as vo,
        to_char(NVL(substr(regexp_substr(tt.jedi_task_parameters, '"--runNumber=[0-9]+[^"]'),
                         regexp_instr(
                             regexp_substr(tt.jedi_task_parameters, '"--runNumber=[0-9]+[^"]'),
                             '(=)+',
                             1,
                             1,
                             1
                         )
        ),'')) as run_number,
        to_char(NVL(substr(regexp_substr(tt.jedi_task_parameters, '"--triggerConfig=\\"(.[^\""])+'),
                         regexp_instr(
                             regexp_substr(tt.jedi_task_parameters, '"--triggerConfig=\\"(.[^\""])+'),
                             '(=\\")+',
                             1,
                             1,
                             1
                         )
        ),'')) as trigger_config,
          (SELECT LISTAGG(jd.datasetname, ', ')
           WITHIN GROUP (ORDER BY datasetname) "input_data"
            from ATLAS_PANDA.jedi_datasets jd
            where jd.jeditaskid = t.taskid
          and type IN ('input')) as input_datasets,
          (SELECT LISTAGG(jd.datasetname, ', ')
           WITHIN GROUP (ORDER BY datasetname) "output_data"
            from ATLAS_PANDA.jedi_datasets jd
            where jd.jeditaskid = t.taskid
          and type IN ('output')) as output_datasets,
          (SELECT sum(jd.nevents)
            from ATLAS_PANDA.jedi_datasets jd
            where jd.jeditaskid = t.taskid
          and type IN ('input')) as requested_events,
          (SELECT sum(jd.neventsused)
            from ATLAS_PANDA.jedi_datasets jd
            where jd.jeditaskid = t.taskid
          and type IN ('input')) as processed_events
      FROM
        task_hashtags t,
        t_task tt
      WHERE t.taskid = tt.taskid;