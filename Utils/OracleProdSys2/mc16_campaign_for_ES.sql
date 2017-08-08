-- QUERY FROM DEFT/JEDI FOR ELASTICSEARCH EVENTS SUMMARY FOR MC16 CAMPAIGN

-- 1. (mc16_tasks) get all tasks for mc16 campaign, including some REQUEST params and restricted by GOOD status
-- 2. (task_hashtags) get lists of hashtags for all mc16 tasks, defining step_names by step_id
-- 3. FINALLY:
--    JOIN with ATLAS_PANDA.jedi_datasets to obtain events numbers
--    nevents - number of input events
--    neventsused - number of processed events
--    JOIN with ATLAS_DEFT.t_production_tag to obtain such parameters as:
--    atlas geometry, conditions tags, software release
-- restrict query by GOOD dataset statuses
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
      and t.status in ('done','finished','running','ready','prepared','registered','defined')
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
        jd.datasetname,
        jd.status as dataset_status,
        jd.nevents AS requested_events,
        jd.neventsused AS processed_events,
        tag.name as tag_name,
        tag.trf_release,
        tag.tag_parameters
      FROM
        task_hashtags t,
        ATLAS_PANDA.jedi_datasets jd,
        ATLAS_DEFT.t_production_tag tag
      WHERE
        t.taskid = jd.jeditaskid
        AND jd.masterid IS NULL
        AND jd.type IN ('input')
        and jd.status in ('ready','done','finished')
        and tag.name = trim(regexp_substr(trim(regexp_substr(t.taskname, '[^.]+',1,5)), '[^_]*$'));