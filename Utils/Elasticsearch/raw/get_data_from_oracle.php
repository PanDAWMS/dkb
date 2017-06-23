<?php
fscanf(STDIN, "%s\n", $p);

$fields = "lower(t.CAMPAIGN) as campaing, lower(t.SUBCAMPAIGN) as sub_campaing, t.phys_group as phys_group, t.project as project, t.PR_ID as request, t.TASKID as task_id, t.status as status, t.taskname as taskname, LISTAGG(hashtag.hashtag, ',') within group (order by ht_t.taskid) as hashtag_list, t.TOTAL_EVENTS as events_total, t.TOTAL_REQ_EVENTS as events_requested, t.timestamp as t_stamp, substr(t.taskname, instrc(t.taskname,'.',1,3)+1,instrc(t.taskname,'.',1,4)-instrc(t.taskname,'.',1,3)-1) as step";
$tables = 'T_PRODUCTION_TASK t, atlas_deft.t_ht_to_task ht_t, atlas_deft.t_hashtag hashtag';
$mc16 = "and lower(t.campaign) = 'mc16' and lower(t.subcampaign) = 'mc16a'";
$conditions = "t.taskid = ht_t.taskid and ht_t.ht_id = hashtag.ht_id $mc16";
$group = "lower(t.CAMPAIGN), lower(t.SUBCAMPAIGN), t.phys_group, t.project, t.status, t.taskid, t.TOTAL_EVENTS, t.TOTAL_REQ_EVENTS, t.timestamp, t.taskname, t.PR_ID,substr(t.taskname, instrc(t.taskname,'.',1,3)+1,instrc(t.taskname,'.',1,4)-instrc(t.taskname,'.',1,3)-1)";
$query = "select $fields FROM $tables WHERE $conditions GROUP BY $group";
#echo "$query\n";

$conn = oci_connect('atlas_deft_r', $p, 'adcr_adg');
if (!$conn) {
    $e = oci_error();
    trigger_error(htmlentities($e['message'], ENT_QUOTES), E_USER_ERROR);
}

$stid = oci_parse($conn, $query);
#echo "Parsed!\n";

oci_execute($stid);
#echo "Execution done!\n";

while ($row = oci_fetch_array($stid, OCI_ASSOC)) {
  echo json_encode($row)."\n";
}

?>
