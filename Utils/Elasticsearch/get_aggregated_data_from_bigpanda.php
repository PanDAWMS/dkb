<?php
date_default_timezone_set('UTC');
function usage() {
  global $argv;
  die("This script gets aggregated data for campaign and creates output for load to elasticsearch\nUsage: php ".$argv[0]." <campaign>\n");
}
if (!isset($argv[1])) {
  usage();
}
$campaign = $argv[1];
$data4upload=array();
$now=time();
foreach(json_decode(file_get_contents("http://bigpanda.cern.ch/report/?campaign=".$campaign."&type=DCC"),true) as $line) {
  $line['@timestamp'] = date('c',$now);
  $line['campagin'] = $campaign;
  printf('{ "index" : { "_index" : "campaign_%s-%s", "_type" : "%s", "_id" : "%s" } } '."\n", 
    strtolower($campaign),
    date("y-m",$now), 
    'aggregated',
    strtolower($line['hashtag'].'-'.$now)
  );
  echo json_encode($line)."\n";
}

?>
