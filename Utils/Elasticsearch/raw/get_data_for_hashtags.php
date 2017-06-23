<?php
date_default_timezone_set('UTC');

function usage() {
  global $argv;
  printf ("Usage: php %s <tag1>[,tag2,...,tagN]\n",$argv[0]);
}

$response = array('requested' => 0, 'total' => 0);
$work_mode = ( php_sapi_name() == 'cli' )? 'cli' : 'web';

if ($work_mode == 'cli' ) {
  if (isset($argv[1])) {
    $tags = explode(',', $argv[1]);
  } else {
    usage();
  }
} else {
  if (isset($_REQUEST['tags'])) {
    $tags = explode(',', $_REQUEST['tags']);
  }
}

if (isset($tags)) {
  #we may have no data for today, so create request for today and yesturday
  $indexes = 'raw_current-*'; 
  $request = json_encode(array(
    'size' => 0,
    '_source' => false,
    'query' => array(
      'query_string' => array(
        'default_field' => 'extended_tags',
        'analyze_wildcard' => true,
        'query' => implode(' AND ', $tags),
      ),
    ),
    'aggs' => array(
      'requested' => array(
        'sum' => array('field' => 'events_requested'),
      ),
      'total' => array(
        'sum' => array('field' => 'events_total'),
      ),
    ),
  ));

  $opts = array(
    CURLOPT_POST => true,
    CURLOPT_CUSTOMREQUEST => 'GET',
    CURLOPT_RETURNTRANSFER => true,
    CURLOPT_CONNECTTIMEOUT => 30,
    CURLOPT_HEADER => false,
    CURLOPT_VERBOSE => false,
    CURLOPT_ENCODING => 'gzip',
    CURLOPT_FORBID_REUSE => false,
    CURLOPT_FRESH_CONNECT => false,
    CURLOPT_BINARYTRANSFER => true,
    CURLOPT_HTTPAUTH => CURLAUTH_ANY,
    CURLOPT_URL => 'http://127.0.0.1:9200/raw_current-*/done,finished/_search',
    CURLOPT_HTTPHEADER => array('Content-Type: application/x-ndjson','Content-Length: '.strlen($request)),
    CURLOPT_POSTFIELDS => $request,
  );
  $ch = curl_init();

  curl_setopt_array($ch, $opts);
  $result = json_decode(curl_exec($ch),true);
  foreach (array_keys($response) as $rf) {
    $response[$rf] = $result['aggregations'][$rf]['value'];
  }
}
echo json_encode($response)."\n";
?>
