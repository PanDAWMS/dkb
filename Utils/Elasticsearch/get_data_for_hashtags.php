<?php
date_default_timezone_set('UTC');

function usage() {
  global $argv;
  printf ("Usage: php %s <campaign> <tag1>[,tag2,...,tagN]\n",$argv[0]);
}

$response = array();
$work_mode = ( php_sapi_name() == 'cli' )? 'cli' : 'web';

if ($work_mode == 'cli' ) {
  if (isset($argv[1],$argv[2])) {
    $campaign = $argv[1];
    $tags = explode(',', $argv[2]);
  } else {
    usage();
  }
} else {
  if (isset($_REQUEST['campaign'], $_REQUEST['tags'])) {
    $campaign = $_REQUEST['campaign'];
    $tags = explode(',', $_REQUEST['tags']);
  }
}

if (isset($campaign,$tags)) {
  $now = time();
  #we may have no data for today, so create request for today and yesturday
  $indexes = array(
    sprintf('%s-%s', $campaign, date('y-m',$now)),
    sprintf('%s-%s', $campaign, date('y-m',$now-3600*24)),
  );
  #use one index if today and yesturday in same month 
  $indexes = array_unique($indexes); 
  $request = strtolower(sprintf('{"index":"%s", "type":"aggregated"}',implode(',', $indexes)))."\n";
  $request .= json_encode(array(
    'size' => 0,
    '_source' => false,
    'query' => array(
      'query_string' => array(
        'default_field' => 'hashtag',
        'analyze_wildcard' => true,
        'query' => implode(' OR ', $tags),
      ),
    ),
    'aggs' => array(
      'tags' => array(
        'aggs' => array(
          'max_date' => array(
            'max' => array(
              'field' => '@timestamp'
            )
          ),
          'finished' => array(
            'max' => array(
              'field' => 'finishedev'
            )
          ),
          'rest' => array(
            'min' => array(
              'field' => 'restev'
            )
          ),
        ),
        'terms' => array(
          'field' => 'hashtag.keyword',
          'order' => array(
            'max_date' => 'desc'
          ),
          'size' => 1000,
        ),
      ),
    ),
  ))."\n";

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
    CURLOPT_URL => 'http://127.0.0.1:9200/_msearch',
    CURLOPT_HTTPHEADER => array('Content-Type: application/x-ndjson','Content-Length: '.strlen($request)),
    CURLOPT_POSTFIELDS => $request,
  );
  $ch = curl_init();

  curl_setopt_array($ch, $opts);
  $result = json_decode(curl_exec($ch),true);
  foreach ($result['responses'][0]['aggregations']['tags']['buckets'] as $result) {
    $response[$result['key']]=array(
      'restev' => $result['rest']['value'],
      'finishedev' => $result['finished']['value'],
    );
  }
}

echo json_encode($response)."\n";
?>
