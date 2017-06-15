<?php

if (isset($argv[1])) {
  $request = file_get_contents($argv[1]);
} else {
  die("please provide file to upload!\n");
}

$request .= "\n";
$opts = array(
  CURLOPT_POST => true,
  CURLOPT_RETURNTRANSFER => true,
  CURLOPT_CONNECTTIMEOUT => 30,
  CURLOPT_HEADER => false,
  CURLOPT_VERBOSE => false,
  CURLOPT_ENCODING => 'gzip',
  CURLOPT_FORBID_REUSE => false,
  CURLOPT_FRESH_CONNECT => false,
  CURLOPT_BINARYTRANSFER => true,
  CURLOPT_HTTPAUTH => CURLAUTH_ANY,
  CURLOPT_URL => 'http://127.0.0.1:9200/_bulk',
  CURLOPT_HTTPHEADER => array('Content-Type: application/x-www-form-urlencoded','Content-Length: '.strlen($request)),
  CURLOPT_POSTFIELDS => $request,
);

$ch = curl_init();

curl_setopt_array($ch, $opts);

$result = curl_exec($ch);
if (($result = curl_errno($ch)) > 0 )  {
  echo "$result"."\n";
  echo curl_error($ch)."\n";
}


?>
