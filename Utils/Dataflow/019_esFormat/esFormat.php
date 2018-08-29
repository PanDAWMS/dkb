#!/usr/bin/env php
<?php
function exception_error_handler($errno, $errstr, $errfile, $errline ) {
   throw new ErrorException($errstr, 0, $errno, $errfile, $errline);
}
set_error_handler("exception_error_handler");

$DEFAULT_INDEX = 'tasks_production';
$ES_INDEX = NULL;
$EOP_MARKER = '';
$EOM_MARKER = "\n";

function check_input($row) {
  $required_fields = array('_id', '_type');

  if (!is_array($row)) {
    fwrite(STDERR, "(WARN) Failed to decode message.\n");
    return FALSE;
  }

  foreach ($required_fields as $field) {
    if (!(isset($row[$field]))) {
      fwrite(STDERR, "(WARN) Required field \"$field\" is not set or empty.\n");
      return FALSE;
    }
  }

  return TRUE;
}

function convertIndexToLowerCase(&$a) {
  $result = array();

  foreach (array_keys($a) as $i) {
    $result[strtolower($i)] = $a[$i];
  }

  $a = $result;
}

function constructIndexJson(&$row) {
  global $ES_INDEX;
  $index = Array(
    'index' => Array(
      '_index' => $ES_INDEX,
      '_type'  => $row['_type'],
      '_id'    => $row['_id'],
    )
  );

  if (isset($row['_parent'])) {
    $index['index']['_parent'] = $row['_parent'];
  }

  foreach ($index['index'] as $key => $val) {
    unset($row[$key]);
  }

  return $index;
}

if (isset($argv[1])) {
  $h = fopen($argv[1], "r");
} else {
  $h = fopen('php://stdin', 'r');
}

$ES_INDEX = getenv('ES_INDEX');
if (!$ES_INDEX) {
  $ES_INDEX = $DEFAULT_INDEX;
}

if ($h) {
  while (($line = fgets($h)) !== false) {
    $row = json_decode($line,true);

    if (!check_input($row)) {
      fwrite(STDERR, "(WARN) Skipping message (\"".substr($line, 0, 1000)."\").\n");
      continue;
    }

    convertIndexToLowerCase($row);

    $index = constructIndexJson($row);

    echo json_encode($index)."\n";
    echo json_encode($row);
    echo $EOM_MARKER;
    echo $EOP_MARKER;
  }
}

fclose($h);

?>
