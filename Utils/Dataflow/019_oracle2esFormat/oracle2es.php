#!/usr/bin/env php
<?php
function exception_error_handler($errno, $errstr, $errfile, $errline ) {
   throw new ErrorException($errstr, 0, $errno, $errfile, $errline);
}
set_error_handler("exception_error_handler");

function check_input(&$row) {
  $required_fields = array('hashtag_list', 'campaign', 'taskid');
  $not_empty_fields = array('campaign', 'taskid');

  if (!is_array($row)) {
    fwrite(STDERR, "(WARN) Failed to decode message.\n");
    return FALSE;
  }

  foreach ($required_fields as $field) {
    if (!(array_key_exists($field, $row))) {
      fwrite(STDERR, "(WARN) Required field \"$field\" is missed.\n");
      return FALSE;
    }
  }

  foreach ($not_empty_fields as $field) {
    if (!(array_key_exists($field, $row) && $row[$field])) {
      fwrite(STDERR, "(WARN) Required field \"$field\" is empty.\n");
      return FALSE;
    }
  }

  return TRUE;
}

function convertIndexToLowerCase(&$a) {
  $result = array();

  foreach (array_keys($a) as $i) {
    $result[strtolower($i)] = $t;
  }

  $a = $result;
}

if (isset($argv[1])) {
  $h = fopen($argv[1], "r");
} else {
  $h = fopen('php://stdin', 'r');
}

if ($h) {
  while (($line = fgets($h)) !== false) {
    $row = json_decode($line,true);

    if (!check_input($row)) {
      fwrite(STDERR, "(WARN) Input checks failed. Skipping message.\n");
      continue;
    }

    convertIndexToLowerCase($row);

    $hashtag_list = $row['hashtag_list'];
    $row['hashtag_list'] = array();
    foreach( explode(',',$hashtag_list) as $tag) {
      $row['hashtag_list'][] = trim($tag);
    }

    printf('{ "index" : {"_index":"prodsys", "_type":"%s", "_id":"%d" } }'."\n", $row['campaign'], $row['taskid']);

    echo json_encode($row)."\n";
  }
}

fclose($h);

?>
