#!/usr/bin/env php
<?php
function exception_error_handler($errno, $errstr, $errfile, $errline ) {
   throw new ErrorException($errstr, 0, $errno, $errfile, $errline);
}
set_error_handler("exception_error_handler");

function check_input(&$row) {
  $required_fields = array('campaign' => 'undefined', 'taskid' => null);

  if (!is_array($row)) {
    fwrite(STDERR, "(WARN) Failed to decode message.\n");
    return FALSE;
  }

  foreach (array_keys($required_fields) as $field) {
    if (!(array_key_exists($field, $row) && $row[$field])) {
      if ($required_fields[$field] !== null) {
        $row[$field] = $required_fields[$field];
      } else {
        fwrite(STDERR, "(WARN) Required field \"$field\" is empty and has no default value.\n");
        return FALSE;
      }
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

    printf('{ "index" : {"_index":"prodsys", "_type":"%s", "_id":"%d" } }'."\n", $row['campaign'], $row['taskid']);

    echo json_encode($row)."\n";
  }
}

fclose($h);

?>
