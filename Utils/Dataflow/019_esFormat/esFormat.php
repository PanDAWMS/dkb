#!/usr/bin/env php
<?php
function exception_error_handler($errno, $errstr, $errfile, $errline ) {
   throw new ErrorException($errstr, 0, $errno, $errfile, $errline);
}
set_error_handler("exception_error_handler");

$DEFAULT_INDEX = 'tasks_production';
$ES_INDEX = NULL;
$EOP_DEFAULTS = Array("stream" => chr(0), "file" => "");
$EOM_DEFAULTS = Array("stream" => chr(30), "file" => chr(30));

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

function decode_escaped($string) {
  return preg_replace('/\\\\([nrtvf\\\\$"]|[0-7]{1,3}|x[0-9A-Fa-f]{1,2})/e',
                      'stripcslashes("$0")', $string);
}

$opts = getopt("e:E:", Array("end-of-message:", "end-of-process:"));
$args = $argv;

foreach ($opts as $key => $val) {
  $match = preg_grep("/^-(-)?".$key."$/", $args);
  foreach ($match as $mkey => $mval) {
    unset($args[$mkey]);
    unset($args[$mkey+1]);
  }
  $match = preg_grep("/^-(-)?".$key."=/", $args);
  foreach ($match as $mkey => $mval) {
    unset($args[$mkey]);
  }
  switch ($key) {
    case "e":
    case "end-of-message":
      $EOM_MARKER = decode_escaped($val);
      break;
    case "E":
    case "end-of-process":
      $EOP_MARKER = decode_escaped($val);
      break;
  }
}

$args = array_values($args);

if (isset($args[1])) {
  $h = fopen($args[1], "r");
  $mode = "file";
} else {
  $h = fopen('php://stdin', 'r');
  $mode = "stream";
}

if (!(isset($EOM_MARKER))) $EOM_MARKER = $EOM_DEFAULTS[$mode];
if (!(isset($EOP_MARKER))) $EOP_MARKER = $EOP_DEFAULTS[$mode];

$EOM_HEX = implode(unpack("H*", $EOM_MARKER));
$EOP_HEX = implode(unpack("H*", $EOP_MARKER));

if ($EOM_MARKER == '') {
  fwrite(STDERR, "(ERROR) EOM marker can not be empty string.\n");
  exit(1);
}
if ($EOM_MARKER == "\n") {
  fwrite(STDERR, "(ERROR) NEWLINE symbol is not allowed as EOM, "
                 ."as it is contained in output messages.\n");
  exit(1);
}


fwrite(STDERR, "(DEBUG) End-of-message marker: '" . $EOM_MARKER . "' (hex: " . $EOM_HEX . ").\n");
fwrite(STDERR, "(DEBUG) End-of-process marker: '" . $EOP_MARKER . "' (hex: " . $EOP_HEX . ").\n");


$ES_INDEX = getenv('ES_INDEX');
if (!$ES_INDEX) {
  $ES_INDEX = $DEFAULT_INDEX;
}

if ($h) {
  while (($line = stream_get_line($h, 0, $EOM_MARKER)) !== false) {
    $row = json_decode($line,true);

    if (!check_input($row)) {
      fwrite(STDERR, "(WARN) Skipping message (\"".substr($line, 0, 1000)."\").\n");
      continue;
    }

    convertIndexToLowerCase($row);

    $index = constructIndexJson($row);

    echo json_encode($index)."\n";
    echo json_encode($row)."\n";
    echo $EOM_MARKER;
    echo $EOP_MARKER;
  }
}

fclose($h);

?>
