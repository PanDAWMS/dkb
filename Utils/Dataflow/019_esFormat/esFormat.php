#!/usr/bin/env php
<?php
function exception_error_handler($errno, $errstr, $errfile, $errline ) {
   throw new ErrorException($errstr, 0, $errno, $errfile, $errline);
}
set_error_handler("exception_error_handler");

# Default values.
# ES index where the documents should be indexed/updated.
$DEFAULT_INDEX = 'tasks_production';
# End-of-process marker, depending on mode.
$EOP_DEFAULTS = Array("stream" => chr(0), "file" => "");
# End-of-message marker, depending on mode.
$EOM_DEFAULTS = Array("stream" => chr(30), "file" => chr(30));

# Variable initialization.
# Action. Possible values are 'index' and 'update'. Note: this variable is not
# a constant and can be changed with command line argument. The "DEFAULT" in
# its name refers to action not being specified in the data being processed,
# not in the script's configuration (like the default values above).
$DEFAULT_ACTION = 'index';
# How many times the update should be retried in case of conflict.
$UPDATE_RETRIES = 3;
# ES index where the documents should be indexed/updated.
$ES_INDEX = NULL;

function check_input($row) {
  /* Check the provided input's correctness.

  The input must:
  - be an array;
  - contain non-empty fields '_id' and '_type'.

  :param row: input to check

  :return: TRUE if input is correct, FALSE if it is not
  :rtype: bool
  */
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
  /* Convert array's keys to lowercase, in place.

  :param a: array to process
  :type a: array
  */
  $result = array();

  foreach (array_keys($a) as $i) {
    $result[strtolower($i)] = $a[$i];
  }

  $a = $result;
}

function getAction($row) {
  global $DEFAULT_ACTION;

  if (isset($row['_update']) and $row['_update'] === true) {
    $action = 'update';
  } else {
    $action = $DEFAULT_ACTION;
  }

  return $action;
}

function constructActionJson($row) {
  /* Generate a json with ES bulk API action information for a given document.

  Action json is generated for each document to be processed in a single
  bulk action, and includes directions about how exactly the document must
  be processed. It contains a single key-value pair. The key is the action to
  be taken, and is determined upon the presence of "update":true in the
  document. The value is an array that contains the remaining information:
  - index name
  - number of retries if the action is update
  - document id
  - document type
  - document's parent, if it is specified.

  The latter three values are taken from the document.

  :param row: document for which the action json should be generated
  :type row: array

  :return: generated action json
  :rtype: array
  */
  global $ES_INDEX;
  global $UPDATE_RETRIES;

  $act = getAction($row);

  $action = Array(
    $act => Array(
      '_index' => $ES_INDEX,
      '_type'  => $row['_type'],
      '_id'    => $row['_id'],
    )
  );

  if ($act == 'update') {
    $action[$act]['_retry_on_conflict'] = $UPDATE_RETRIES;
  }

  if (isset($row['_parent'])) {
    $action[$act]['_parent'] = $row['_parent'];
  }

  return $action;
}

function constructDataJson($row) {
  /* Prepare the document for bulk operation.

  - Remove fields starting with an underscore. These are service fields
    that are processed separately and should not be included into the
    resulting data.
  - If action is 'update' then 'doc_as_upsert' is set to 'true' if the data
    is complete. This means that the same document should be used as a new
    document to be indexed if there is no existing document to update.
    For incomplete data it is set to 'false', and different copies of data
    are provided for indexing and updating.

  :param row: document to be prepared
  :type row: array

  :return: prepared document
  :rtype: array
  */
  $data = $row;

  if (isset($data['_incomplete'])) {
    $incompl = $data['_incomplete'];
  }

  foreach ($data as $key => $val) {
    if (strncmp($key, '_', 1) === 0)
      unset($data[$key]);
  }

  $act = getAction($row);
  $insert_data = $data;
  $update_data = $data;
  if (isset($incompl)) {

    # "_update_required" field must be specified explicitly in two cases:
    #  * message is incomplete (so that record in ES gets properly marked);
    #  * we perform "update" operation (since we want to use 'doc_as_upsert' ES
    #    option, if possible, using "insert_data" for both cases --
    #    insert-if-missed and existing document update -- and still get
    #    resulting document properly marked)
    if ($incompl or $act == 'update') {
      $insert_data['_update_required'] = $incompl;
    }

    if (!$incompl) {
      # We must specify explicitly that update is not required after this operation
      # (just in case it was set to "true" previously).
      $update_data['_update_required'] = false;
    }
  }

  if ($act == 'update') {
    # We can do "clean upsert" (when insert and update documents are the same)
    # only when $incompl is NOT set to True:
    $clean_upsert = !(isset($incompl) and $incompl);
    $data = Array(
      'doc' => $update_data,
      'doc_as_upsert' => $clean_upsert
    );
    if (!$clean_upsert) {
      $data['upsert'] = $insert_data;
    }
  } else {
    $data = $insert_data;
  }

  return $data;
}

function decode_escaped($string) {
  return preg_replace('/\\\\([nrtvf\\\\$"]|[0-7]{1,3}|x[0-9A-Fa-f]{1,2})/e',
                      'stripcslashes("$0")', $string);
}

$opts = getopt("e:E:", Array("end-of-message:", "end-of-process:",
                             "update"));
$args = $argv;

foreach ($opts as $key => $val) {
  $match = preg_grep("/^-(-)?".$key."$/", $args);
  foreach ($match as $mkey => $mval) {
    unset($args[$mkey]);
    if ($val !== false) {
      unset($args[$mkey+1]);
    }
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
    case "update":
      $DEFAULT_ACTION = "update";
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

    $action = constructActionJson($row);
    $data = constructDataJson($row);

    echo json_encode($action)."\n";
    echo json_encode($data)."\n";
    echo $EOM_MARKER;
    echo $EOP_MARKER;
  }
}

fclose($h);

?>
