<?php
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
