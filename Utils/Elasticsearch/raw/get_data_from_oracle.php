<?php
function exception_error_handler($errno, $errstr, $errfile, $errline ) {
   throw new ErrorException($errstr, 0, $errno, $errfile, $errline);
}
set_error_handler("exception_error_handler");

fwrite(STDERR, "Password:\n");
fscanf(STDIN, "%s\n", $p);

$query_file = __DIR__.'/../../OracleProdSys2/mc16_campaign_for_ES.sql';
$replaces = array(
  '/--.*\n/' => '',
  '/\s+/' => ' ',
  '/;\s*$/' => ''
);
$query = preg_replace(array_keys($replaces), array_values($replaces), file_get_contents($query_file));
fwrite(STDERR, "Will send query:\n\n$query\n\n");
$conn = oci_connect('atlas_deft_r', $p, 'adcr_adg');
if (!$conn) {
    $e = oci_error();
    trigger_error(htmlentities($e['message'], ENT_QUOTES), E_USER_ERROR);
}

$stid = oci_parse($conn, $query);
fwrite(STDERR, "Parsed!\n");

oci_execute($stid);
fwrite(STDERR, "Execution done!\n");

while ($row = oci_fetch_array($stid, OCI_ASSOC)) {
  unset($row['TAG_PARAMETERS']);
  try {
    echo json_encode($row)."\n";
  } catch (Exception $e) {
    fwrite(STDERR, "Bad row '".print_r($row,true)."'\n");
    fwrite(STDERR, $e->getMessage()."\n");
    die();
  }

}

?>
