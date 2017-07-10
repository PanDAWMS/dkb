<?php
fscanf(STDIN, "%s\n", $p);

$query_file = __DIR__.'/../../OracleProdSys2/mc16_campaign_for_ES.sql';
$replaces = array(
  '/--.*\n/' => '',
  '/\s+/' => ' ',
  '/;\s*$/' => ''
);
$query = preg_replace(array_keys($replaces), array_values($replaces), file_get_contents($query_file));

$conn = oci_connect('atlas_deft_r', $p, 'adcr_adg');
if (!$conn) {
    $e = oci_error();
    trigger_error(htmlentities($e['message'], ENT_QUOTES), E_USER_ERROR);
}

$stid = oci_parse($conn, $query);
#echo "Parsed!\n";

oci_execute($stid);
#echo "Execution done!\n";

while ($row = oci_fetch_array($stid, OCI_ASSOC)) {
  echo json_encode($row)."\n";
}

?>
