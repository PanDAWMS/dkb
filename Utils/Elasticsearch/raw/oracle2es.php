<?php
function convertIndexToLowerCase(&$a) {
  $result = array();

  foreach (array_keys($a) as $i) {
    $t = $a[$i];
    unset($a[$i]);
    $result[strtolower($i)] = $t;
  }
  $a = $result;
}

function strlowerMapping($mapping) {
  $result = array();

  foreach ($mapping as $pc => $pcm) {
    $pc = strtolower($pc);
    $result[$pc] = array();
    foreach ($pcm as $pctag) {
      $result[$pc][] = strtolower($pctag);
    }
  }

  return $result;
}

function getCategory($mapping, $tags) {
  $result = array();

  foreach ($tags as $tag) {
    foreach ($mapping as $c=>$mappingTags) {
      if (in_array(strtolower($tag), $mappingTags)) {
        $result[] = $c;
      }
    }
  }

  return $result;
}


function getPhysCat(array $tags, $taskname) {
  $result = array();
  #https://github.com/PanDAWMS/panda-bigmon-atlas/blob/devel/atlas/prodtask/hashtag.py
  $PHYS_CATEGORIES_MAP = array(
    "BPhysics" => array("charmonium","Jpsi","Bs","Bd","Bminus","Bplus",'CHARM','BOTTOM','BOTTOMONIUM','B0'),
    "BTag" => array("bTagging", "btagging"),
    "Diboson" => array("diboson","ZZ", "WW", "WZ", "WWbb", "WWll", "zz", "ww", "wz", "wwbb","wwll"),
    "DrellYan" => array("drellyan"),
    "Exotic" => array("exotic", "monojet", "blackhole", "technicolor", "RandallSundrum",
    "Wprime", "Zprime", "magneticMonopole", "extraDimensions", "warpedED",
    "randallsundrum", "wprime", "zprime", "magneticmonopole",
    "extradimensions", "warpeded", "contactInteraction","contactinteraction",'SEESAW'),
    "GammaJets" => array("photon", "diphoton"),
    "Higgs" => array("WHiggs", "ZHiggs", "mH125", "Higgs", "VBF", "SMHiggs", "higgs", "mh125",
    "zhiggs", "whiggs", "bsmhiggs", "chargedHiggs","BSMHiggs","smhiggs"),
    "Minbias" => array("minBias", "minbias"),
    "Multijet" => array("dijet", "multijet", "qcd"),
    "Performance" => array("performance"),
    "SingleParticle" => array("singleparticle"),
    "SingleTop" => array("singleTop", "singletop"),
    "SUSY" => array("SUSY", "pMSSM", "leptoSUSY", "RPV", "bino", "susy", "pmssm", "leptosusy", "rpv",'MSSM'),
    "Triboson" => array("tripleGaugeCoupling", "triboson", "ZZW", "WWW", "triplegaugecoupling", "zzw", "www"),
    "TTbar" => array("ttbar"),
    "TTbarX" => array("ttw","ttz","ttv","ttvv","4top","ttW","ttZ","ttV","ttWW","ttVV"),
    "Upgrade" => array("upgrad"),
    "Wjets" => array("W", "w"),
    "Zjets" => array("Z", "z"),
  );

  $PHYS_SHORT_MAP = array(
    'BPhysics'  => array('upsilon'),
    'Exotic'    => array('4topci'),
    'Higgs'     => array('h125', 'xhh'),
    'Multijet'  => array('jets'),
    'SingleTop' => array('singletop', '_wt', '_wwbb'),
    'SUSY'      => array('tanb'),
    'TTbar'     => array('ttbar', '_tt_'),
    'TTbarX'    => array('ttbb', 'ttgamma', '3top'),
    'Wjets'     => array('_wenu_'),
  );

  $PHYS_CATEGORIES_MAP = strlowerMapping($PHYS_CATEGORIES_MAP);
  $result = getCategory(strlowerMapping($PHYS_CATEGORIES_MAP), $tags);

  if (count($result) == 0 ) {
    $t = explode('.',$taskname);
    $phys_short = strtolower($t[2]);
    $result = getCategory(strlowerMapping($PHYS_SHORT_MAP), array($phys_short));
  }

  if (count($result) == 0 ) {
    $result[] = 'Uncategorized';
  }

  return $result;
}

$h = fopen($argv[1], "r");
$now = time();
if ($h) {
  while (($line = fgets($h)) !== false) {
    $row = json_decode($line,true);
    convertIndexToLowerCase($row);

    $hashtag_list = $row['hashtag_list'];
    $row['hashtag_list'] = array();
    foreach( explode(',',$hashtag_list) as $tag) {
      $row['hashtag_list'][] = trim($tag);
    }
    $row['physics_category'] = getPhysCat($row['hashtag_list'], $row['taskname']);

    printf('{ "index" : {"_index":"mc16", "_type":"event_summary", "_id":"%d" } }'."\n", $row['taskid']);
     
    echo json_encode($row)."\n";
  }
}

fclose($h);

?>
