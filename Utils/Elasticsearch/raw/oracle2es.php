<?php
function getPhysCat(array $tags) {
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
  foreach ($PHYS_CATEGORIES_MAP as $pc => $pcm) {
    unset($PHYS_CATEGORIES_MAP[$pc]);
    $pc = strtolower($pc);
    $PHYS_CATEGORIES_MAP[$pc] = array();
    foreach ($pcm as $pctag) {
      $PHYS_CATEGORIES_MAP[$pc][] = strtolower($pctag);
    }
  }
  foreach ($tags as $tag) {
    foreach ($PHYS_CATEGORIES_MAP as $pc=>$tctags) {
      if (in_array(strtolower($tag), $tctags)) {
        $result[] = $pc;
      }
    }
  }
  if (count($result) == 0 ) {
    $result[] = 'unknown';
  }
  return $result;
}

$h = fopen($argv[1], "r");
$now = time();
if ($h) {
  while (($line = fgets($h)) !== false) {
    $row = json_decode(strtolower($line),true);
    if (!isset($row['sub_campaing'])) {
      $row['sub_campaing'] = $row['campaing'];
    }
    printf('{ "index" : {"_index":"raw_current-%s", "_type":"%s", "_id":"%d" } }'."\n", $row['step'], $row['status'], $row['task_id']);
    $row['hashtag'] = explode(',', $row['hashtag_list']);
    $row['physics_category'] = getPhysCat($row['hashtag']);
    $row['extended_tags'] = $row['hashtag'];
    $tn = explode('.', $row['taskname']);
    if (isset($tn[2])) {
      $row['physics_short_full'] = $tn[2];
      $row['physics_short'] = explode('_', $row['physics_short_full']);
      foreach ($row['physics_short'] as $ps) {
        $row['extended_tags'][] = $ps;
      }
    }
    $row['prod_tags'] = explode('_', $tn[count($tn)-1]);
    $additional_hashtags = array('campaing', 'sub_campaing', 'phys_group', 'prod_tags', 'physics_category');


    foreach ($additional_hashtags as $addon) {
      if (isset($row[$addon])) {
        if (is_array($row[$addon])) {
          foreach($row[$addon] as $ra) {
            if ($ra != '') {
              $row['extended_tags'][] = $ra;
            }
          }
        } else {
          if ($row[$addon] != '') {
            $row['extended_tags'][] = $row[$addon];
          }
        }
      }
    }
    foreach( $row['physics_short'] as $tn) {
      $row['extended_tags'][] = $tn;
    }
    $row['@timestamp'] = $row['t_stamp'];
    unset($row['task_id'], $row['status'], $row['hashtag_list'], $row['t_stamp']);
    echo json_encode($row)."\n";
  }
}

fclose($h);

?>
