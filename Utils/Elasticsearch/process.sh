#!/bin/sh

ORACLE_OUT=`mktemp`
ES_IN=`mktemp`

log() {
  echo "$(date): $*" >&2
}
getDataFromOracle() {
  #step 1 - get data from oracle
  log "Getting data from oracle"
  ./Oracle2JSON.py --config config/settings.cfg --input query/mc16_campaign_for_ES.sql || exit 1
}
convertDataToESFormat() {
  #step 2 - create data for ES
  log "Converting data to ES format"
  php oracle2es.php || exit 2
}

getDataFromOracle > $ORACLE_OUT 
convertDataToESFormat < $ORACLE_OUT >$ES_IN 
$(dirname $0)/tools/load_data.sh $ES_IN #step 3 - load data to ES
rm -f $ORACLE_OUT $ES_IN
