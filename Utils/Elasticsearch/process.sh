#!/bin/sh

ORACLE_OUT=`mktemp`
ES_IN=`mktemp`
OFFSET_FILE=./.ora_offset

log() {
  echo "$(date): $*" >&2
}
getDataFromOracle() {
  #step 1 - get data from oracle
  log "Getting data from oracle"
  if [ -f "$OFFSET_FILE" ]; then
    source "$OFFSET_FILE"
    log "Last offset: $OFFSET"
  else
    log "No last offset found; use '01-01-1970 00:00:00'"
    OFFSET='01-01-1970 00:00:00'
  fi
  NEW_OFFSET=`date +"%d-%m-%Y %H:%M:%S"`
  ./Oracle2JSON.py --config config/settings.cfg --input query/mc16_campaign_for_ES.sql --offset "$OFFSET" || exit 1
}
convertDataToESFormat() {
  #step 2 - create data for ES
  log "Converting data to ES format"
  php oracle2es.php || exit 2
}

getDataFromOracle > $ORACLE_OUT 
convertDataToESFormat < $ORACLE_OUT >$ES_IN 
$(dirname $0)/tools/load_data.sh $ES_IN #step 3 - load data to ES
echo OFFSET="'${NEW_OFFSET}'" > "$OFFSET_FILE"
rm -f $ORACLE_OUT $ES_IN
