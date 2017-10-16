#!/bin/sh -l

base_dir=$( cd "$( dirname "$0" )" && pwd )
ORACLE_OUT=`mktemp`
ES_IN=`mktemp`
OFFSET_FILE=$base_dir/.ora_offset

log() {
  date | tr -d '\n' >&2
  echo ": $*" >&2
}
getDataFromOracle() {
  #setp 1 - get dta from oracle
  log "Getting data from oracle"
  if [ -f "$OFFSET_FILE" ]; then
    source "$OFFSET_FILE"
    log "Last offset: $OFFSET"
  else
    log "No last offset found; use '01-01-1970 00:00:00'"
    OFFSET='01-01-1970 00:00:00'
  fi
  NEW_OFFSET=`date +"%d-%m-%Y %H:%M:%S"`
  $base_dir/Oracle2JSON.py --config $base_dir/config/settings.cfg --input $base_dir/query/mc16_campaign_for_ES.sql --offset "$OFFSET" || exit 1
}
convertDataToESFormat() {
  #step 2 - create data for ES
  log "Converting data to ES format"
  php $base_dir/oracle2es.php || exit 2
}
putDataToES() {
  #step 3 - load data to ES

  log "Putting data to ES"
  $base_dir/tools/load_data.sh - || exit 3
}

getDataFromOracle > $ORACLE_OUT 
if [ -s "$ORACLE_OUT" ]; then
  convertDataToESFormat < $ORACLE_OUT >$ES_IN
  putDataToES <$ES_IN
  echo OFFSET="'${NEW_OFFSET}'" > "$OFFSET_FILE"
else
  log "No data changed since last offset."
fi
rm -f $ORACLE_OUT $ES_IN
