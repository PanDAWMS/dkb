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
  php $base_dir/../Dataflow/019_oracle2esFormat/oracle2es.php || exit 2
}
putDataToES() {
  #step 3 - load data to ES

  #some default settings and configs loading
  ES_CONFIG="$base_dir/config/es"
  log "Loading defaults and config $ES_CONFIG if any"
  ES_HOST='127.0.0.1'
  ES_PORT='9200'
  [ -f "$ES_CONFIG" ] && source "$ES_CONFIG"
  [ -n "$ES_USER" -a "$ES_PASSWORD" ] && ES_AUTH="--user ${ES_USER}:${ES_PASSWORD}"

  log "Putting data to ES"
  curl $ES_AUTH "http://$ES_HOST:$ES_PORT/_bulk?pretty" --data-binary @- || exit 3
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
