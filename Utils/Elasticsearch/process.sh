#!/bin/sh

ORACLE_OUT=`mktemp`
ES_IN=`mktemp`

log() {
  date | tr -d '\n' >&2
  echo ": $*" >&2
}
getDataFromOracle() {
  #setp 1 - get dta from oracle
  log "Getting data from oracle"
  ./Oracle2JSON.py --config config/settings.cfg --input query/mc16_campaign_for_ES.sql || exit 1
}
convertDataToESFormat() {
  #step 2 - create data for ES
  log "Converting data to ES format"
  php oracle2es.php || exit 2
}
putDataToES() {
  #step 3 - load data to ES

  #some default settings and configs loading
  ES_CONFIG='config/es'
  log "Loading defaults and config $ES_CONFIG if any"
  ES_HOST='127.0.0.1'
  ES_PORT='9200'
  [ -f "$ES_CONFIG" ] && source "$ES_CONFIG"
  [ -n "$ES_USER" -a "$ES_PASSWORD" ] && ES_AUTH="--user ${ES_USER}:${ES_PASSWORD}"

  log "Putting data to ES"
  curl $ES_AUTH "http://$ES_HOST:$ES_PORT/_bulk?pretty" --data-binary @- || exit 3
}

getDataFromOracle > $ORACLE_OUT 
convertDataToESFormat < $ORACLE_OUT >$ES_IN 
putDataToES <$ES_IN
rm -f $ORACLE_OUT $ES_IN
