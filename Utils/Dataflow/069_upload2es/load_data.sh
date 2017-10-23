#!/usr/bin/env bash
log() {
  echo "$(date): $*" >&2
}

usage() {
  echo "USAGE:
$(basename "$0") [FILE...]

PARAMETERS:
  FILE -- file in NDJSON format for loading to Elasticsearch via bulk interface
"
}

base_dir=$( cd "$( dirname "$( readlink -f "$0" )" )" && pwd )

ES_CONFIG="$(base_dir)/../config/es"
log "Loading defaults and config $ES_CONFIG if any"
ES_HOST='127.0.0.1'
ES_PORT='9200'

[ -f "$ES_CONFIG" ] && source "$ES_CONFIG"
[ -n "$ES_USER" -a "$ES_PASSWORD" ] && ES_AUTH="--user ${ES_USER}:${ES_PASSWORD}"

DELIMETER=`echo -e -n "\x00"`

cmd="curl $ES_AUTH http://$ES_HOST:$ES_PORT/_bulk?pretty --data-binary @"

load_files () {
  [ -z "$1" -o ! -f "$1" ] && log $(usage) && exit 1

  log "Putting data to ES"
  for INPUTFILE in $*;
  do
    ${cmd}${INPUTFILE} || exit 3
  done
}

load_stream () {
  log "Switched to the stream mode."
  while read -r -d "$DELIMITER" line; do
    echo "$line" | ${cmd}-
  done
}

if [ -z "$1" ] ; then
  load_stream
else
  load_files $*
fi
