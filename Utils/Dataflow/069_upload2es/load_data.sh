#!/bin/sh
log() {
  echo "$(date): $*" >&2
}

usage() {
  echo "USAGE:
$(basename "$0") FILE

PARAMETERS:
  FILE -- file in NDJSON format for loading to Elasticsearch via bulk interface
"
}

base_dir=$( cd "$( dirname "$( readlink -f "$0" )" )" && pwd )

[ -z "$1" -o ! -f "$1" ] && log $(usage) && exit 1
 
ES_CONFIG="$(base_dir)/../config/es"
log "Loading defaults and config $ES_CONFIG if any"
ES_HOST='127.0.0.1'
ES_PORT='9200'

[ -f "$ES_CONFIG" ] && source "$ES_CONFIG"
[ -n "$ES_USER" -a "$ES_PASSWORD" ] && ES_AUTH="--user ${ES_USER}:${ES_PASSWORD}"

log "Putting data to ES"
curl $ES_AUTH "http://$ES_HOST:$ES_PORT/_bulk?pretty" --data-binary @${1} || exit 3
