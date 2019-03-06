#!/usr/bin/env bash
log() {
  echo "$(date): $*" >&2
}

usage() {
  echo "USAGE:
$(basename "$0") [-c CONFIG] [FILE...]

PARAMETERS:
  CONFIG -- configuration file
  FILE   -- file in NDJSON format for loading to Elasticsearch via bulk interface
"
}

base_dir=$( cd "$( dirname "$( readlink -f "$0" )" )" && pwd )

ES_CONFIG="${base_dir}/../../Elasticsearch/config/es"

while [ -n "$1" ]; do
  case "$1" in
    -c|--config)
      [ -n "$2" ] && ES_CONFIG="$2" || { usage >&2 && exit 1; }
      shift;;
    --)
      shift
      break;;
    -*)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1;;
    *)
      break;;
  esac
  shift
done

log "Loading defaults and config $ES_CONFIG if any"
ES_HOST='127.0.0.1'
ES_PORT='9200'

[ -f "$ES_CONFIG" ] && source "$ES_CONFIG"
[ -n "$ES_USER" -a "$ES_PASSWORD" ] && ES_AUTH="--user ${ES_USER}:${ES_PASSWORD}"

CURL_N_MAX=10
SLEEP=5
DELIMITER=`echo -e -n "\x00"`
EOProcess=`echo -e -n "\x06"`

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
    n=`ps axf | grep '[c]url' | grep "$HOST:$PORT" | wc -l`
    while [ $n -gt $CURL_N_MAX ]; do
      sleep $SLEEP
      n=`ps axf | grep '[c]url' | grep "$HOST:$PORT" | wc -l`
    done
    echo "$line" | ${cmd}- &
    echo -n "$EOProcess"
  done
}

if [ -z "$1" ] ; then
  load_stream
else
  load_files $*
fi
