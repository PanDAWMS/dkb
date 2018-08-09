#!/usr/bin/env bash
log() {
  echo "$(date): ($1): ${@:2}" >&2
}

usage() {
  echo "USAGE:
$(basename "$0") [FILE...]

PARAMETERS:
  FILE -- file in NDJSON format for loading to Elasticsearch via bulk interface
"
}

while [[ $# > 1 ]]
do
  key="$2"
  case $key in
    -h|--help)
      usage
      exit
      ;;
    -E|--eop)
      EOP="$3"
      shift
      ;;
    -B|--eob)
      EOB="$3"
      shift
      ;;
    -*)
      log ERROR "Unknown option: $key" $(usage)
      exit 1
      ;;
    *)
      break
      ;;
  esac
  shift
done

base_dir=$( cd "$( dirname "$( readlink -f "$0" )" )" && pwd )

ES_CONFIG="${base_dir}/../../Elasticsearch/config/es"
log INFO "Loading defaults and config $ES_CONFIG if any"
ES_HOST='127.0.0.1'
ES_PORT='9200'

[ -f "$ES_CONFIG" ] && source "$ES_CONFIG"
[ -n "$ES_USER" -a "$ES_PASSWORD" ] && ES_AUTH="--user ${ES_USER}:${ES_PASSWORD}"

CURL_N_MAX=10
SLEEP=5
EOBatch=$'\4'

cmd="curl $ES_AUTH http://$ES_HOST:$ES_PORT/_bulk?pretty --data-binary @"

load_files () {
  [ -z "$1" -o ! -f "$1" ] && log NOTSET $(usage) && exit 1

  log INFO "Putting data to ES"
  for INPUTFILE in $*;
  do
    ${cmd}${INPUTFILE} || exit 3
  done
}

load_stream () {
  if ((`echo -ne "$EOBatch" | wc -c` > 1)) ; then
    log ERROR "Fail to switch to the stream mode. "`
               `"End-of-batch marker is too long (> 1 byte)."
  else
    log INFO "Switched to the stream mode."
    while read -r -d `echo -ne "$EOBatch"` line; do
      n=`ps axf | grep '[c]url' | grep "$HOST:$PORT" | wc -l`
      while [ $n -gt $CURL_N_MAX ]; do
        sleep $SLEEP
        n=`ps axf | grep '[c]url' | grep "$HOST:$PORT" | wc -l`
      done
      echo "$line" | ${cmd}- &
      echo -ne "$EOProcess"
    done
  fi
}

if [ -z "$1" ] ; then
  if [ -z "$EOP" ] ; then
    EOProcess="\0"
  else
    EOProcess="$EOP"
  fi
  load_stream
else
  if [ -z "$EOP" ] ; then
    EOProcess=""
  else
    EOProcess="$EOP"
  fi
  load_files $*
fi
