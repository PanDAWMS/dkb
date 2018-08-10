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

OPTIONS:
  -b, --batch {e[abled]|d[isabled]} Specifies batch-mode: (e)nabled|(d)isabled.
  -B, --eob <EOB>                   Specifies the delimiter between sets of input
                                    data in the stream mode.
                                    Default: \x17
                                    No batch-mode: \n
  -E, --eop <EOP>                   Specifies end-of-process marker.
                                    Default:
                                    * in a (f)ile mode: none
                                    * in a (s)tream mode: \0
"
}

base_dir=$( cd "$( dirname "$( readlink -f "$0" )" )" && pwd )

ES_CONFIG="${base_dir}/../../Elasticsearch/config/es"

while [ -n "$1" ]; do
  case "$1" in
    -c|--config)
      [ -n "$2" ] && ES_CONFIG="$2" || { usage >&2 && exit 1; }
      shift;;
    -h|--help)
      usage
      exit;;
    -E|--eop)
      EOP="$2"
      shift;;
    -B|--eob)
      EOB="$2"
      shift;;
    -b|--batch)
      BATCHMODE="${2,,}"
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
EOBatch='\x17'
BATCHMODE="d"

case $BATCHMODE in
  e|enabled)
    [ -z "$EOB" ] && EOBatch="\n"
    ;;
  d|disabled)
    ( [ -z "$EOB" ] || [ $EOB == "\x17" ] || [ $EOBatch == "\x17" ] ) && EOBatch="\n"
    ( [ -n "$EOB" ] && [ ! $EOB == "\x17" ] ) && EOBatch="$EOB"
    ;;
  *)
    log "Unexpected batch-mode parameter." && { [ -n "$BATCHMODE" ] && usage && return 2; }
    break
    ;;
esac

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
  if ((`echo -ne "$EOBatch" | wc -c` > 1)) ; then
    log "Fail to switch to the stream mode. "`
               `"End-of-batch marker is too long (> 1 byte)."
  else
    log "Switched to the stream mode."
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
