#!/usr/bin/env bash

usage() {
  echo "USAGE:
$(basename "$0") [-c CONFIG] [FILE...]

PARAMETERS:
  CONFIG -- configuration file
  FILE   -- file in NDJSON format for loading to Elasticsearch via bulk interface
"
}

base_dir=$( cd "$( dirname "$( readlink -f "$0" )" )" && pwd )

ES_CONFIG="${base_dir}/../../../Elasticsearch/config/es"

. "$base_dir"/../shell_lib/log

verify_ndjson() {
  while read -r json_line; do
    err=`echo "$json_line" | jq "." 2>&1 >/dev/null`
    [ -n "$err" ] && log WARN "Failed to parse input as JSON ($err): $json_line"
  done
}

jq_response_parser='
  (
    "Succesfully loaded: " + (
      if(.items) then (
        .items | map(select(.[].error == null)) | length
      ) else
        0
      end
      | tostring
    )
  ),
  (
    select(.errors) | .items[][] | select(.error)
    | "Failed to load record: " + ( {(._id): .error} | tostring )
  ),
  (
    select(.error) | "Failed to load record: " + (.error|tostring)
  )'

parse_bulk_response() {
  jq -c "$jq_response_parser" \
  | sed -E -e 's/(^|[^\])"/\1/g' -e 's/\\"/"/g' \
  | while read -r line; do
      [[ "$line" =~ Failed* ]] && lvl=ERROR || lvl=
      log $lvl "$line"
    done;
}

# ----------

while [ -n "$1" ]; do
  case "$1" in
    --config|-c)
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

cmd="curl -sS $ES_AUTH http://$ES_HOST:$ES_PORT/_bulk?pretty --data-binary @"

load_files () {
  [ -z "$1" -o ! -f "$1" ] && log $(usage) && exit 1

  log "Putting data to ES"
  for INPUTFILE in $*;
  do
    cat ${INPUTFILE} | verify_ndjson
    ${cmd}${INPUTFILE} | parse_bulk_response
    [ ${PIPESTATUS[0]} -ne 0 ] && exit 3
  done
  return 0
}

load_stream () {
  log "Switched to the stream mode."
  while read -r -d "$DELIMITER" line; do
    verify_ndjson <<< "$line"
    n=`ps axf | grep '[c]url' | grep "$HOST:$PORT" | wc -l`
    while [ $n -gt $CURL_N_MAX ]; do
      sleep $SLEEP
      n=`ps axf | grep '[c]url' | grep "$HOST:$PORT" | wc -l`
    done
    { echo "$line" | ${cmd}- | parse_bulk_response; } &
    echo -n "$EOProcess"
  done
  return 0
}

if [ -z "$1" ] ; then
  load_stream
else
  load_files $*
fi
