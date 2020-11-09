#!/bin/bash

base_dir=$( cd "$(dirname "$(readlink -f "$0")")"; pwd)
functions="${base_dir}/../../Dataflow/shell_lib"
. "${functions}/log"

# Configuration
# -------------

transfer_file="${base_dir}/transfer_pipe"

scroll_log="${base_dir}/last_scroll_id"
load_data_log="${base_dir}/last_load_data"
load_log="${base_dir}/last_load_response"
tid_log="${base_dir}/last_tid"

transform_jq="${base_dir}/transform-to-nested.jq"

# STDERR will be redirected to this file if specified
logfile=

DEBUG=
NOOP=


SRC_INDEX='production_tasks'
TGT_INDEX='production_tasks-nested'

SRC_ENDPOINT="http://127.0.0.1:9200/${SRC_INDEX}/task/_search"
TGT_ENDPOINT="http://127.0.0.1:9200/_bulk"
SCROLL_ENDPOINT="http://127.0.0.1:9200/_search/scroll"

SCROLL_SIZE=500

# Functions
# ---------

extract_query='
{
  "query": {
    "bool": {
      "should": [
        {"match_all": {}},
        {"has_child": {
          "type": "output_dataset",
          "query": {"match_all": {}},
          "inner_hits": {
            "name": "datasets",
            "size": 100
          }
        }}
      ],
      "must": {
        "range": {
          "taskid": {"gt": %%LAST_TID%%}
        }
      }
    }
  },
  "sort": [
    {"taskid": {"order": "asc"}}
  ]
}
'

last_tid() {
  [ -s "$tid_log" ] && cat "$tid_log" || echo 0
}

save_tid() {
  [ -s "$load_data_log" ] || { log "Failed to save last tid: load log" \
                                   "not found or empty (${load_data_log})." \
                               && return 1; }

  tail -n 1 "$load_data_log" | jq ".taskid" > "$tid_log"
}

scroll_query() {
  LAST_TID=$(last_tid)
  log "Getting records $N-$((N+SCROLL_SIZE)) (tid > $LAST_TID)."
  if [ -z "$1" ]; then
    log "Creating scroll query."
    [ -n "$DEBUG" ] && set -x
    echo "$extract_query" | sed -e "s/%%LAST_TID%%/$LAST_TID/" \
      | curl -X POST "${SRC_ENDPOINT}?scroll=5m&size=$SCROLL_SIZE" \
             -H 'Content-Type: application/json' \
             -d @-
    set +x
  else
    [ -n "$DEBUG" ] && log TRACE "Querying scroll API with ID: ${scroll_id}."
    [ -n "$DEBUG" ] && set -x
    curl -X POST "${SCROLL_ENDPOINT}" \
         -H 'Content-Type: application/json' \
         -d "{\"scroll_id\": $1, \"scroll\": \"5m\"}"
    set +x
  fi \
  | tee "$transfer_file" \
  | jq '._scroll_id'
}

transform() {
  [ -n "$DEBUG" ] && log TRACE "Transforming data."
  jq --arg TGT_INDEX "${TGT_INDEX}" -c -f "$transform_jq"
}

load_nested() {
  if [ -z "$NOOP" ]; then
    log INFO "Loading data."
    curl -X POST "$TGT_ENDPOINT" \
      -H 'Content-Type: application/json' \
      --data-binary @-
  else
    cat > /dev/null
    echo '{"errors": false}'
  fi
}

transform_and_index() {
  cat "$transfer_file" \
    | transform \
    | tee "$load_data_log" \
    | load_nested \
    | tee "$load_log" \
    | jq ".errors"
}

delete_scroll() {
  [ -z "$1" ] \
    || {
         log INFO "Removing scroll with ID: $1"
         [ -n "$DEBUG" ] && set -x
         curl -sS -X DELETE ${SCROLL_ENDPOINT}/?pretty \
              -H 'Content-Type: application/json' \
              -d'{"scroll_id": ['"$1"']}'
         set +x
       }
}


# Program body
# ------------

# Redirect STDERR to logfile if specified
[ -n "$logfile" ] && exec 2>>"$logfile"

N=0

scroll_id=$(scroll_query "$scroll_id")
echo "$scroll_id" > "$scroll_log"

errors=$(transform_and_index)

while [ "$errors" = "false" ]; do
  save_tid || break
  N=$((N+SCROLL_SIZE))
  scroll_id=$(scroll_query "$scroll_id")
  echo "$scroll_id" > "$scroll_log"
  errors=$(transform_and_index)
done

[ "$errors" != "false" ] && log ERROR "$errors"

delete_scroll "$scroll_id"
rm "$transfer_file"
