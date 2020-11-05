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

# STDERR will be redirected to this file if specified
logfile=


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
      ]
    }
  }
}
'

scroll_query() {
  log "Getting records $N-$((N+SCROLL_SIZE))."
  if [ -z "$1" ]; then
    log "Creating scroll query."
    cmd="
      curl -X POST '${SRC_ENDPOINT}?scroll=5m&size=$SCROLL_SIZE'
        -H 'Content-Type: application/json'
        -d '${extract_query}'
    "
    log DEBUG "cURL: $cmd"
    $cmd
  else
    log TRACE "Querying scroll API with ID: ${scroll_id}."
    cmd="
      curl -X POST '${SCROLL_ENDPOINT}'
        -H 'Content-Type: application/json'
        -d '{\"scroll_id\": $1, \"scroll\": \"5m\"}'
    "
    log DEBUG "cURL: $cmd"
    $cmd
  fi \
  | tee "$transfer_file" \
  | jq '._scroll_id'
}

transform_to_nested() {
  log TRACE "Transforming data."
  jq --arg TGT_INDEX "${TGT_INDEX}" -c -f reindex-transform.jq
}

load_nested() {
  log TRACE "Loading data."
  curl -sS -X POST "$TGT_ENDPOINT" \
    -H 'Content-Type: application/json' \
    --data-binary @-
}

transform_and_index() {
  cat "$transfer_file" \
    | transform_to_nested \
    | tee "$load_data_log" \
    | load_nested \
    | tee "$load_log" \
    | jq ".errors"
}

delete_scroll() {
  [ -z "$1" ] \
    || {
         cmd="
               curl -X DELETE '${SCROLL_ENDPOINT}/?pretty'
                    -H 'Content-Type: application/json'
                    -d'{"'"scroll_id"'": [$1]}'
         "
         log DEBUG "cURL: $cmd"
         $cmd
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
  N=$((N+SCROLL_SIZE))
  scroll_id=$(scroll_query "$scroll_id")
  echo "$scroll_id" > "$scroll_log"
  errors=$(transform_and_index)
done

[ "$errors" != "false" ] && log ERROR "$errors"

delete_scroll "$scroll_id"
rm "$transfer_file"
