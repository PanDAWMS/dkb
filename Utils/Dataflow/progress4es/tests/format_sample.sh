#!/usr/bin/env sh

# Format data sample before loading it to the DKB ES

base_dir=$(cd "$(dirname "$(readlink -f "$0")")"; pwd)

ES_INDEX=daily_progress_sample
ES_DOC_TYPE=task_progress

[ -n "$1" ] && sample="$1" || sample="$base_dir/sample.json"

out="$sample".preload

[ -f "$out" ] && rm "$out"

while read -r line;
do
  echo "$line" | jq -c '{"index": {"_index":"'"$ES_INDEX"'", "_type": "'"$ES_DOC_TYPE"'", "_id": (( .date | tostring ) + "_" + ( .taskid | tostring ))}}, .' >> $out
done < "$sample"
