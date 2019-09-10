#!/bin/bash

transfer_file="transfer_pipe"

N=0
echo "Getting records $N-$((N+500))."
scroll_id=`curl -X POST -H 'Content-Type: application/json' 'http://127.0.0.1:9200/prodsys_rucio_ami_20190903/task/_search?scroll=5m&size=500' -d @extract_query.json \
  | tee "transfer_file" | jq '._scroll_id'`
echo "Scroll ID: $scroll_id" | tee scroll_id.log

#transform and index data
echo "Indexing..."
errors=`cat "transfer_file" \
  | { jq -c '.hits.hits[] | {"index": {"_index": "tasks_nested", "_type": "task", "_id": ._id} }, {"output_dataset": .inner_hits.datasets.hits.hits | map(._source | .name = .datasetname | del(.datasetname))} + ._source' && echo ''; } \
  | tee load.log \
  | curl -s 'http://127.0.0.1:9200/_bulk?pretty' -H 'Content-Type: application/json' --data-binary @- | tee -a load.log | jq ".errors"`

while [ "$errors" = "false" ]; do
  N=$((N+500))
  echo "Getting records $N-$((N+500))."
  echo "CURL: curl -X POST -H 'Content-Type: application/json' 'http://127.0.0.1:9200/_search/scroll?pretty' -d'"'{"scroll_id": '$scroll_id', "scroll": "5m"}'"'"
  scroll_id=`curl -X POST -H 'Content-Type: application/json' 'http://127.0.0.1:9200/_search/scroll?pretty' -d'{"scroll_id": '$scroll_id', "scroll": "5m"}' \
    | tee "transfer_file" | jq '._scroll_id'`
  echo "Scroll ID: $scroll_id" | tee scroll_id.log
  echo "Indexing..."
  errors=`cat "transfer_file" \
    | { jq -c '.hits.hits[] | {"index": {"_index": "tasks_nested", "_type": "task", "_id": ._id} }, {"output_dataset": .inner_hits.datasets.hits.hits | map(._source | .name = .datasetname | del(.datasetname))} + ._source' && echo ''; } \
    | tee load.log \
    | curl -s 'http://127.0.0.1:9200/_bulk?pretty' -H 'Content-Type: application/json' --data-binary @- | tee -a load.log | jq ".errors"`
done

curl -X DELETE 'http://127.0.0.1:9200/_search/scroll/?pretty' -H 'Content-Type: application/json' -d'{"scroll_id": ['$scroll_id']}'
