#!/bin/sh
curl -H "Content-Type: application/x-ndjson" -XPUT "http://localhost:9200/_template/`basename ${1%.*}`?pretty" --data-binary "@${1}"
