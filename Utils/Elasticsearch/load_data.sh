#!/bin/sh

[ -z "$1" -o ! -f "$1" ] && echo "First argument should be a file in ndjson format for loading to Elasticsearch via bulk interface" || \
curl 'http://127.0.0.1:9200/_bulk?pretty' --data-binary @${1}
