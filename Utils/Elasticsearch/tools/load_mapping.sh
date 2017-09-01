#!/bin/sh
[ -z "$1" -o ! -f "${1}" ] && echo "First argument should be path to  mapping file!" || \
curl -H "Content-Type: application/x-ndjson" -XPUT "http://localhost:9200/_template/`basename ${1%.*}`?pretty" --data-binary "@${1}"
