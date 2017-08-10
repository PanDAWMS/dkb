#!/bin/sh
curl -H "Content-Type: application/x-ndjson" -XPUT 'http://localhost:9200/_template/mc16?pretty' --data-binary "@mc16.mapping"
