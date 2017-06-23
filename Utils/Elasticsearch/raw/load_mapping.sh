#!/bin/sh
curl -H "Content-Type: application/x-ndjson" -XPUT 'http://localhost:9200/_template/raw_current?pretty' --data-binary "@raw_.mapping"
