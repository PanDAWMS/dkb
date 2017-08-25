#!/bin/sh
INDEX_NAME="${1:-raw_current-*}"
curl -XDELETE "http://localhost:9200/${INDEX_NAME}?pretty"
