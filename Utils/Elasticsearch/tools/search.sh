#!/bin/sh
ES_HOST='localhost'
ES_PORT=9200
[ -f "config/es" ] && source "config/es"
[ -n "$ES_USER" -a -n "$ES_PASSWORD" ] && AUTH="-u $ES_USER:$ES_PASSWORD"
curl $AUTH -H "Content-Type: application/x-ndjson" -XPOST "http://$ES_HOST:$ES_PORT/prodsys/_search?pretty" --data-binary "@$1"
