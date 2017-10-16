#!/bin/sh

base_dir=$( cd "$( dirname "$( readlink -f "$0" )" )" && pwd )

ES_CONFIG="$base_dir/../../Elasticsearch/config/es"

ES_HOST='127.0.0.1'
ES_PORT='9200'
[ -f "$ES_CONFIG" ] && source "$ES_CONFIG"
[ -n "$ES_USER" -a "$ES_PASSWORD" ] && ES_AUTH="--user ${ES_USER}:${ES_PASSWORD}"

curl $ES_AUTH "http://$ES_HOST:$ES_PORT/_bulk?pretty" --data-binary @${1}
