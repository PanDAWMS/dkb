#/usr//bin/env bash

base_dir=$(cd "$(dirname "$(readlink -f "$0")")"; pwd)

ES_CONFIG=$base_dir/../../Elasticsearch/config/es

[ -r "$ES_CONFIG" ] \
  || { echo "Can't access configuration file: $ES_CONFIG" >&2 \
       && exit 1; }

set -a
  . "$ES_CONFIG"
set +a

./esFormat.php
