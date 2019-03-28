#/usr//bin/env bash

base_dir=$(cd "$(dirname "$(readlink -f "$0")")"; pwd)

usage() {
  echo "USAGE:
$(basename "$0") [-c CONFIG] [--] [ARGS]
  
PARAMETERS:
  CONFIG -- configuration file
  ARGS   -- arguments to be passed to the PHP script
"
}

ES_CONFIG=$base_dir/../../Elasticsearch/config/es

while [ -n "$1" ]; do
  case "$1" in
    --config|-c)
      [ -n "$2" ] && ES_CONFIG="$2" || { usage >&2 && exit 1; }
      shift;;
    --)
      shift
      break;;
    *)
      break;;
  esac
  shift
done

[ -r "$ES_CONFIG" ] \
  || { echo "Can't access configuration file: $ES_CONFIG" >&2 \
       && exit 1; }

set -a
  . "$ES_CONFIG"
set +a

$base_dir/esFormat.php "$@"
