#/usr/bin/env bash

base_dir=$(cd "$(dirname "$(readlink -f "$0")")"; pwd)

usage() {
  echo "USAGE:
$(basename "$0") [-h] [-c CONFIG] [--] [ARGS]

Use -h or --help to get this help message.

PARAMETERS:
  CONFIG -- Elasticsearch configuration file
  ARGS   -- arguments to be passed to the PHP script
"
}

ES_CONFIG=$base_dir/../../Elasticsearch/config/es
CONFIG_DEFAULT=TRUE

while [ -n "$1" ]; do
  case "$1" in
    --help|-h)
      usage >&2 && exit 1;;
    --config|-c)
      [ -n "$2" ] && { ES_CONFIG="$2" && CONFIG_DEFAULT=""; } \
                  || { usage >&2 && exit 1; }
      shift;;
    --)
      shift
      break;;
    *)
      break;;
  esac
  shift
done

[ $CONFIG_DEFAULT ] && echo No config file specified, using \
                            the default value: $ES_CONFIG >&2

[ -r "$ES_CONFIG" ] \
  || { echo "Can't access configuration file: $ES_CONFIG" >&2 \
       && exit 1; }

set -a
  . "$ES_CONFIG"
set +a

$base_dir/esFormat.php "$@"
