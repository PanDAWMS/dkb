#/usr/bin/env bash

base_dir=$(cd "$(dirname "$(readlink -f "$0")")"; pwd)

ES_CONFIG=$base_dir/../../../Elasticsearch/config/es
CONFIG_DEFAULT=TRUE

usage() {
  echo "usage: $(basename "$0") [-h] [-c CONFIG] [-- [ARGS]]
  
optional arguments:
  -h, --help   show this help message and exit

  -c CONFIG    configuration file with parameters required to
               prepare data for indexing in Elasticsearch
               Default value: $ES_CONFIG

  --           separator for explicit division of arguments to
               be passed further to PHP script (after) and 
               ones indended for this script (before).

ARGS, arguments to be passed to the PHP script:"
# Display part of esFormat.php's help describing its arguments.
$base_dir/esFormat.php -h 2>&1 | sed 1,5d >&2
}

while [ -n "$1" ]; do
  case "$1" in
    --help|-h)
      usage >&2; exit 0;;
    --config|-c)
      [ -n "$2" ] && ES_CONFIG="$2" && CONFIG_DEFAULT="" \
                  || { usage >&2; exit 1; }
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
