#!/bin/sh
usage() {
  cat >&2 <<EOF
  Usage: $0 [-c <ElasticSearch config>] <mapping_file>
    -c path/to/confg/file. Default is $(dirname $0)/../config/es
EOF
  exit 1
}

ES_CONFIG="$(dirname $0)/../config/es"
ES_HOST='127.0.0.1'
ES_PORT='9200'
ES_PATH=''
ES_PROTO='http'

while getopts c: opt; do
  case $opt in		
    c) [ -f "$OPTARG" ] && ES_CONFIG="$OPTARG" || echo "Config $OPTARG not found! Will use default values!" >&2 ;;
    *) usage;
  esac
done
[ -f "$ES_CONFIG" ] && . $ES_CONFIG || echo "Could not found es_confg file $ES_CONFIG! Will use default values" >&2
[ -n "$ES_USER" -a "$ES_PASSWORD" ] && ES_AUTH="--user ${ES_USER}:${ES_PASSWORD}"
shift $((OPTIND-1)) 

[ "$ES_PATH" == '/' ] && ES_PATH=''

[ -z "$1" -o ! -f "${1}" ] \
  && echo "You must provide path to mapping file!" \
  || curl $ES_AUTH -H "Content-Type: application/x-ndjson" \
          -XPUT --data-binary "@${1}" \
          "${ES_PROTO}://${ES_HOST}:${ES_PORT}${ES_PATH}/_index_template/`basename ${1%.*}`?pretty"
