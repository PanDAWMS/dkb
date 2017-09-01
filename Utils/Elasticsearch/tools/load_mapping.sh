#!/bin/sh
usage() {
  cat >&2 <<EOF
  Usage: $0 [-e <ElasticSearch url>] <mapping_file>
    -e -- url where we may found elasticsearch. localhost:9200 is default
EOF
  exit 1
}

ES='localhost:9200'
while getopts e: opt; do
  case $opt in		
    e) ES="$OPTARG";;
    *) usage;
  esac
done
shift $((OPTIND-1)) 

[ -z "$1" -o ! -f "${1}" ] && echo "You must provide path to mapping file!" || \
curl -H "Content-Type: application/x-ndjson" -XPUT "http://${ES}/_template/`basename ${1%.*}`?pretty" --data-binary "@${1}"
