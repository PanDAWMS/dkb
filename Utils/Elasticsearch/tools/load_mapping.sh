#!/bin/sh
usage() {
  cat >&2 <<EOF
  Usage: $0 [-e <ElasticSearch url>] mapping_file
    -e -- url where we may found elasticsearch. localhost:9200 is default
EOF
  exit 1
}

ES='localhost'
while getopts e: opt; do
  case $opt in		
    e) ES="$OPTARG";;
    *) usage;
  esac
done
shift $((OPTIND-1)) 

curl -H "Content-Type: application/x-ndjson" -XPUT "http://${ES}:9200/_template/`basename ${1%.*}`?pretty" --data-binary "@${1}"
