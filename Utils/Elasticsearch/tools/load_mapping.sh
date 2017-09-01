#!/bin/sh
usage() {
  cat >&2 <<EOF
  Usage: $0 [-e <ElasticSearch url>] [-a path/to/auth/file] mapping_file
    -e -- url where we may found elasticsearch. localhost:9200 is default
    -a -- auth information file for elasticsearch. File shoudl contain one string in following format:
      login:password
EOF
  exit 1
}

getAuthFromFile() {
  [ -f "$1" ] && cat "$1" || echo "File $1 not found. Auth will not be used." >&2 
}

ES='localhost'
while getopts e:a: opt; do
  case $opt in		
    e) ES="$OPTARG";;
    a) AUTH="$(getAuthFromFile $OPTARG)";;
    *) usage;
  esac
done
[ -n "$AUTH" ] && AUTH="-u $AUTH" 
shift $((OPTIND-1)) 

curl $AUTH -H "Content-Type: application/x-ndjson" -XPUT "http://${ES}:9200/_template/`basename ${1%.*}`?pretty" --data-binary "@${1}"
