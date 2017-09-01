#!/bin/sh
usage() {
  cat >&2 <<EOF
  Usage: $0 [-u <ElasticSearch url>] [-p <port>] [-a path/to/auth/file] [-c path/to/es/config] mapping_file
    -u -- url where we may found elasticsearch. localhost is default
    -p -- port where we may found elasticseatch at $ES_HOST
    -a -- auth information file for elasticsearch. File shoudl contain one string in following format:
      login:password
    -c -- elasticsearch config file path. THis file may contain ES_HOST, ES_PORT, ES_USER and ES_PASSWORD variables
EOF
  exit 1
}

getAuthFromFile() {
  [ -f "$1" ] && cat "$1" || echo "File $1 not found. Auth will not be used." >&2 
}

ES_HOST='localhost'
ES_PORT=9200
while getopts u:p::a:c: opt; do
  case $opt in		
    u) ES_HOST="$OPTARG";;
    p) ES_PORT="$OPTARG";;
    a) AUTH="$(getAuthFromFile $OPTARG)";;
    c) 
      [ -f "$OPTARG" ] && source "$OPTARG" || echo "Could not find file $OPTARG. Config not loaded!" >&2
      [ -n "$ES_USER" -a -n "$ES_PASSWORD" ] && AUTH="$ES_USER:$ES_PASSWORD"
    ;;
    *) usage;
  esac
done
[ -n "$AUTH" ] && AUTH="-u $AUTH" 
shift $((OPTIND-1)) 

curl $AUTH -H "Content-Type: application/x-ndjson" -XPUT "http://${ES_HOST}:${ES_PORT}/_template/`basename ${1%.*}`?pretty" --data-binary "@${1}"
