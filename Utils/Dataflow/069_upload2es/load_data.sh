#!/usr/bin/env bash

base_dir=$( cd "$( dirname "$( readlink -f "$0" )" )" && pwd )

ES_CONFIG="$base_dir/../../Elasticsearch/config/es"

ES_HOST='127.0.0.1'
ES_PORT='9200'
[ -f "$ES_CONFIG" ] && source "$ES_CONFIG"
[ -n "$ES_USER" -a "$ES_PASSWORD" ] && ES_AUTH="--user ${ES_USER}:${ES_PASSWORD}"

CURL_N_MAX=10
SLEEP=5
DELIMETER=`echo -e -n "\x00"`
EOProcess=`echo -e -n "\x06"`

log () {
  date | tr -d '\n' >&2
  echo ": $*" >&2
}

usage () {
  echo "
USAGE:
  `basename ${0}` [file1 file2 ...]
"
}

cmd="curl $ES_AUTH http://$ES_HOST:$ES_PORT/_bulk?pretty --data-binary @"

load_files () {
  if [ -z "$1" ] ; then
    log "(ERROR) Input file is not specified."
    usage >&2
    exit 2
  fi

  for INPUTFILE in $*;
  do
    ${cmd}${INPUTFILE}
  done
}

load_stream () {
  while read -r -d "$DELIMITER" line; do
    n=`ps axf | grep '[c]url' | grep "$HOST:$PORT" | wc -l`
    while [ $n -gt $CURL_N_MAX ]; do
      sleep $SLEEP
      n=`ps axf | grep '[c]url' | grep "$HOST:$PORT" | wc -l`
    done
    echo "$line" | ${cmd}- &
    echo -n "$EOProcess"
  done
}

if [ -z "$1" ] ; then
  load_stream
else
  load_files $*
fi
