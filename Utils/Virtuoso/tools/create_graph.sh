#!/usr/bin/env bash

# Creates specified graph explicitly
# so that it can be deleted later with DROP command.
# TODO: add authentication when Virtuoso becomes secure.

# Default values
HOST='nosql.tpu.ru'
PORT='8890'

DROP='y'


usage () {
  echo "
USAGE
  ./create_graph.sh [<options>] GraphIRI

DEFAULT
  

OPTIONS
 Virtuoso connection parameters:
  -H, --host <host>             hostname (can't be empty)
                                DEFAULT: $HOST
  -P, --port <port>             listen port (can't be empty)
                                DEFAULT: $PORT

 Script parameters:
  -d, --drop                    Try to drop the graph before creating.
  -h, --help                    Print this message and exit.
"
}

while [[ $# > 0 ]]
do
  key="$1"
  case $key in
    -h|--help)
      usage
      exit
      ;;
    -H|--host)
      HOST="$2"
      shift
      ;;
    -p|--port)
      PORT="$2"
      shift
      ;;
    -d|--drop)
      DROP="YES"
      ;;
    --)
      break
      ;;
    -*)
      echo "Unknown option: $key."
      usage
      exit 1
      ;;
    *)
      break
      ;;
  esac
  shift
done

[ -z "$HOST" ] && echo "(ERROR) empty host value." >&2 && exit 2
[ -z "$PORT" ] && echo "(ERROR) empty port value." >&2 && exit 2

if [ -n "$1" ]; then
 GRAPH="$1"
else
  echo "(ERROR) No GraphIRI specified." >&2
  exit 2
fi

[ "x$DROP" = "xYES" ] && curl -F "query=DROP GRAPH <$GRAPH>"  http://$HOST:$PORT/sparql > /dev/null

q="CREATE GRAPH <$GRAPH>"

curl -F "query=$q" http://$HOST:$PORT/sparql > /dev/null && exit

e=$?
echo "(ERROR) An error occurs while creating the graph." >&2
exit $e
