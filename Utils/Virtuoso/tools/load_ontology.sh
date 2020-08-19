#!/usr/bin/env bash

# Default values
HOST='nosql.tpu.ru'
PORT='8890'
GRAPH=
GRAPH_PATH='DAV/home/dba/ATLAS'

# File .credentials may contain variable definition for USER and PASSWD
if [ -f ".credentials" ]; then
  USER=`sed -n 1p .credentials`
  PASSWD=`sed -n 2p .credentials`
fi

CLEAR='n'

INPUTFILE="../ontology/ATLAS.owl"

usage () {
  echo "
USAGE
  ./load_ontology.sh [<options>] [<OWL_file>]

DEFAULT
  If OWL file is not specified, try to upload $INPUTFILE.

OPTIONS
 Virtuoso connection parameters:
  -H, --host <host>             hostname (can't be empty)
                                DEFAULT: $HOST
  -P, --port <port>             listen port (can't be empty)
                                DEFAULT: $PORT

  -u, --user <username>         user name.
                                DEFAULT: $USER
  -p, --password <password>     user password.
                                DEFAULT: $PASSWD
  -g, --graph <graph>           graph to load data to.
                                DEFAULT: http://<host>:<port>/$GRAPH_PATH

 Script parameters:
  -c, --clear                   Clear graph before uploading the ontology.
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
    -g|--graph)
      GRAPH="$2"
      shift
      ;;
    -u|--user)
      USER="$2"
      shift
      ;;
    -p|--password)
      PASSWD="$2"
      shift
      ;;
    -c|--clear)
      CLEAR="y"
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
[ -z "$USER" ] && echo "(ERROR) empty user value." >&2 && exit 2
[ -z "$PASSWD" ] && echo "(ERROR) empty password value." >&2 && exit 2
[ -z "$GRAPH" ] && GRAPH=http://$HOST:$PORT/$GRAPH_PATH

case "$CLEAR" in
  y)
    CLEAR="YES"
    ;;
  n)
    CLEAR="NO"
    ;;
  *)
    echo "Unknown value for option -c|--clear." >&2
    usage >&2
    exit 1
    ;;
esac


[ -n "$1" ] && INPUTFILE="$1" && echo "(WARN) No ontology file is passed. Try $INPUTFILE" >&2

if ! [ -s "$INPUTFILE" ]; then
  echo "(ERROR) FILE:$INPUTFILE is empty or unaccessable." >&2
  continue
fi

[ "x$CLEAR" = "xYES" ] && curl -F "query=CLEAR GRAPH <$GRAPH>"  http://$HOST:$PORT/sparql > /dev/null

cmd="curl -f -X POST --digest -u $USER:$PASSWD -H Content-Type:application/rdf+xml -T $INPUTFILE -G http://$HOST:$PORT/sparql-graph-crud-auth --data-urlencode graph=$GRAPH"

$cmd >/dev/null &&  { echo "(INFO) Ontology is successfully uploaded."; exit; }

e=$?
echo "(ERROR) An error occurs while uploading the ontology." >&2
exit $e
