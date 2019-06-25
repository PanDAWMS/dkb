#!/usr/bin/env bash

# upload2virtuoso.sh


# Default values
HOST='nosql.tpu.ru'
PORT='8890'
GRAPH=
GRAPH_PATH='DAV/ATLAS'
MODE="f"
DELIMITER="NOT SPECIFIED"

# File .credentials may contain variable definition for USER and PASSWD
if [ -f ".credentials" ]; then
  USER=`sed -n 1p .credentials`
  PASSWD=`sed -n 2p .credentials`
fi


SLEEP=1
CURL_N_MAX=10

BATCHMODE="d"
EOB_DEFAULT="NOT_SPECIFIED"
EOB="$EOB_DEFAULT"
EOMessage='\n'

EOP_set="N"
EOB_set="N"
EOM_set="N"

usage () {
  echo "
Upload TTL or SPARQL query files to Virtuoso.

USAGE:
  upload2virtuoso [<options>] [--] <input_files> 

PARAMETERS:
  <input_files>  - Space separated list of the input files.
                   Use '-' for STDIN.

OPTIONS:
 Virtuoso connection parameters:
  -H, --host <host>             hostname (can't be empty)
                                Default: $HOST
  -P, --port <port>             listen port (can't be empty)
                                Default: $PORT

  -u, --user <username>         user name.
                                Default: $USER
  -p, --password <password>     user password.
                                Default: $PASSWD
  -g, --graph <graph>           graph to load data to.
                                Default: http://<host>:<port>/$GRAPH_PATH

 Script parameters:
  -t, --type {t[tl]|s[parql]}   Input files type.
                                If not specified, try to understand from file
                                extention.
  -m, --mode {s|f}              Operating mode: (s)tream, (f)ile or (k)afka:
                                * in a (f)ile mode reads <input_files>;
                                * in a (s)tream mode waits for input from STDIN,
                                  sending data to Virtuoso;
                                  messages are to be delimited by <delimiter>.
  -e, --eom                     Specifies EOM (End-of-message) marker.
                                Default: '\n'
  -E, --eop                     Specifies EOP (End-of-process) marker.
                                Default:
                                File mode: ''
                                Stream mode: '\0'
  -b, --batch {e[abled]|d[isabled]} Specifies batch-mode: (e)nabled|(d)isabled.
  -B, --eob <EOB>               Specifies the delimiter between sets of input
                                Default ('\x11' is a random one):

   -b   ||  X   |  X   |   X    |   X    ||  'e'   | 'e'  |  'e'   |  'e'   |
------- || ---- | ---- | ------ | ------ || ------ | ---- | ------ | ------ |
   -B   ||  X   |  ''  | '\x17' | '\x11' ||   X    |  ''  | '\x17' | '\x11' |
======= || ==== | ==== | ====== | ====== || ====== | ==== | ====== | ====== |
EOBatch || '\n' | '\n' | '\x17' | '\x11' || '\x17' | '\n' | '\x17' | '\x11' |

  -h, --help                    Print this message and exit.
"
}

upload_files () {
  if [ "$EOP_set" == "N" ] ; then
    EOProcess=""
  else
    EOProcess="$EOP"
  fi

  if [ -z "$1" ] ; then
    echo "(ERROR) Input file is not specified." >&2
    usage
    exit 2
  fi

  for INPUTFILE in $*
  do

    if ! [ -s "$INPUTFILE" ] && [ "$INPUTFILE" != "-" ]; then
      echo "(ERROR) FILE:$INPUTFILE is empty or unaccessable." >&2
      continue
    fi

    if [ -z "$TYPE" ]; then
      fname=`basename $INPUTFILE`
      ext=${fname##*.}
      case ${ext,,} in
        ttl)
          t='ttl'
          ;;
        sparql)
          t='sparql'
          ;;
        *)
          echo "(ERROR) $INPUTFILE: unknown extention." >&2
          break
          ;;
      esac
    else
      t=$TYPE
    fi

    [ -z "$t" ] && continue

    echo "(INFO) Sending file: $INPUTFILE" >&2
    case $t in
      t|ttl)
        cmd="$cmdTTL -T $INPUTFILE"
        ;;
      s|sparql)
        cmd="$cmdSPARQL@$INPUTFILE"
        ;;
      *)
        echo "(ERROR) Unexpected input file type: $t." >&2 && { [ -n "$TYPE" ] && usage && return 2; }
        break
        ;;
    esac

    eval "$cmd" || { echo "(ERROR) An error occured while uploading file: $INPUTFILE" >&2; continue; }
    echo -ne "$EOProcess"
  done

  return 0;

}

upload_stream () {

  if [ "$EOP_set" == "N" ] ; then
    EOProcess="\0"
  else
    EOProcess="$EOP"
  fi

  [ -z "$TYPE" ] && { echo "(ERROR) input data format is not specified. Exiting." >&2; return 2;}
  while [[ $# > 0 ]]
  do
    key="$1"
    case $key in
      -d|--delimiter)
        delimiter="$2"
        shift;
        ;;
      -*)
        echo "(WARNING) upload_stream: Ignoring unknown option: $key." >&2
        ;;
      *)
        break
        ;;
      esac
      shift
  done

  case $TYPE in
    t|ttl)
      cmd="$cmdTTL --data-urlencode res-file@-"
      ;;
    s|sparql)
      cmd="$cmdSPARQL@-"
      ;;
    *)
      echo "(ERROR) Unexpected input stream type: $t." >&2 && { [ -n "$TYPE" ] && usage && return 2; }
      break
      ;;
  esac

  while true; do
    if [ -z "$(echo -ne $delimiter)" ] ; then
      while eval "read -d \$'$delimiter' line"; do
        if [ "$EOMessage" != "\n" ]; then
          $line=${line/$EOMessage/"\n"}
        fi
        n=`ps ax | grep 'curl' | grep "$HOST:$PORT" | grep -v 'grep' | wc -l`
        while [ $n -gt $CURL_N_MAX ]; do
          sleep $SLEEP
          n=`ps axf | grep 'curl' | grep "$HOST:$PORT" | grep -v 'grep' | wc -l`
        done
        echo "$line" | $cmd &>/dev/null || { echo "(ERROR) An error occured while uploading stream data." >&2; continue; } &
        echo -ne "$EOProcess"
      done
    else
      while read -r -d "$delimiter" line; do
        if [ "$EOMessage" != "\n" ]; then
          $line=${line/$EOMessage/"\n"}
        fi
        n=`ps ax | grep 'curl' | grep "$HOST:$PORT" | grep -v 'grep' | wc -l`
        while [ $n -gt $CURL_N_MAX ]; do
          sleep $SLEEP
          n=`ps axf | grep 'curl' | grep "$HOST:$PORT" | grep -v 'grep' | wc -l`
        done
        echo "$line" | $cmd &>/dev/null || { echo "(ERROR) An error occured while uploading stream data." >&2; continue; } &
        echo -ne "$EOProcess"
      done
    fi
  done
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
    -P|--port)
      PORT="$2"
      shift
      ;;
    -g|--graph)
      GRAPH="$2"
      shift
      ;;
    -t|--type)
      TYPE="${2,,}"
      shift
      ;;
    -m|--mode)
      MODE="${2,,}"
      shift
      ;;
    -b|--batch)
      if [ -z "$2" ] || [[ "$2" == -* ]];
      then
        BATCHMODE="e"
      else
        BATCHMODE="$2"
        shift
      fi
      ;;
    -B|--eob)
      EOB="$2"
      EOB_set="Y"
      shift
      ;;
    -E|--eop)
      EOP="$2"
      EOP_set="Y"
      shift
      ;;
    -e|--eom)
      EOM="$2"
      EOM_set="Y"
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
    --)
      shift
      break
      ;;
    -*)
      echo "(ERROR) Unknown option: $key." >&2
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
[ -z "$GRAPH" ] && GRAPH=http://$HOST:$PORT/$GRAPH_PATH

[ "$EOM_set" == "Y" ] && EOMessage="$EOM"
[ -z "$EOMessage" ] && EOMessage='\n'

[ "$EOB_set" == "Y" ] && EOBatch="$EOB"
[ -z "$EOB" ] && EOBatch='\n'

case $BATCHMODE in
  "e"|"enabled")
    [ "$EOB" == "$EOB_DEFAULT" ] && EOBatch='\x17'
    ;;
  "d"|"disabled")
    [ "$EOB" == "$EOB_DEFAULT" ] && EOBatch='\n'
    ;;
  *)
    log "Unexpected batch-mode parameter."
    ;;
esac

cmdTTL="curl --retry 3 -s -f -X POST --digest -u $USER:$PASSWD -H Content-Type:text/turtle -G http://$HOST:$PORT/sparql-graph-crud-auth --data-urlencode graph=$GRAPH"
cmdSPARQL="curl --retry 3 -s -f -H 'Accept: text/csv' -G http://$HOST:$PORT/sparql --data-urlencode query"

case $MODE in
  f)
    upload_files $*;
    ;;
  s)
    upload_stream -d "$EOBatch";
    ;;
  *)
    echo "(ERROR) $MODE: unsupported mode."  >&2
    exit 2
    ;;
esac

exit $?
