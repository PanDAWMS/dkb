#!/usr/bin/env bash

base_dir=`dirname $0`
. $base_dir/../018_PDFDownloader/switch_realm

[ -z "$KAFKA_HOME" ] && KAFKA_HOME=/usr/local/kafka/default/
[ -z "$CONFIGS" ] && CONFIGS=$base_dir/config



pidfile=$base_dir/.processes

for f in ./libs/*.jar; do
  CLASSPATH="$CLASSPATH:$f"
done
export CLASSPATH

LOG4J_OPTS_ORIG=$LOG4J_OPTS

CONNECT_CLASS=org.apache.kafka.connect.cli.ConnectStandalone

STAGES="010GlancePapers 015CDSDocs 055DocTTL 054DocContentTTL 060VirtuosoSink"

usage () {
  echo "USAGE
$0 {<stage>|all} {start|stop|restart}

STAGES
  010GlancePapers -- Glance source for papers
  015CDSDocs -- get data from CDS
  055DocTTL -- generate TTL with document metadata
  054DocContentTTL -- generate TTL with document content information
  060VirtuosoSink -- TTL and SPARQL sinks to Virtuoso
"
}

_command () {
  EXTRA_ARGS="-name $1"
  realm=
  case $1 in
    060VirtuosoSink)
      cmd="$base_dir/run-class.sh $EXTRA_ARGS $CONNECT_CLASS $CONFIGS/060-connect-standalone.properties $CONFIGS/060-virtuoso-ttl-sink.properties"
      ;;
    010GlancePapers)
      cmd="$base_dir/run-class.sh $EXTRA_ARGS $CONNECT_CLASS $CONFIGS/010-connect-standalone.properties $CONFIGS/010-glance-lop-source.properties"
      realm=cern
      ;;
    015CDSDocs)
      cmd="./runStreamsApplication.sh $CONFIGS/015-application.properties $CONFIGS/015-topology.properties"
      realm=cern
      ;;
    055DocTTL)
      cmd="./runStreamsApplication.sh $CONFIGS/055-application.properties $CONFIGS/055-topology.properties"
      ;;
    054DocContentTTL)
      cmd="./runStreamsApplication.sh $CONFIGS/054-application.properties $CONFIGS/054-topology.properties"
      realm=kiae
      ;;
    *)
      echo "Unknown command: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
  export LOG4J_OPTS="-Dlog.file.name=$1 $LOG4J_OPTS"
  export COMMAND=$cmd
  export EXTRA_ARGS
  export REALM=$realm
}

_start() {
  local pgid
  _command "$1"
  export LOG4J_OPTS
  export KAFKA_LOG4J_OPTS="$LOG4J_OPTS"
  [ -n "$REALM" ] && switch_realm $REALM
  if [ -n "$COMMAND" ]; then
    echo $COMMAND
    export LOG_DIR="$base_dir/logs"
    EXTRA_ARGS="$EXTRA_ARGS" nohup $COMMAND >> /dev/null &
    pgid=`ps -o pgid= $!`
  fi
  [ -n "$pgid" ] && echo "$1 $pgid" >> $pidfile
}

_stop() {
  local pgid
  pgids=`grep $1 $pidfile | awk '{print $2}'`
  echo "Stopping instance of $1">&2
  for pgid in $pgids; do
    javaPid=`ps a -o pgid,pid,command | grep -e $pgid | grep java | grep $1 | awk '{print $2}'`
    if [ -n "$javaPid" ]; then
      echo "Sending SIGTERM to PID: $javaPid" >&2
      kill -s SIGTERM $javaPid
    fi
  done
  # Now check if processes are finished
  for pgid in $pgids; do
    n=0
    [ -z "$pgid" ] && continue
    while ps a -o pgid | grep $pgid | grep java &>/dev/null; do
      [ $n -gt 3 ] && echo "Can't stop JAVA process in group (PGID=$pgid)" >&2 && exit 2
      sleep 5;
      let n=$n+1;
    done
    sed -i.bak -e"/$1/d" $pidfile
  done
}

[ -n "$2" ] && task="$2" || task="start"

[ "x$1" = "xall" ] && stages=$STAGES || stages=$1

[ -z "$stages" ] && usage >&2 && exit 1

for stage in $stages; do
  LOG4J_OPTS=$LOG4J_OPTS_ORIG
  case $task in
    start)
      _start $stage
      ;;
    stop)
      _stop $stage
      ;;
    restart)
      _stop $stage && _start $stage
      ;;
    *)
      echo "Unknown task: $task" >&2
      usage >&2
      exit 1
      ;;
  esac
done
