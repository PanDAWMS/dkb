#!/usr/bin/env bash

[ -z "$KAFKA_HOME" ] && KAFKA_HOME=/usr/local/kafka/default/
[ -z "$CONFIGS" ] && CONFIGS=`dirname $0`/config

pidfile=./.processes

for f in ./libs/*.jar; do
  CLASSPATH="$CLASSPATH:$f"
done
export CLASSPATH

usage () {
  echo "USAGE
$0 <stage> {start|stop|restart}

STAGES
  010GlancePapers -- Glance source for papers
  050Links2TTL -- Links to TTL transformation
  060VirtuosoSink -- TTL and SPARQL sinks to Virtuoso
  010to055 -- data stream from 010 to 055
"
}

_command () {
  case $1 in
    060VirtuosoSink)
      cmd="$KAFKA_HOME/bin/connect-standalone.sh $CONFIGS/060-connect-standalone.properties $CONFIGS/060-virtuoso-ttl-sink.properties $CONFIGS/060-virtuoso-sparql-sink.properties"
      ;;
    010GlancePapers)
      cmd="$KAFKA_HOME/bin/connect-standalone.sh $CONFIGS/010-connect-standalone.properties $CONFIGS/010-glance-lop-source.properties"
      ;;
    050Links2TTL)
      cmd="./runStreamsApplication.sh $CONFIGS/050-application.properties $CONFIGS/050-topology.properties"
      ;;
    010to055)
      cmd="./runStreamsApplication.sh $CONFIGS/010-015-055-application.properties $CONFIGS/010-015-055-topology.properties"
      ;;
    *)
      echo "Unknown command: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
  echo $cmd
}

_start() {
  local pgid
  cmd=`_command $1`
  logfile=$1.log
  if [ -n "$cmd" ]; then
    echo $cmd
    nohup $cmd >>$logfile &
    pgid=`ps -o pgid= $!`
  fi
  [ -n "$pgid" ] && echo "$1 $pgid" >> $pidfile
}

_stop() {
  local pgid
  pgids=`grep $1 $pidfile | awk '{print $2}'`
  echo "Stopping instance of $1">&2
  for pgid in $pgids; do
    javaPid=`ps a -o pgid,pid,ucmd | grep $pgid |grep java | awk '{print $2}'`
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

case $task in
  start)
    _start $1
    ;;
  stop)
    _stop $1
    ;;
  restart)
    _stop $1 && _start $1
    ;;
  *)
    echo "Unknown task: $task" >&2
    usage >&2
    exit 1
    ;;
esac
