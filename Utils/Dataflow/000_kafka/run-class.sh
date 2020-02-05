#!/usr/bin/env bash

[ -z "$KAFKA_HOME" ] && KAFKA_HOME=/data/kafka

base_dir=`dirname $0`
base_dir=$(cd $base_dir; pwd)

CLASSPATH="$CLASSPATH:$base_dir/"

for f in $base_dir/libs/*.jar ; do
  CLASSPATH="$CLASSPATH:$f"
done

LOG4J_OPTS="-Dlog4j.configuration=file:$base_dir/config/log4j.properties $LOG4J_OPTS"
LOG4J_OPTS="-Dlog.dir=${base_dir}/logs $LOG4J_OPTS"

export LOG_DIR="$base_dir/logs"
export KAFKA_LOG4J_OPTS=$LOG4J_OPTS

$KAFKA_HOME/bin/kafka-run-class.sh "$@"
