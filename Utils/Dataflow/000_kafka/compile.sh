#!/usr/bin/env bash

[ -z "$KAFKA_HOME" ] && KAFKA_HOME=/usr/local/kafka/default

for f in ./libs/*.jar $KAFKA_HOME/libs/*jar $KAFKA_HOME/dependant-libs/*/*.jar; do
  CLASSPATH="$CLASSPATH:$f"
done

javac -cp $CLASSPATH $*
