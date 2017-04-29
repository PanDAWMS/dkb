#!/usr/bin/env bash

base_dir=`dirname $0`

export LOG4J_OPTS="-Dlog.file.name=ExternalProcessor"

$base_dir/run-class.sh ru.kiae.dkb.kafka.streams.runExternalProcessor "$@"
