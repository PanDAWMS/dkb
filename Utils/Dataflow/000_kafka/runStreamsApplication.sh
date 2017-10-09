#!/usr/bin/env bash

base_dir=`dirname $0`



$base_dir/run-class.sh $EXTRA_ARGS ru.kiae.dkb.kafka.streams.cli.StreamsApplication "$@"
