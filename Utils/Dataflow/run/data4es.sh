#!/usr/bin/env bash

START_OFFSET="09-05-2016 00:00:00"
BATCH_SIZE=100
DEBUG=

base_dir=$( cd "$(dirname "$(readlink -f "$0")")"; pwd)

# Create aux directories
# ---
pipe_dir=$base_dir/.pipe
mkdir -p $pipe_dir

buffer_dir=$base_dir/.buffer
mkdir -p $buffer_dir

# Define commands to be used as dataflow nodes
# ---
# Oracle Connector
cmd_09="${base_dir}/../009_oracleConnector/Oracle2JSON.py"
cfg_09="${base_dir}/ora2json.cfg"
cmd_OC="$cmd_09 --config $cfg_09 --mode SQUASH"

# Stage 16
cmd_16="${base_dir}/../016_task2es/task2es.py -m s"

# Stage 19
cmd_19="${base_dir}/../019_esFormat/run.sh"

# Stage 69
cmd_69="${base_dir}/../069_upload2es/load_data.sh"

# Stage 91
cmd_91="${base_dir}/../091_datasetsRucio/datasets_processing.py -m s"
# ---

# Reset offset value for Oracle Connector.
[ -n "$START_OFFSET" ] \
    && sed -i.bak -e"s/^offset = .*$/offset = $START_OFFSET/" \
           $cfg_09

# Service (glue) functions
# ---
# EOP filter (required due to the unconfigurable EOP marker in pyDKB)
eop_filter() {
  sed -e"s/\\x00//g"
}

# Buffer file name
get_buffer() {
  [ -n "$1" ] && name=$1 || name=default
  echo $buffer_dir/$name
}

# Flush Sink Connector buffer to STDOUT and reset it
flush_buffer() {
  bufer=`get_buffer "$1"`
  cat $buffer
  echo -e '\x00'
  rm -f $buffer
}

# Glue between processing functions and Sink Connector
# Buffers records and then send them as batch of $BATCH_SIZE
mediator() {
  buffer=`get_buffer "$1"`
  i=0
  while read -r line; do
    echo $line >> $buffer
    let i=$i+1
    let f=$i%$BATCH_SIZE
    [ $f -eq 0 ] && flush_buffer "$1"
  done
  flush_buffer "$1"
}
# ---

# Create named pipes for dataflow branching
# ---
branch() {
  [ -z "$1" ] \
    && echo "Branch name not specified." >&2\
    && return 1

  name="$pipe_dir/$1"
  if [ -e "$name" ]; then
    [ -p "$name" ] \
      && { echo $name; return 0; } \
      || name=`mktemp -u $name.XXXX`
  fi

  mkfifo $name \
    || { echo "Failed to create named pipe for branch $1" >&2;
         return 1; }

  echo $name
}

b_91=`branch b_91` && b_16=`branch b_16` \
  || exit $?

# ---

# Run Dataflow
# ---

# Source subchain: Oracle Connector
source_chain() {
  $cmd_OC | tee $b_16 $b_91
}

# Parallel subchains: 16 and 91
chain_16() {
  cat $b_16 | $cmd_16 | eop_filter
}

chain_91() {
  cat $b_91 | $cmd_91 | eop_filter
}

# Sink chain
sink_chain() {
  [ -n "$DEBUG" ] \
    && cmd="tee 69.$1.inp" \
    || cmd=$cmd_69
  $cmd_19 | mediator "$1" | $cmd > /dev/null
}

out=/dev/null
[ -n "$DEBUG" ] \
  && out='src.out'
source_chain > $out &

[ -n "$DEBUG" ] \
  && out='16.out'
chain_16 | tee $out | sink_chain b16 &

[ -n "$DEBUG" ] \
  && out='91.out'
chain_91 | tee $out | sink_chain b91 &

wait
