#!/usr/bin/env bash

START_OFFSET="09-05-2016 00:00:00"
BATCH_SIZE=100
DEBUG=

base_dir=$( cd "$(dirname "$(readlink -f "$0")")"; pwd)

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
# ---

# Reset offset value for Oracle Connector.
[ -n "$START_OFFSET" ] \
    && sed -i.bak -e"s/^offset = .*$/offset = $START_OFFSET/" \
           $cfg_09

# Buffer for sink connector (Stage 69)
buffer="${base_dir}/.record_buffer"
touch $buffer || { echo "Failed to access buffer file." >&2; exit 2; }

# Service (glue) functions
# ---
# EOP filter (required due to the unconfigurable EOP marker in pyDKB)
eop_filter() {
  sed -e"s/\\x00//"
}

# Flush Sink Connector buffer to STDOUT and reset it
flush_buffer() {
  cat $buffer
  echo -e '\x00'
  rm -f $buffer
}

# Glue between processing functions and Sink Connector
# Buffers records and then send them as batch of $BATCH_SIZE
mediator() {
  i=0
  while read -r line; do
    echo $line >> $buffer
    let i=$i+1
    let f=$i%$BATCH_SIZE
    [ $f -eq 0 ] && flush_buffer
  done
  flush_buffer
}
# ---

# Run Dataflow

[ -n "$DEBUG" ] \
   && $cmd_OC | tee oc.out | \
      $cmd_16 | eop_filter | tee 16.out | \
      $cmd_19 | tee 19.out | mediator > 69.inp \
   || $cmd_OC | $cmd_16 | eop_filter | \
      $cmd_19 | mediator | $cmd_69
