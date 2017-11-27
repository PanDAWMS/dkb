#!/usr/bin/env bash

START_OFFSET="09-05-2016 00:00:00"
BATCH_SIZE=100
DEBUG=

base_dir=$( cd "$(dirname "$(readlink -f "$0")")"; pwd)

# Oracle Connector
cmd_OC="${base_dir}/../009_oracleConnector/Oracle2JSON.py"
cfg_OC="${base_dir}/ora2json.cfg"

# Stage 16
cmd_16="${base_dir}/../016_task2es/task2es.py -m s"

# Stage 19
cmd_19="${base_dir}/../019_esFormat/esFormat.php"

# Stage 69
cmd_69="${base_dir}/../069_upload2es/load_data.sh"

# Reset offset value
[ -n "$START_OFFSET" ] \
    && sed -i.bak -e"s/^offset = .*$/offset = $START_OFFSET/" \
           $cfg_OC

# Run Oracle Connector
buffer="${base_dir}/.record_buffer"
touch $buffer || { echo "Failed to access buffer file." >&2; exit 2; }

# EOP filter
eop_filter() {
  sed -e"s/\\x00//"
}

flush_buffer() {
  cat $buffer
  echo -e '\x00'
  rm -f $buffer
}

oracle_connector() {
  $cmd_OC --config $cfg_OC --mode "SQUASH"
}

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

# Run Oracle Connector

[ -n "$DEBUG" ] \
   && oracle_connector | tee oc.out | \
      $cmd_16 | eop_filter | tee 16.out | \
      $cmd_19 | tee 19.out | mediator > 69.inp \
   || oracle_connector | $cmd_16 | eop_filter | \
      $cmd_19 | mediator | $cmd_69
