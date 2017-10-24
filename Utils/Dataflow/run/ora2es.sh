#!/usr/bin/env bash

START_OFFSET="09-05-2016 00:00:00"
BATCH_SIZE=100

base_dir=$( cd "$(dirname "$(readlink -f "$0")")"; pwd)

# Oracle Connector
cmd_OC="${base_dir}/../009_oracleConnector/Oracle2JSON.py"
cfg_OC="${base_dir}/ora2json.cfg"

# Stage 19
cmd_19="${base_dir}/../019_oracle2esFormat/oracle2es.php"

# Stage 69
cmd_69="${base_dir}/../069_upload2es/load_data.sh"

# Reset offset value
sed -i.bak -e"s/^offset = .*$/offset = $START_OFFSET/" \
    $cfg_OC

# Run Oracle Connector
buffer="${base_dir}/.record_buffer"
touch $buffer || { echo "Failed to access buffer file." >&2; exit 2; }

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

oracle_connector | $cmd_19 | mediator | $cmd_69
