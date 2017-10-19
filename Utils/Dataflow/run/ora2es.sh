#!/usr/bin/env bash

base_dir=$( cd "$(dirname "$(readlink -f "$0")")"; pwd)

# Oracle Connector
cmd_OC="${base_dir}/../../Elasticsearch/Oracle2JSON.py"
cfg_OC="${base_dir}/ora2json.cfg"

# Stage 19
cmd_19="${base_dir}/../../019_oracle2esFormat/oracle2es.php"

# Stage 69
cmd_69="${base_dir}/../../069_upload2es/load_data.sh"

# Run Oracle Connector
BATCH_SIZE=100
i=0
$cmd_OC --config $cfg_OC --mode "SQUASH" | $cmd_19 | while read -r line; do
  echo $line
  let i=$i+1
  let f=$i%$BATCH_SIZE
  [ $f -eq 0 ] && echo -e '\x00'
done | $cmd_69
