#!/usr/bin/env sh

# Get data sample from Chicago ES
# Time interval: 2020-04-01 00:00:00 -- 2020-04-29 00:00:00
# Jobs filter: gShare != ['User Analysis', 'Group Analysis', 'COVID']

base_dir=$( cd "$(dirname "$(readlink -f "$0")")"; pwd)

LEFT_TS="2020-04-01T00:00:00"
RIGHT_TS="2020-04-29T00:00:00"

query_orig="$base_dir/../010_chicagoES/queries/progress.json"
query_tmp="$base_dir/query.tmp"
sample="$base_dir/sample.json"

cat "$query_orig" | \
  sed -e 's/%(left_ts)s/"'$LEFT_TS'"/' \
      -e 's/%(right_ts)s/"'$RIGHT_TS'"/' \
  > "$query_tmp"

auth="FIXME:FIXME"
host="192.170.227.66:9200"


r=""

query_uces() {
  curl -sS -u "$auth" -H 'Content-Type: application/json' -d@"$query_tmp" -X GET 'http://'"$host"'/jobs*/_search'
}

after_key() {
  echo "$1" |  jq -c ".aggregations.progress.after_key"
}

data() {
  echo "$1" | jq -c ".aggregations.progress.buckets[] | { date: .key.date, taskid: .key.task, processed_events: .events.value }"
}

[ -f "$sample" ] && rm "$sample"


# Getting data from Chicago ES

while true
do
  echo "'$cmd'"
  r=`query_uces`
  echo "$r" > response.tmp
  [ -z "$r" ] && break
  ak=$(after_key "$r")
  [ -z "$ak" ] && break
  mv "$query_tmp"{,.bak}
  cat "$query_tmp".bak | jq ".aggs.progress.composite.after=$ak" > "$query_tmp"
  sample_data=`data "$r"` || break
  echo "$sample_data" >> "$sample"
  [ -z "$sample_data" ] && break
done
