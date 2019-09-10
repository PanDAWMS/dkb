#!/bin/sh

. query/deriv.param

old_idx="prodsys_rucio_ami_20190903"
new_idx="tasks_nested"
old_qfile="query/deriv-old.json"
new_qfile="query/deriv-new.json"

clear_cache() {
  curl -s -X POST "http://127.0.0.1:9200/$old_idx,$new_idx/_cache/clear/" >/dev/null
  sync
  echo 3 > /proc/sys/vm/drop_caches
}

echo "query,type,N,sec"
for i in $PARAM_SET; do
  clear_cache
  echo "param set: '$i'" >&2
  project_param=PROJECT_${i}
  ctag_param=CTAG_${i}
  output_param=OUTPUT_${i}
  PROJECT=${!project_param}
  CTAG=${!ctag_param}
  OUTPUT=${!output_param}
  echo "PROJECT: '$PROJECT'" >&2
  echo "CTAG: '$CTAG'" >&2
  echo '' >&2
  for o in $OUTPUT; do
    echo -e "\e[1A\e[0K\rOUTPUT: '$o'" >&2
    cat $old_qfile \
    | sed -e's/%%PROJECT%%/'"${PROJECT}"'/' -e's/%%CTAG%%/'"${CTAG}"'/' -e's/%%OUTPUT%%/'"${o}"'/' \
    | tee "old-${o}-q.json" \
    | curl -s -X GET "http://127.0.0.1:9200/${old_idx}/task/_search" -H 'Content-Type: application/json' -d @- \
    | tee "old-${o}.json"
  done \
  | jq -s -c -r '["deriv", "old", (map(.hits.total) | add), (map(.took) | add)] | @csv'

  cat $new_qfile \
  | sed -e's/%%PROJECT%%/'"${PROJECT}"'/' -e's/%%CTAG%%/'"${CTAG}"'/' \
  | tee new-q.json \
  | curl -s -X GET "http://127.0.0.1:9200/${new_idx}/task/_search" -H 'Content-Type: application/json' -d @- \
  | tee new.json \
  | jq -c -r '[ "deriv", "new", .hits.total, .took ] | @csv'
done
