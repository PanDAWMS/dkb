#!/bin/sh

. query/keyword.param

old_idx="prodsys_rucio_ami_20190903"
new_idx="tasks_nested"
old_qfile="query/keyword-old.json"
new_qfile="query/keyword-new.json"

clear_cache() {
  curl -s -X POST "http://127.0.0.1:9200/$old_idx,$new_idx/_cache/clear/" >/dev/null
  sync
  echo 3 > /proc/sys/vm/drop_caches
}

echo "query,type,N,sec"
echo "$QUERY" | while read q; do
  clear_cache
  echo "q_param: '$q'" >&2
  cat $old_qfile \
  | sed -e's/%%QUERY%%/'"${q}"'/' \
  | tee old-q.json \
  | curl -s -X GET "http://127.0.0.1:9200/${old_idx}/task/_search" -H 'Content-Type: application/json' -d @- \
  | tee old.json \
  | jq -c -r '[ "keyword", "old", .hits.total, .took ] | @csv'
  cat $new_qfile \
  | sed -e's/%%QUERY%%/'"${q}"'/' \
  | tee new-q.json \
  | curl -s -X GET "http://127.0.0.1:9200/${new_idx}/task/_search" -H 'Content-Type: application/json' -d @- \
  | tee new.json \
  | jq -c -r '[ "keyword", "new", .hits.total, .took ] | @csv'
done
