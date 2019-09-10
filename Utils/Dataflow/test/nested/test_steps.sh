#!/bin/sh

. query/steps.param

old_idx="prodsys_rucio_ami_20190903"
new_idx="tasks_nested"
old_qfile="query/steps-old.json"
new_qfile="query/steps-new.json"

clear_cache() {
  curl -s -X POST "http://127.0.0.1:9200/$old_idx,$new_idx/_cache/clear/" >/dev/null
  sync
  echo 3 > /proc/sys/vm/drop_caches
}

echo "query,type,N,sec"
for htag in $HASHTAG; do
  clear_cache
  echo "htag '$htag'" >&2
  cat $old_qfile \
  | sed -e's/%%HASHTAG%%/'"${htag}"'/' \
  | tee old-q.json \
  | curl -s -X GET "http://127.0.0.1:9200/${old_idx}/task/_search" -H 'Content-Type: application/json' -d @- \
  | tee old.json \
  | jq -c -r '[ "steps", "old", .hits.total, .took ] | @csv'
  cat $new_qfile \
  | sed -e's/%%HASHTAG%%/'"${htag}"'/' \
  | tee new-q.json \
  | curl -s -X GET "http://127.0.0.1:9200/${new_idx}/task/_search" -H 'Content-Type: application/json' -d @- \
  | tee new.json \
  | jq -c -r '[ "steps", "new", .hits.total, .took ] | @csv'
done
