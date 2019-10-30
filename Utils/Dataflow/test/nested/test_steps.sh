#!/bin/sh

. query/steps.param
. clear_cache

old_idx="prodsys_rucio_ami_20190903"
new_idx="tasks_nested"
old_qfile="query/steps-old.json"
new_qfile="query/steps-new.json"

echo "query,type,N,sec"
for htag in $HASHTAG; do
  clear_cache $old_idx $new_idx
  echo "htag '$htag'" >&2
  cat $old_qfile \
  | sed -e's/%%HASHTAG%%/'"${htag}"'/' \
  | tee old-${htag}-q.json \
  | curl -s -X GET "http://127.0.0.1:9200/${old_idx}/task/_search" -H 'Content-Type: application/json' -d @- \
  | tee old-${htag}.json \
  | jq -c -r '[ "steps", "old", .hits.total, .took ] | @csv'

  cat $new_qfile \
  | sed -e's/%%HASHTAG%%/'"${htag}"'/' \
  | tee new-${htag}-q.json \
  | curl -s -X GET "http://127.0.0.1:9200/${new_idx}/task/_search" -H 'Content-Type: application/json' -d @- \
  | tee new-${htag}.json \
  | jq -c -r '[ "steps", "new", .hits.total, .took ] | @csv'
done
