#!/bin/sh

. query/stat-deriv.param

old_idx="prodsys_rucio_ami_20190903"
new_idx="tasks_nested"
old_qfile="query/stat-deriv-old.json"
old_formatsq="query/formats.json"
new_qfile="query/stat-deriv-new.json"

clear_cache() {
  curl -s -X POST "http://127.0.0.1:9200/$old_idx,$new_idx/_cache/clear/" >/dev/null
  sync
  echo 3 > /proc/sys/vm/drop_caches
}

echo "query,type,N,sec"
for pr_id in $PR_ID; do
  clear_cache
  echo "pr_id: '$pr_id'" >&2
  FORMATS=`cat $old_formatsq \
  | sed -e's/%%PR_ID%%/'"${pr_id}"'/' \
  | tee old-formats-q.json \
  | curl -s -X GET "http://127.0.0.1:9200/${old_idx}/task/_search" -H 'Content-Type: application/json' -d @- \
  | jq -c '.aggregations.format.buckets | map({ (.key) : {"has_child": {"type": "output_dataset", "query": {"term": {"data_format": .key }}}}}) | add'`

  cat $old_qfile \
  | sed -e's/%%PR_ID%%/'"${pr_id}"'/' -e's/%%FORMAT_FILTERS%%/'${FORMATS}'/' \
  | tee old-q.json \
  | curl -s -X GET "http://127.0.0.1:9200/${old_idx}/task/_search" -H 'Content-Type: application/json' -d @- \
  | tee old.json \
  | jq -c -r '[ "stat-deriv", "old", .hits.total, .took ] | @csv'

  cat $new_qfile \
  | sed -e's/%%PR_ID%%/'"${pr_id}"'/' \
  | tee new-q.json \
  | curl -s -X GET "http://127.0.0.1:9200/${new_idx}/task/_search" -H 'Content-Type: application/json' -d @- \
  | tee new.json \
  | jq -c -r '[ "stat-deriv", "new", .hits.total, .took ] | @csv'
done
