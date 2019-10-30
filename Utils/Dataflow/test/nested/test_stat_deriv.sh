#!/bin/sh

. query/stat-deriv.param
. clear_cache

old_idx="prodsys_rucio_ami_20190903"
new_idx="tasks_nested"
old_qfile="query/stat-deriv-old.json"
old_formatsq="query/formats.json"
new_qfile="query/stat-deriv-new.json"

echo "query,type,N,sec"
for pr_id in $PR_ID; do
  clear_cache $old_idx $new_idx
  echo "pr_id: '$pr_id'" >&2
  FORMATS=`cat $old_formatsq \
  | sed -e's/%%PR_ID%%/'"${pr_id}"'/' \
  | tee old-formats-q.json \
  | curl -s -X GET "http://127.0.0.1:9200/${old_idx}/task/_search" -H 'Content-Type: application/json' -d @- \
  | jq -c '.aggregations.format.buckets | map({ (.key) : {"has_child": {"type": "output_dataset", "query": {"term": {"data_format": .key }}}}}) | add'`

  took=`jq ".took" "formats-${pr_id}.json"`
  cat $old_qfile \
  | sed -e's/%%PR_ID%%/'"${pr_id}"'/' -e's/%%FORMAT_FILTERS%%/'${FORMATS}'/' \
  | tee old-q.json \
  | curl -s -X GET "http://127.0.0.1:9200/${old_idx}/task/_search" -H 'Content-Type: application/json' -d @- \
  | tee old.json \
  | jq -c -r '[ "stat-deriv", "old", .hits.total, .took+'"$took"' ] | @csv'

  cat $new_qfile \
  | sed -e's/%%PR_ID%%/'"${pr_id}"'/' \
  | tee new-q.json \
  | curl -s -X GET "http://127.0.0.1:9200/${new_idx}/task/_search" -H 'Content-Type: application/json' -d @- \
  | tee new.json \
  | jq -c -r '[ "stat-deriv", "new", .hits.total, .took ] | @csv'
done
