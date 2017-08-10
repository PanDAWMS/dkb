#!/bin/sh
for i in $*; do
  php oracle2es.php $i
done  > raw_current.ndjson 
../load_data.sh raw_current.ndjson
rm -f raw_current.ndjson
