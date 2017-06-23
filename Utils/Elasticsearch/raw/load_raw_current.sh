#!/bin/sh
for i in $*; do
  php oracle2es.php $i
done  > raw_current.ndjson 
php ../load_data.php raw_current.ndjson
rm -f raw_current.ndjson
