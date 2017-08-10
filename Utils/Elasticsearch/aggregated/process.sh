#!/bin/sh
LIST='MC16a'
tmp=`mktemp`
for i in $LIST; do
  php get_aggregated_data_from_bigpanda.php $i > $tmp
  ../load_data.sh $tmp 
  rm -f $tmp
done
