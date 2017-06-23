#!/bin/sh
LIST='MC16a'
tmp=`mktemp`
for i in $LIST; do
  php get_aggregated_data_from_bigpanda.php $i > $tmp
  php load_data.php $tmp 
  rm -f $tmp
done
