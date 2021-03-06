#!/bin/bash
functions="$(dirname $0)/functions.sh"
if [ -f "$functions" ]; then
  . "$functions"
else
  echo "functions file '$functions' not found!"
  exit 1
fi

e=0
while read f; do
  [ -z "$f" ] && continue
  r=`getAddedLines "$f" | grep -e '\s$'`;
  if [ $? -eq 0 ];then
    echo "Trailing spaces found in $f:"
    getLinesFromFile "$f" "$r"
    e=1
  fi
done <<< $(getChangedFiles | grep 'README$')
exit $e

