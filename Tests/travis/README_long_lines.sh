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
  r=`getAddedLines "$f" | grep -e '^.\{81,\}' | while IFS= read -r line; do
    url=$(echo "$line" | getURL)
    trimmed_line=$(echo "$line" | sed -e 's/^\s*//')
    [ "$url" = "$trimmed_line" ] && continue
    echo "$line"
  done`
  if [ -n "$r" ];then
    echo "Very long line(s) in $f:"
    getLinesFromFile "$f" "$r"
    e=1
  fi
done <<< $(getChangedFiles | grep 'README$')
exit $e

