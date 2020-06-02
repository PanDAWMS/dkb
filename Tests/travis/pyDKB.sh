#!/bin/bash
functions="$(dirname $0)/functions.sh"
if [ -f "$functions" ]; then
  . "$functions"
else
  echo "functions file '$functions' not found!"
  exit 1
fi

getChangedFiles | grep -q -e '^Utils/Dataflow/pyDKB' -e '^Utils/Dataflow/test/pyDKB'
have_files_to_check=$?

e=0
if [ $have_files_to_check -eq 0 ]; then
  $(dirname $0)/../../Utils/Dataflow/test/pyDKB/test.sh
  e=$?
fi
exit $e
