#!/bin/bash

# Test stage run modes.

base_dir=$(cd "$(dirname "$(readlink -f "$0")")"; pwd)
orig_dir=`pwd`

cd $base_dir

usage() {
  echo "$0 [-h] [-l]

Run pyDKB stage functionality test cases.

OPTIONS
  -h, --help    show this message and exit
  -l, --list    show test cases description
" >&2
}

list_case() {
  for c in case/*; do
    echo -n "`basename "$c"`: "
    cat $c/info
  done
}

test_case() {
  case=$1
  case_id=`basename $case`

  cmd=`cat $case/cmd`
  eval "$cmd"  2>&1 1> out.tmp | \
    grep -v '(WARN) pyDKB.dataflow.cds failed (No module named invenio_client.contrib)' >  err.tmp

  err_correct=0
  out_correct=0

  diff $case/out  out.tmp > /dev/null && out_correct=1
  diff $case/err err.tmp > /dev/null && err_correct=1

  [ $out_correct -ne 1 ] && echo "FAIL: $case_id (STDOUT) (cmd: '$cmd')"
  [ $err_correct -ne 1 ] && echo "FAIL: $case_id (STDERR) (cmd: '$cmd')"
  [ $out_correct -eq 1 ] && [ $err_correct -eq 1 ] && echo " OK : $case_id"
}

while [ -n "$1" ]; do
  case "$1" in
    -l|--list)
      list_case && exit 0
      ;;
    -h|--help)
      usage && exit 0
      ;;
    -*)
      echo "Unknown option: $1" && usage && exit 1
      ;;
    *)
      break
      ;;
  esac
  shift
done

for case in case/*; do
  test_case $case
done

cd $orig_dir
