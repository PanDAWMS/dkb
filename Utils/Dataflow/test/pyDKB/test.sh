#!/bin/bash

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

# Test stage run modes.

base_dir=$(cd "$(dirname "$(readlink -f "$0")")"; pwd)
orig_dir=`pwd`

cd $base_dir

usage() {
  echo "$0 [-h] [-l] [-c N[,N...]] [--todo]

Run pyDKB stage functionality test cases.

OPTIONS
  -h, --help    show this message and exit
  -l, --list    show test cases description
  -c, --case N[,N...]
                run specified test case(s)
  --todo        list known issues for improvements
" >&2
}

list_case() {
  [ -n "$1" ] && info="$1"
  for c in "case/"*; do
    if [ -f "$c/$info" ] || [ -z "$info" ]; then
      echo -n "`basename "$c"`: "
      cat "$c/info"
      if [ -n "$info" ]; then
        echo "--- ${info^^} ---"
        cat "$c/$info"
        echo -e "------------\n"
      fi
    fi
  done
}

test_case() {
  case=$1
  case_id=`basename $case`

  before=`cat $case/before 2>/dev/null`
  [ -n "$before" ] && before="$before;"
  cmd=`cat $case/cmd`
  after=`cat $case/after 2>/dev/null`

  eval "$before $cmd; $after"  2>&1 1> out.tmp | \
    grep -v '(WARN) pyDKB.dataflow.cds failed (No module named invenio_client.contrib)' | \
    sed -e"s#$base_dir#\$base_dir#" >  err.tmp

  err_correct=0
  out_correct=0

  diff $case/out  out.tmp > /dev/null && out_correct=1
  diff $case/err err.tmp > /dev/null && err_correct=1

  [ $out_correct -ne 1 ] && echo -e "${RED}FAIL${NC}: $case_id (STDOUT) (cmd: '$cmd')"
  [ $err_correct -ne 1 ] && echo -e "${RED}FAIL${NC}: $case_id (STDERR) (cmd: '$cmd')"
  [ $out_correct -eq 1 ] && [ $err_correct -eq 1 ] && echo -e " ${GREEN}OK${NC} : $case_id"
}

CASES=""

while [ -n "$1" ]; do
  case "$1" in
    -l|--list)
      list_case
      exit 0
      ;;
    -h|--help)
      usage && exit 0
      ;;
    -c|--case)
      ( [ -z "$2" ] || [[ "$2" = -* ]] ) && \
        echo "-c/--case: test case ID not specified" \
             "(use --help for usage info)." >&2 && exit 1
      CASES=`echo "case/$2" | sed -e's/,/ case\//g'`
      shift
      ;;
    --todo)
      list_case todo
      exit 0
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

[ -z "$CASES" ] && CASES='case/*'

for c in $CASES; do
  if [ -d "$c" ]; then
    test_case $c
  else
    echo "Invalid test: '$c' (directory not found)." >&2
  fi
done

cd $orig_dir
