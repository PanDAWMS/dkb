#!/bin/bash

#set -x

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

exit_code=0
# Test stage run modes.

base_dir=$(cd "$(dirname "$(readlink -f "$(echo $0|sed -e 's/^-//')")")"; pwd)
orig_dir=`pwd`

cd $base_dir

usage() {
  echo "$0 [-h] [-l [TYPE] [--no-info]] [-c N[,N...]]

Run pyDKB stage functionality test cases.

OPTIONS
  -h, --help    show this message and exit
  -l, --list [TYPE]
                show test case information: description by default,
                TYPE if specified
  --no-info     don't show case descriptions with --list command
  -c, --case N[,N...]
                run specified test case(s)
" >&2
}

list_case() {
  [ -n "$1" ] && info="$1"
  for c in "case/"*; do
    if [ -f "$c/$info" ] || [ -z "$info" ]; then
      echo -n "`basename "$c"`: "
      [ -z "$NO_INFO" ] && cat "$c/info"
      if [ -n "$info" ] && [ "$info" != 'info' ] ; then
        [ -n "$NO_INFO" ] && sed_cmd='1!s/^/    /' || sed_cmd='s/^/    /'
        cat "$c/$info" | sed "$sed_cmd"
        [ -z "$NO_INFO" ] && echo ""
      fi
    fi
  done
}

test_case() {
  case=$1
  case_id=`basename $case`

  base_dir_mod=`echo $base_dir | sed -e "s|test/pyDKB||g"`

  before=`cat $case/before 2>/dev/null`
  [ -n "$before" ] && before="$before;"
  cmd=`cat $case/cmd`
  after=`cat $case/after 2>/dev/null`
  eval "$before $cmd; $after"  2>&1 1> out.tmp | \
    grep -a -v "(WARN) (pyDKB.dataflow.cds) Submodule failed (No module named 'invenio_client')" | \
    sed -E -e"s#$base_dir#\$base_dir#" \
           -e"s#$base_dir_mod##" \
           -e's#\$base_dir/./../../##g' \
           -e"s#^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} ##" \
           -e"s#, line [0-9]+#, line <NNN>#" \
           -e"s#/.*/os.py#os.py#" | sort > err.tmp

  err_correct=0
  out_correct=0
   
  cat $case/err | sort > err2.tmp 
  diff -a -u $case/out out.tmp &> "${case_id}_out.diff"
  diff -a -u err2.tmp err.tmp &> "${case_id}_err.diff"

  if [ -s "${case_id}_out.diff" ]; then
    echo -e "${RED}FAIL${NC}: $case_id (STDOUT) (cmd: '$cmd')"
    cp out.tmp "${case_id}.out"
  else
    rm "${case_id}_out.diff"
  fi
  if [ -s "${case_id}_err.diff" ]; then
    echo -e "${RED}FAIL${NC}: $case_id (STDERR) (cmd: '$cmd')"
    cp err.tmp "${case_id}.err"
  else
    rm "${case_id}_err.diff"
  fi

  result=0

  ! [ -f "${case_id}_err.diff" ] && ! [ -f "${case_id}_out.diff" ] && \
    echo -e " ${GREEN}OK${NC} : $case_id" || \
    result=1

  return $result
}

CASES=""

while [ -n "$1" ]; do
  case "$1" in
    -l|--list)
      ( [ -z "$2" ] || [[ "$2" = -* ]] ) && INFO=info || \
        { INFO="$2"; shift; }
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
    --no-info)
      NO_INFO=1
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

if [ -n "$INFO" ]; then
  list_case "$INFO"
  exit 0
fi

[ -z "$CASES" ] && CASES='case/*'

rm *_{err,out}.diff 2>/dev/null
rm *.{err,out} 2>/dev/null

for c in $CASES; do
  if [ -d "$c" ]; then
    test_case $c
    result=$?
    [ $exit_code -eq 0 -a $result -eq 1 ] && exit_code=$result
  else
    echo "Invalid test: '$c' (directory not found)." >&2
  fi
done

cd $orig_dir
exit $exit_code
