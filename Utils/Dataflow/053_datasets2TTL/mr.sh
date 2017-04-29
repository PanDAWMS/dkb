#!/usr/bin/env bash

INPUT_DIR=/user/DKB/temp/datasets

usage () {
  echo "USAGE:
  ./mr.sh <options>

OPTIONS:
  -f, --flags        FLAGS  String of additional flags for csv2sparql
  -r, --reducers     N      Number of reduce tasks (that is to say, the number of
                            output files).
  -i, --input        DIR    Input directory in HDFS
                            DEFAULT: $INPUT_DIR
  -t, --ttl-output   DIR    Output directory in HDFS for TTL files
                            DEFAULT: \${INPUT_DIR}_TTL
  -l, --link-output  DIR    Output directory in HDFS for TTL files
                            DEFAULT: \${INPUT_DIR}_LINK
  -c, --clear               Clear output directories before running MapReduce processing.
"
}

while [[ $# > 0 ]]
do
  key="$1"
  case $key in
    -h|--help)
      usage
      exit
      ;;
    -f|--flags)
      FLAGS="$2"
      shift
      ;;
    -r|--reducers)
      N="$2"
      shift
      ;;
    -i|--input)
      INPUT_DIR="$2"
      shift
      ;;
    -t|--ttl-output)
      TTL_DIR="$2"
      shift
      ;;
    -l|--link-output)
      LINK_DIR="$2"
      shift
      ;;
    -c|--clear)
      CLEAR="YES"
      ;;
    -*)
      echo "Unknown option: $key."
      usage
      exit 1
      ;;
    *)
      break
      ;;
  esac
  shift
done

[ -z "$INPUT_DIR" ] && echo "Empty input dir value" >&2 && exit 1
[ -z "$TTL_DIR" ] && TTL_DIR="${INPUT_DIR}_TTL"
[ -z "$LINK_DIR" ] && LINK_DIR="${INPUT_DIR}_LINK"
[ -z "$N" ] && N=0

[ "x$CLEAR" = "xYES" ] && hadoop fs -rm -r "$LINK_DIR"
PARAMETERS="-L $FLAGS"
t=`hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -D mapreduce.job.reduces="0" \
  -files ./csv2sparql.py,./simple_reducer.py \
  -mapper "./csv2sparql.py -m m $PARAMETERS" \
  -reducer ./simple_reducer.py \
  -input "$INPUT_DIR" \
  -output "$LINK_DIR"`


[ "x$CLEAR" = "xYES" ] && hadoop fs -rm -r "$TTL_DIR"
PARAMETERS="$FLAGS"
t=`hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -D mapreduce.job.reduces="$N" \
  -files ./csv2sparql.py,./simple_reducer.py \
  -mapper "./csv2sparql.py -m m $PARAMETERS" \
  -reducer ./simple_reducer.py \
  -input "$INPUT_DIR" \
  -output "$TTL_DIR"`
