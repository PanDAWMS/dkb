#!/usr/bin/env bash

INPUT_DIR=/user/DKB/temp/pdfanalyzer_results_files
CSV_DIR=/user/DKB/temp/datasets_mr

usage () {
  echo "USAGE:
  ./mr.sh <options>

OPTIONS:
  -f, --flags        FLAGS  String of additional flags for csv2sparql
  -r, --reducers     N      Number of reduce tasks (that is to say, the number of
                            output files).
  -i, --input        FILE   Input file in HDFS
                            DEFAULT: $INPUT_DIR
  -o, --csv-output   DIR    Output directory in HDFS for CSV files
                            DEFAULT: $CSV_DIR
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
    -o|--csv-output)
      CSV_DIR="$2"
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

[ -z "$INPUT_DIR" ] && echo "Empty input file value" >&2 && exit 1
[ -z "$CSV_DIR" ] && CSV_DIR="${INPUT_DIR}_CSV"
[ -z "$N" ] && N=0


[ "x$CLEAR" = "xYES" ] && hadoop fs -rm -r "$CSV_DIR"
PARAMETERS="$FLAGS"
t=`hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
  -D mapreduce.job.reduces="$N" \
  -files ./get_metadata.py,./simple_reducer.py \
  -mapper "./get_metadata.py -m m $PARAMETERS" \
  -reducer ./simple_reducer.py \
  -input "$INPUT_DIR" \
  -output "$CSV_DIR"`
