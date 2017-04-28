#!/bin/bash

# Importing data from Oracle to HDFS

# Configuration
# Oracle DB connection parameters
# IP address (will be overwritten if --ora-host is specified)
ORA_HOST="172.17.34.3"

# Port (will be overwritten if --ora-port is specified)
ORA_PORT="12344"

# Service (will be overwritten if --ora-service is specified)
ORA_SERVICE="adcr.cern.ch"

# In this directory TABLENAME directory will be created
# Default value (will be overwritten if --output-dir is specified):
BASE_DIR="/var/sqoop/`whoami`"

# TABLENAME/<timestamp> subdirectory (leave blank if don't want to create one)
# Default value (will be overwritten if --timestamp and/or --no-ts-dir is specified):
TS_DIR=`date +%s`

# Leave blank if don't want to remove target directory
RM_TG_DIR="--delete-target-dir"

# Oracle password file (will be owerwritten if --database is specified: ${PW_FILE}_${ORA_DB})
PW_FILE=$HOME/.orapw


ORA_DB=
HDFS_F_TYPE=
ORA_T_OWNER=
ORA_TABLE=
TYPE_MAPPING=

known_dbs="DEFT PRODSYS1"
DEFT_known_tables="T_PRODMANAGER_REQUEST T_PRODUCTION_TASK T_PRODUCTION_DATASET T_PRODUCTION_CONTAINER T_INPUT_DATASET T_PRODMANAGER_REQUEST_STATUS T_PROJECTS T_PRODUCTION_STEP T_PRODUCTION_TAG"
PRODSYS1_known_tables="T_PRODUCTIONDATASETS_EXEC T_TASK_REQUEST"
known_ftypes="AVRO TEXT"

usage () {
  echo "
USAGE: oracle-hdfs-import.sh [<options>] table_name [table_name...]

OPTIONS:
  Oracle:
    -b, --database     Oracle database code name:
                        DEFT (default)
                        PRODSYS1
    -o, --tab-owner    Oracle table owner. Default:
                        DEFT: ATLAS_DEFT
                        PRODSYS1: ATLAS_GRISLI
    -t, --timestamp    Upper border for timestamp fields.
                       NOTE: the timestamp will be treated in the Oracle local timezone.
    -h, --ora-host     Connection parameter: host (default: 172.17.34.3)
    -p, --ora-port     Connection parameter: port (default: 12344)
    -s, --ora-service  Connection parameter: service (default: adcr.cern.ch)

  HDFS:
    -f, --file-type    Output file type: {text|avro} (default: avro)
    -m, --type-mapping Comma-separated list of maps: <column_name>=<JAVA type>. 
                       No spaces between maps!
                       This custom mapping will be added to the default one.
                       Default mapping is to treat Oracle NUMBER as Integer.
    -O, --output-dir   Output HDFS directory (default: $BASE_DIR)
    -n, --no-ts-dir    Do not create timestamp subdirectory in output directory.

  Tables:
    -a, --all-tables   Run script for all known tables.
    -e, --exclude      Comma-separated list of tables to be excluded from operation.
                       No spaces between table names!
    -l, --list-tables  List all the known tables (for given DB) and exit.
"
}

while [[ $# > 0 ]]
do
  key="$1"

  case $key in
    -b|--database)
      ORA_DB=`echo "$2" | tr '[:lower:]' '[:upper:]'`
      PW_FILE="${PW_FILE}_${ORA_DB}"
      shift
      ;;
    -f|--filetype)
      HDFS_F_TYPE=`echo "$2" | tr '[:lower:]' '[:upper:]'`
      shift
      ;;
    -O|--output-directory)
      BASE_DIR="$2"
      shift
      ;;
    -o|--tab-owner)
      ORA_T_OWNER=`echo "$2" | tr '[:lower:]' '[:upper:]'`
      shift
      ;;
    -h|--ora-host)
      ORA_HOST="$2"
      shift
      ;;
    -p|--ora-port)
      ORA_PORT="$2"
      shift
      ;;
    -s|--ora-service)
      ORA_SERVICE="$2"
      shift
      ;;
    -m|--type-mapping)
      TYPE_MAPPING=$2
      shift
      ;;
    -a|--all-tables)
      ALL_TABLES=YES
      ;;
    -e|--exclude)
      EXCLUDE_TABLES=`echo "$2" | tr '[:lower:]' '[:upper:]'`
      shift
      ;;
    -l|--list-tables)
      LIST_TABLES=YES
      ;;
    -t|--timestamp)
      TIMESTAMP="$2"
      [ -n "$TS_DIR" ] && TS_DIR=$TIMESTAMP
      shift
      ;;
    -n|--no-ts-dir)
      NO_TS_DIR="YES"
      ;;
    --)
      shift
      break
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

[ -z "$ORA_DB" ] && ORA_DB="DEFT"

case $ORA_DB in
  DEFT)
    connection="jdbc:oracle:thin:@$ORA_HOST:$ORA_PORT/$ORA_SERVICE"
    ora_user='atlas_deft_r'
    [ -z "$ORA_T_OWNER" ] && ORA_T_OWNER='ATLAS_DEFT'
    ;;
  PRODSYS1)
    connection="jdbc:oracle:thin:@$ORA_HOST:$ORA_PORT/$ORA_SERVICE"
    ora_user='atlas_grisli_r'
    [ -z "$ORA_T_OWNER" ] && ORA_T_OWNER='ATLAS_GRISLI'
    ;;
  *)
    echo "Unknown database: $ORA_DB (expected one of: ${known_dbs// /,}.)"
    exit 2
    ;;
esac

kt_varname="${ORA_DB}_known_tables"
known_tables=${!kt_varname}

if [ "x$LIST_TABLES" = "xYES" ]; then
  echo "Known tables for $ORA_DB:
"
  [ -n "$known_tables" ] && echo $known_tables | tr ' ' '\n'  && exit 0
fi  

[ "x$ALL_TABLES" = "xYES" ] && tables=$known_tables

if [ -n "$*" ]; then
  if [ -z "$tables" ]; then
    tables=`echo "$*" | tr '[:lower:]' '[:upper:]'` 
  else
     echo "WARN: Ignoring arguments: $*"
  fi
fi

[ -z "$tables" ] && usage && exit 1
[ -z "$HDFS_F_TYPE" ] && HDFS_F_TYPE='AVRO'
[ -n "$TIMESTAMP" ] && DATETIME="to_date('`TZ=UTC date -d"@$TIMESTAMP" +'%Y-%m-%d %T'`', 'YYYY-MM-DD HH24:MI:SS')"
[ "x$NO_TS_DIR" = "xYES" ] && TS_DIR=

case $HDFS_F_TYPE in
  AVRO)
    filetype="avrodatafile"
    ;;
  TEXT)
    filetype="textfile"
    ;;
  *)
    echo "Unknown file type: $HDFS_F_TYPE (expected one of: ${known_ftypes// /,}.)"
    exit 2
    ;;
esac

for ORA_TABLE in $tables
do
  [[ "$EXCLUDE_TABLES" =~ (^|,)"$ORA_TABLE"(,|$) ]] && echo "Skipping $ORA_TABLE." && continue
  query=
  columns=
  split_by=
  default_map=
  where=
  if [ "x$ORA_DB" = "xDEFT" ]; then
    case $ORA_TABLE in
      T_PRODMANAGER_REQUEST)
        columns="PR_ID,MANAGER,DESCRIPTION,REFERENCE_LINK,STATUS,PROVENANCE,"\
"REQUEST_TYPE,CAMPAIGN,SUB_CAMPAIGN,PHYS_GROUP,ENERGY_GEV,PROJECT,"\
"REFERENCE,EXCEPTION,LOCKED,IS_FAST"
        split_by=PR_ID
        default_map="PR_ID=Long,ENERGY_GEV=Integer,EXCEPTION=Integer,LOCKED=Integer,IS_FAST=Integer"
        ;;
      T_PRODUCTION_TASK)
        columns='*'
        split_by=PR_ID
        default_map="TASKID=Long,STEP_ID=Integer,PR_ID=Long,PARENT_TID=Long,TOTAL_EVENTS=Long,"\
"TOTAL_REQ_JOBS=Long,TOTAL_DONE_JOBS=Long,PRIORITY=Integer,"\
"CURRENT_PRIORITY=Integer,CHAIN_TID=Long,DYNAMIC_JOB_DEFINITION=Integer,"\
"BUG_REPORT=Long,TOTAL_REQ_EVENTS=Long,PILEUP=Integer,"\
"NFILESTOBEUSED=Long,NFILESUSED=Long,NFILESFINISHED=Long,"\
"NFILESFAILED=Long,NFILESONHOLD=Long,IS_EXTENSION=Integer"
        [ -n "$DATETIME" ] && where="TIMESTAMP is NULL or TIMESTAMP < $DATETIME"
        ;;
      T_PRODUCTION_DATASET)
        columns='*'
        split_by=NAME
        default_map="TASKID=Long,PARENT_TID=Long,PR_ID=Long,"\
"EVENTS=Long,FILES=Integer,CONTAINER_FLAG=Integer"
        [ -n "$DATETIME" ] && where="TIMESTAMP is NULL or TIMESTAMP < $DATETIME"
        ;;
      T_PRODUCTION_CONTAINER)
        columns='*'
        split_by=NAME
        default_map="PARENT_TID=Long,PR_ID=Long"
        [ -n "$DATETIME" ] && where="TIMESTAMP is NULL or TIMESTAMP < $DATETIME"
        ;;
      T_INPUT_DATASET)
        columns='*'
        split_by=IND_ID
        default_map="IND_ID=Long,SLICE=Long,PR_ID=Long,PRIORITY=Long,"\
"INPUT_EVENTS=Long,HIDED=Integer,CLONED_FROM=Long"
        ;;
      T_PRODMANAGER_REQUEST_STATUS)
        columns='*'
        split_by=REQ_S_ID
        default_map="REQ_S_ID=Long,PR_ID=Long"
        [ -n "$DATETIME" ] && where="TIMESTAMP is NULL or TIMESTAMP < $DATETIME"
        ;;
      T_PROJECTS)
        columns='*'
        split_by=PROJECT
        default_map="BEGIN_TIME=Long,END_TIME=Long,TIMESTAMP=Long"
        [ -n "$DATETIME" ] && where="TIMESTAMP is NULL or TIMESTAMP < $TIMESTAMP"
        ;;
      T_PRODUCTION_STEP)
        columns='*'
        split_by=STEP_ID
        default_map="STEP_ID=Long,PRIORITY=Integer,INPUT_EVENTS=Long,"\
"PR_ID=Long,STEP_T_ID=Long,IND_ID=Long,STEP_PARENT_ID=Long"
        [ -n "$DATETIME" ] && where="(STEP_DEF_TIME is NULL or STEP_DEF_TIME < $DATETIME) AND "\
"(STEP_APPR_TIME is NULL or STEP_APPR_TIME < $DATETIME) AND "\
"(STEP_EXE_TIME is NULL or STEP_EXE_TIME < $DATETIME) AND "\
"(STEP_DONE_TIME is NULL or STEP_DONE_TIME < $DATETIME)"
        ;;
      T_PRODUCTION_TAG)
        columns='NAME,TRF_NAME,TRF_CACHE,TRF_RELEASE,dbms_lob.substr(TAG_PARAMETERS, 1000,1) AS TAG_PARAMETERS,USERNAME,CREATED,TASKID,STEP_T_ID'
        split_by=TASKID
        default_map="TASKID=Long,STEP_T_ID=Long"
        [ -n "$DATETIME" ] && where="CREATED is NULL or CREATED < $DATETIME"
        ;;
      *)
        echo "Unknown table name: $ORA_TABLE (expected one of: ${known_tables// /,}.)"
        exit 2
        ;;
    esac
  elif [ "x$ORA_DB" = "xPRODSYS1" ]; then
    case $ORA_TABLE in
      T_PRODUCTIONDATASETS_EXEC)
        columns='*'
        split_by=TASK_ID # Mind that it is not a unique value; PK is (task_id, task_pid)
        default_map="TASK_ID=Long,TASK_PID=Long,FILES=Integer,EVENTS=Integer,"\
"TIMESTAMP=Long,FILE_SIZE_MB=Long"
        [ -n "$DATETIME" ] && where="TIMESTAMP is NULL or TIMESTAMP < $TIMESTAMP"
        ;;
      T_TASK_REQUEST)
        columns='*'
        split_by=REQID
        default_map="REQID=Long,CPUPEREVENT=Integer,MEMORY=Integer,FIRST_INPUTFILE_N=Integer,"\
"TOTAL_INPUT_FILES=Integer,TOTAL_EVENTS=Long,EVENTS_PER_FILE=Integer,PRIORITY=Integer,TIMESTAMP=Long,"\
"UPDTIME=Long,TOTAL_REQ_JOBS=Long,TOTAL_DONE_JOBS=Long,TOTAL_F_EVENTS=Long,"\
"TOTAL_AVAIL_JOBS=Long,PARENT_TID=Long,BUG_REPORT=Long,BUG_REFERENCE=Long,STARTTIME=Long,"\
"PPTIMESTAMP=Long"
        [ -n "$DATETIME" ] && where="TIMESTAMP is NULL or TIMESTAMP < $TIMESTAMP"
        ;;
      *)
        echo "Unknown table name: $ORA_TABLE (expected one of: ${known_tables// /,}.)"
        exit 2
        ;;
    esac
  fi
 
  [ -z "$columns" ] && columns = '*'
  [ -n "$ORA_T_OWNER" ] && table="${ORA_T_OWNER}.${ORA_TABLE}" || table=$ORA_TABLE
  [ -z "$where" ] && where='WHERE $CONDITIONS' || where="WHERE \$CONDITIONS AND $where"
  [ -z "$query" ] && query="SELECT $columns FROM $table $where"
  map=`echo "$default_map" "$TYPE_MAPPING" | tr ' ' ','`
  [ -n "$map" ] && map_column_java="--map-column-java $map"

  sqoop import                                \
    --as-$filetype                            \
    --target-dir $BASE_DIR/$ORA_TABLE/$TS_DIR \
    $RM_TG_DIR                                \
    --connect $connection                     \
    --query "$query"                          \
    --split-by $split_by                      \
    $map_column_java                          \
    --username $ora_user                      \
    --password-file file://$PW_FILE

done
