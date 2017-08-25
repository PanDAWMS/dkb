#!/bin/sh

HOSTS='itrac{5101,5129,5137,5170}.cern.ch'
PORT=10121
TIMEOUT=3600
if [ "$1" == "" ]; then
  ORACLE_HOST=`eval echo $HOSTS | tr ' ' '\n' | shuf -n1`
else 
  ORACLE_HOST="$1"
fi

echo "Will create tunnel to ${ORACLE_HOST}:${PORT} for $TIMEOUT seconds"

ssh lxplus.cern.ch -L $PORT:${ORACLE_HOST}:${PORT} "echo 'Done!'; sleep $TIMEOUT"

