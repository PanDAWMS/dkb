#!/bin/sh
usage() {
  cat <<EOF
Usage:
  $0 Docerfile.Something
EOF
}
FILE_NAME=`basename $1`
[ "${FILE_NAME%.*}" != "Dockerfile" ] && usage && exit 1

docker build \
  --file="$1" \
  --force-rm=true \
  --label="${FILE_NAME#*.}" \
  --no-cache=true \
  --rm=true \
  --tag="`echo ${FILE_NAME#*.} | tr '[A-Z]' '[a-z]'`" \
  .
