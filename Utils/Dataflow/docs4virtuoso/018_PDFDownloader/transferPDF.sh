#!/bin/bash

# Downloads PDF file from CDS and upload copy to the HDFS
#
# USAGE:
#     ./transferPDF.sh URL [ REQUIRED_NAME ]

PID=$$
HDFS_DIR="/user/DKB/store/PDF"
base_dir=`dirname $0`

. $base_dir/switch_realm

usage() {
    echo "
USAGE:
    $(basename $0) url [req-name]

PARAMETERS:
    url      -- URL with the original file
    req-name -- required file name to store in HDFS
" >&2
}


upload() {
    # Upload file to HDFS
    [ -z "$1" ] && return 1
    switch_realm kiae || return $?
    [ -z "$2" ] && hdfs_file=$HDFS_DIR/`basename $1` || hdfs_file=$HDFS_DIR/$2
    hadoop fs -put -f $1 $HDFS_DIR/$2
    ret=$?
    [ $ret -eq 0 ] && export hdfs_file
    rm -f $1
    return $ret
}


download() {
    # Download file from the original location
    [ -z "$1" ] && return 1
    switch_realm cern || return $?
    url=$1
    cookie=/tmp/cern-sso-cookie-$PID
    tmpf=`mktemp /tmp/XXXXXXX.pdf`

    cern-get-sso-cookie --krb -r -u "$url" -o $cookie

    if [ $? -ne 0 ]; then
        echo "Failed to get CERN SSO cookie." >&2
        exit 3
    fi

    curl -k -L -f -s  --cookie $cookie --cookie-jar $cookie $url -o $tmpf || tmpf=
    export local_file=$tmpf
}

which cern-get-sso-cookie 2>&1 >/dev/null

if [ $? -ne 0 ]; then
    echo "cern-get-sso-cookie is not installed.
See http://linux.web.cern.ch/linux/docs/cernssocookie.shtml for details." >&2
    exit 2
fi

if [ -z "$1" ]; then
    usage
    exit 1
fi


download $1 >&2 \
  && upload $local_file $2 >&2

ret=$?

echo "$hdfs_file"

exit $ret
