#!/usr/bin/env bash

usage () {
  echo "
Get information about papers and supporting documents from GLANCE server.

USAGE:
  local_papersFromGLANCE -u USER [<options>] 

OPTIONS:

OUTPUT
  -f, --file     FILE           Output to FILE.
  -p, --pipe     PIPE           Create named pipe PIPE and output to it.
				Note that when outputting to pipe, programm is
                                going to hang unless there's someone to read
                                from the pipe.
				If neither FILE nor PIPE is specified, output
                                goes to STDOUT.

MARKERS
  -e, --eom      EOM            End-of-message marker.
        Note that EOM goes after a message to $tmp.
                                Default: '\n'
  -E, --eop      EOP            End-of-process marker.
        Note that EOP always goes to standard output.
                                Default:
                                Stream mode - '\0'
                                File mode - ''

KERBEROS
  -u, --username USER           Cern account login.
  -r, --retry                   Try to get new Kerberos ticket in case of
                                expired one or if it is not found.
GENERAL
  -t, --tmp                     Do not remove local temporary file.
  -h, --help                    Print this message and exit.
"
}

EOMessage='\n'

EOP_set="N"
EOM_set="N"

while [[ $# > 0 ]]
do
  key="$1"
  case $key in
    -h|--help)
      usage
      exit
      ;;
    -f|--file)
      FILE="$2"
      shift
      ;;
    -p|--pipe)
      PIPE="$2"
      shift
      ;;
    -r|--retry)
      RETRY="YES"
      ;;
    -u|--user)
      USR="$2"
      shift
      ;;
    -t|--tmp)
      CLEAN="NO"
      ;;
    -e|--eom)
      EOM="$2"
      EOM_set="Y"
      shift
      ;;
    -E|--eop)
      EOP="$2"
      EOP_set="Y"
      shift
      ;;
    -*)
      echo "Unknown option: $key" >&2
      usage >&2
      exit 1
      ;;
    *)
      break
      ;;
  esac
  shift
done

[ -z "$CLEAN" ] && CLEAN="YES"
if [ -z "$USR" ] ; then
  echo "Username is not specified." >&2
  usage >&2
  exit 1
fi

if [ -n "$PIPE" ]; then
  if ! ( [ -p "$PIPE" ] || mkfifo "$PIPE" 2>&1 > /dev/null ) ; then
    echo "Can not create a FIFO named pipe: $PIPE. Exiting." >&2
    exit 2
  fi
  FILE="$PIPE"
fi

if [ -n "$FILE" ] || [ -n "$PIPE" ]
then
  EOProcess=""
else
  EOProcess="\0"
fi

[ "$EOM_set" == "Y" ] && EOMessage="$EOM"
[ "$EOP_set" == "Y" ] && EOProcess="$EOP"

if [ -z "$EOMessage" ] ; then
  echo "EOM marker is not specified. Exiting." >&2
  exit 1
fi

GLANCE_REQUEST="https://glance-stage.cern.ch/api/atlas/analysis/papers"
cookie='~/glance.cookie'
json='~/list_of_papers.json'
cmd1="cern-get-sso-cookie --nocertverify --krb --url $GLANCE_REQUEST --outfile $cookie"
cmd2="curl -k -L -s -f --cookie $cookie --cookie-jar $cookie $GLANCE_REQUEST -o $json"
cmd3="cat $json"
cmd4="rm $cookie $json"

# Check Kerberos ticket
if ! krenew -H 60 ; then
  if [ -n "$RETRY" ]; then
    kinit "$USR@CERN.CH"
  else
   echo "No Kerberos ticket for CERN.CH realm found. Exiting." >&2
   exit 3 
  fi
fi
klist | grep 'CERN.CH' >/dev/null || { echo "No Kerberos ticket for CERN.CH realm found. Exiting." >&2; exit 3; }

# Lxplus address
lxplus=lxplus.cern.ch

option="-q -o StrictHostKeyChecking=no -o GSSAPIAuthentication=yes -o GSSAPIDelegateCredentials=yes -o GSSAPITrustDNS=yes"
ssh $option -K $USR@$lxplus "( $cmd1; $cmd2; ) 2>&1 >/dev/null" 2>&1 > /dev/null
[ $? ] || { echo "Can not execute remote commands. Exiting." >&2; exit 4; }

tmp=/tmp/list_of_papers.json
scp $option $USR@$lxplus:$json $tmp >&2

[ $? ] || { echo "Can not copy file from remote. Exiting." >&2; exit 4; }

echo -ne "$EOMessage" >> $tmp

if [ -n "$FILE" ] ; then
  cat $tmp > $FILE
else
  cat $tmp
fi

echo -ne "$EOProcess"

[ "$xCLEAN" = "xYES" ] && rm $tmp >&2

ssh $option -K $USR@$lxplus "$cmd4;" 2>&1 > /dev/null 
[ $? ] || { echo "Can not execute remote commands. Exiting." >&2; exit 4; }
