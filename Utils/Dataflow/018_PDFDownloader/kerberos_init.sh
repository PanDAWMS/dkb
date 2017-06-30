#/bin/bash

# Initialize Kerberos tickets for KIAE and CERN realms

KRB_TMP=~/.krb5
KRB_TKT=krb5cc_$UID

mkdir -p "$KRB_TMP"

krb_init() {
    # Call kinit for given realm and put ticket to a tmp directory
    [ -z "$1" ] && return 1
    case $1 in
        cern)
            realm="CERN.CH"
            ;;
        kiae)
            realm="HADOOP.NOSQL.KIAE.RU"
            ;;
        *)
            echo "Unknown realm: $1" >&2
    esac

    read -p "Username ($realm) ($USER): " user
    [ -z "$user" ] && user=$USER

    kinit -c "${KRB_TMP}/${KRB_TKT}.${1}" "$user"@"$realm"
}

for realm in kiae cern; do
    krb_init $realm
done
