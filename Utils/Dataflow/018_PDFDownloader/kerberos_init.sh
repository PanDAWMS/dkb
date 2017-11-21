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
            def_user=`echo "$USER" | cut -c 1-8`
            ;;
        kiae)
            realm="HADOOP.NOSQL.KIAE.RU"
            def_user=$USER
            ;;
        *)
            echo "Unknown realm: $1" >&2
    esac

    read -p "Username ($realm) ($def_user): " user
    [ -z "$user" ] && user=$def_user

    kinit -c "${KRB_TMP}/${KRB_TKT}.${1}" "$user"@"$realm"
}

for realm in kiae cern; do
    krb_init $realm
done
