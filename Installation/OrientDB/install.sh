#!/bin/bash

# OrientDB installation
# Requirements: Java >= 1.7

[ -z "$ODB_HOME" ] && ODB_HOME="/usr/local/orientdb"
[ -z "$ODB_VERSION" ] && ODB_VERSION="2.2.21"
[ -z "$ODB_USER" ] && ODB_USER="orientdb"
[ -z "$ODB_LOG_DIR" ] && ODB_LOG_DIR="/var/log/orientdb"

! [ `which java` ] \
    && echo "Java is not installed." >&2 \
    && exit 1

JAVA_VERSION=`java -version 2>&1 | head -n 1 | cut -d' ' -f 3 | tr -d '"'`

JAVA_MAJOR=`echo $JAVA_VERSION | cut -d'.' -f 1`
JAVA_MINOR=`echo $JAVA_VERSION | cut -d'.' -f 2`

( [ $JAVA_MAJOR -lt 1 ] || [ $JAVA_MINOR -lt 7 ] ) \
    && echo "Java version >= 1.7 is required" >&2 \
    && exit 1

_install() {
    if ! [ -d ~/git.orientdb ] ; then
        git clone https://github.com/orientechnologies/orientdb \
           ~/git.orientdb
    fi
    cd ~/git.orientdb
    git config --f ./.gitattributes core.autocrlf false
    sed -i.bak -e's/ = /=/' ./.gitattributes
    git checkout -- `git ls-files -m`
    git checkout master
    git pull
    git checkout tags/$ODB_VERSION

    if ! [ -d $ODB_HOME/$ODB_VERSION ]; then
        DIR=distribution/target/orientdb-community-$ODB_VERSION.dir
        DIR=$DIR/orientdb-community-$ODB_VERSION
        if ! [ -d $DIR ]; then
            mvn clean install -DskipTests
            r=$?
            if [ $r != 0 ]; then
                echo "Maven failed to install OrientDB. Return code: $r"
                exit $r
            fi
        fi

        mkdir -p $ODB_HOME
        [ $? != 0 ] && return 1
        mv -T $DIR $ODB_HOME/$ODB_VERSION
    fi
    [ -L $ODB_HOME/default ] && unlink $ODB_HOME/default
    ln -f -s $ODB_HOME/$ODB_VERSION $ODB_HOME/default
}

_configure() {
    cfg_common \
        && cfg_service \
        && cfg_user mgolosova \
        && cfg_user mgri
}

cfg_common() {
    cd $ODB_HOME/default
    id "$ODB_USER" &>/dev/null
    [ $? != 0 ] && useradd -c "OrientDB user" -r $ODB_USER


    pwd
    chmod 755 bin/*.sh
    chmod -R 600 config
    chmod u+x config

    chown -R orientdb $ODB_HOME

    mkdir -p $ODB_LOG_DIR
    chown -R orientdb $ODB_LOG_DIR

    sed -i.bak -r -e"s%YOUR_ORIENTDB_INSTALLATION_PATH%$ODB_HOME/default%" \
                  -e"s%USER_YOU_WANT_ORIENTDB_RUN_WITH%$ODB_USER%" \
                  -e"s%(LOG_DIR=)\"[./a-zA-Z_]*\"$%\1\"$ODB_LOG_DIR\"%" \
                  bin/orientdb.sh
}

cfg_service() {
    if [ `which systemctl 2>/dev/null` ]; then
        service_systemd
    else
        service_init
    fi
}

service_systemd() {
    cat << EOF > /etc/systemd/system/orientdb.service
[Unit]
Description=OrientDB Graph Database
After=network.target
After=syslog.target

[Install]
WantedBy=multi-user.target

[Service]
Type=forking
PIDFile=$ODB_HOME/default/orientdb-service.pid
User=$ODB_USER
Group=$ODB_USER
ExecStart=$ODB_HOME/default/bin/orientdb.sh start
ExecStop=$ODB_HOME/default/bin/orientdb.sh stop
ExecStatus=$ODB_HOME/default/bin/orientdb.sh status
EOF

systemctl enable orientdb.service
}

service_init() {
    [ -L /etc/init.d/orientdb ] && unlink /etc/init.d/orientdb
    ln -f -s $ODB_HOME/default/bin/orientdb.sh /etc/init.d/orientdb
    [ -L /etc/rc5.d/S45orientdb ] && unlink /etc/rc5.d/S45orientdb
    ln -f -s /etc/init.d/orientdb /etc/rc5.d/S45orientdb
}

cfg_user() {
    CFG=$ODB_HOME/default/config/orientdb-server-config.xml
    username=$1
    case "$username" in
        mgolosova)
            pw="{PBKDF2WithHmacSHA256}A0CC259561B0FD89F90300FFDC18D532FF64EF60752B1CE9:1A3F920FBD7F2744E86798E58F86EAF6039B4636FB410B8B:65536"
            res="*"
            ;;
        mgri)
            pw="{PBKDF2WithHmacSHA256}F422CA93379CFA94D6FF7BA07098365A825A589BCE0A7755:B452D5FED2F4BF3859E7C04C21C811A7F1D1569BEDAF9BC2:65536"
            res="server.connect,server.listDatabases,database.create"
            ;;
        *)
            pw="changeImmediately"
            res="server.connect"
            ;;
    esac

    newline="\ \ \ \ \ \ \ \ <user resources=\"$res\" password=\"$pw\" name=\"$username\"/>"
    grep 'name="'$username'"' $CFG &>/dev/null
    if [ $? == 0 ]; then
      sed -i.bak.$username -e'/name="'"$username"'"'"/c\
    $newline" \
    $CFG
    else
      sed -i.bak.$username -e"/<users>/a\
    $newline" \
    $CFG
    fi
}

_install \
    && _configure \
    && service orientdb start
