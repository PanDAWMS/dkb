#!/bin/sh
KIBANA_MODE='rpms'
#KIBANA_DIR='/usr/local/kibana'	#uncomment for binary installation
[ -n "$KIBANA_DIR" ] && KIBANA_MODE='bin'

#ES and kibana versions must be in sync.
VERSION='5.4.1'

es_config() {
  CONFIG_FILE='/etc/elasticsearch/elasticsearch.yml'
  LOG_DIR='/var/log/es/'
  DATA_DIR='/var/es/data/'
  [ ! -f "${CONFIG_FILE}.orig" ] && mv $CONFIG_FILE{,.orig} 
  cat <<EOF >/etc/elasticsearch/elasticsearch.yml
cluster.name: dkb
node.name: `hostname -s`
path.data: ${DATA_DIR}
path.logs: ${LOG_DIR}
network.host: 127.0.0.1
http.port: 9200
EOF
  for d in $LOG_DIR $DATA_DIR; do
    mkdir -p "$d"
    chown elasticsearch:elasticsearch "$d" -R
  done
  chkconfig --levels 345 elasticsearch on
  service elasticsearch start
}

es_rpms() {
  cat <<EOF >/etc/yum.repos.d/es.repo
[elasticsearch-5.x]
name=Elasticsearch repository for 5.x packages
baseurl=https://artifacts.elastic.co/packages/5.x/yum
gpgcheck=1
gpgkey=https://artifacts.elastic.co/GPG-KEY-elasticsearch
enabled=1
autorefresh=1
type=rpm-md
EOF
  yum install -y elasticsearch-$VERSION
}

kibana_rpms() {
  cat <<EOF >/etc/yum.repos.d/kibana.repo
[kibana-5.x]
name=Kibana repository for 5.x packages
baseurl=https://artifacts.elastic.co/packages/5.x/yum
gpgcheck=1
gpgkey=https://artifacts.elastic.co/GPG-KEY-elasticsearch
enabled=1
autorefresh=1
type=rpm-md
EOF
  yum install -y kibana-$VERSION
}
kibana_bin() {
  BIN_FILE="kibana-${VERSION}-linux-x86_64.tar.gz"
  wget https://artifacts.elastic.co/downloads/kibana/$BIN_FILE -O $BIN_FILE
  mkdir -p "$KIBANA_DIR"
  tar -xzf $BIN_FILE -C $KIBANA_DIR --strip-components=1
  rm -f "$BIN_FILE"
  grep -e '^kibana' /etc/passwd && useradd -r kibana
  chown kibana:kibana "$KIBANA_DIR" -R
}
kibana_config() {
  KIBANA_CONFIG='/etc/kibana/kibana.yml'
  mkdir -p `dirname $KIBANA_CONFIG`
  [ -f "$KIBANA_CONFIG.orig" ] && mv $KIBANA_CONFIG{,.orig}
  cat <<EOF >$KIBANA_CONFIG
server.port: 5601
server.host: 127.0.0.1
elasticsearch.url: http://127.0.0.1:9200
server.name: `hostname -s`
EOF
  [ "$KIBANA_MODE" == 'bin' ] && \
    cp kibana.init.d /etc/init.d/kibana && \
    sed -i -e "s|%%%KIBANA_DIR%%%|$KIBANA_DIR|" /etc/init.d/kibana && \
    chkconfig --add kibana
  chkconfig --levels 345 kibana on
  service kibana start
}

LIST="es_rpms es_coinfig kibana_${KIBANA_MODE} kibana_config"
[ -n "$1" ] && LIST="$*"
for action in $LIST; do
  $action
done
