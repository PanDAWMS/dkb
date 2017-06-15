#!/bin/sh

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

LIST='es_rpms es_coinfig'
[ -n "$1" ] && LIST="$*"
for action in $LIST; do
  $action
done
