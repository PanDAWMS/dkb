#!/bin/sh
KIBANA_MODE='rpms'
#KIBANA_DIR='/usr/local/kibana'	#uncomment for binary installation
[ -n "$KIBANA_DIR" ] && KIBANA_MODE='bin'

#ES and kibana versions must be in sync.
VERSION='5.4.1'
KIBANA_HOST='127.0.0.1'
KIBANA_PORT=5601
ES_HOST='127.0.0.1'
ES_PORT='9200'

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
network.host: ${ES_HOST}
http.port: ${ES_PORT}
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
server.port: ${KIBANA_PORT}
server.host: ${KIBANA_HOST}
elasticsearch.url: http://${ES_HOST}:${ES_PORT}
server.name: `hostname -s`
EOF
  [ "$KIBANA_MODE" == 'bin' ] && \
    cp kibana.init.d /etc/init.d/kibana && \
    sed -i -e "s|%%%KIBANA_DIR%%%|$KIBANA_DIR|" /etc/init.d/kibana && \
    chkconfig --add kibana
  chkconfig --levels 345 kibana on
  service kibana start
}

nginx_rpms() {
  yum install -y nginx
  mv /etc/nginx/conf.d/ssl.conf{,.disabled}
  rm -f /etc/nginx/conf.d/*.conf
}

nginx_config() {
  ES_HTPASSWD='/etc/nginx/es.htpasswd'
  KIBANA_HTPASSWD='/etc/nginx/kibana.htpasswd'
  cat <<EOF >/etc/nginx/conf.d/es.conf
server {
  server_name  `hostname --fqdn`;
  listen 	`hostname --fqdn`:${ES_PORT};
  location / {
            access_log /var/log/nginx/es.access.log main;
            error_log  /var/log/nginx/es.error.log  ;
            auth_basic "Please provide ES user and password";
            auth_basic_user_file $ES_HTPASSWD;
            proxy_pass         http://${ES_HOST}:${ES_PORT};
            proxy_set_header   Host               \$host;
            proxy_set_header   X-Real-IP          \$remote_addr;
            proxy_set_header   X-Forwarded-For    \$proxy_add_x_forwarded_for;
            client_body_buffer_size     128k;
            client_max_body_size        256m;

            proxy_connect_timeout       90;
            proxy_send_timeout          90;
            proxy_read_timeout          90;

            proxy_buffer_size           256k;
            proxy_buffers               8 256k;
            proxy_busy_buffers_size     512k;
            proxy_temp_file_write_size  512k;
        }
}
EOF

  cat <<EOF >/etc/nginx/conf.d/kibana.conf
server {
  server_name  `hostname --fqdn`;
  listen       `hostname --fqdn`:${KIBANA_PORT};
  location / {
            access_log /var/log/nginx/kibana.access.log main;
            error_log  /var/log/nginx/kibana.error.log  ;
            auth_basic "Please provide Kibana user and password";
            auth_basic_user_file $KIBANA_HTPASSWD;
            proxy_pass         http://${KIBANA_HOST}:${KIBANA_PORT};
            proxy_set_header   Host               \$host;
            proxy_set_header   X-Real-IP          \$remote_addr;
            proxy_set_header   X-Forwarded-For    \$proxy_add_x_forwarded_for;
            client_body_buffer_size     128k;

            proxy_connect_timeout       90;
            proxy_send_timeout          90;
            proxy_read_timeout          90;

            proxy_buffer_size           256k;
            proxy_buffers               8 256k;
            proxy_busy_buffers_size     512k;
            proxy_temp_file_write_size  512k;
        }
}
EOF

  htpasswd -b -n esuser "`cat es.passwd`" | head -n1 > "$ES_HTPASSWD"
  htpasswd -b -n kuser "`cat kibana.passwd`" | head -n1 > "$KIBANA_HTPASSWD"

  chkconfig --levels 345 nginx on
  service nginx status && service nginx reload || service nginx start
}

LIST="es_rpms es_coinfig kibana_${KIBANA_MODE} kibana_config nginx_rpms nginx_config"
[ -n "$1" ] && LIST="$*"
for action in $LIST; do
  $action
done
