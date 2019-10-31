#!/bin/bash

env_vars=.env
( set -o posix; set; ) > $env_vars

WWW_DIR=/data/www/dkb
RUN_DIR=/var/run/wsgi
LOG_DIR=/var/log/dkb
CFG_DIR="conf"
SOCK="$RUN_DIR"/api-fcgi.socket
APP_USER=www
APP_GROUP=
ADDR=127.0.0.1:5080
MANAGE_WWW=1
MANAGE_NGINX=
NGINX_DIR=/etc/nginx
NGINX_USER=
NGINX_GROUP=
MANAGE_SERVICE=
MANAGE_SEL=


base_dir=$(readlink -f $(cd $(dirname "$0"); pwd))
build_dir="${base_dir}/build"
cfg_file=~/.dkb-api

usage() {
  echo "
USAGE
  $(basename "$0") [options] command ...

COMMANDS
  clean     clean up build directory
  build     extend files with parameter values
  install   build and copy files to system directories
  uninstall remove WWW files, remove service,
              undo changes in SELinux policy
  start     start application
  stop      stop application
  restart   restart application
  status    check application status

OPTIONS
  GENERAL
    -h, --help       show this message and exit

    -w, --www        manage www files (default)

    -W, --no-www     do not manage www files

    -n, --nginx      manage Nginx config file

    -S, --service    manage Linux service

    --sel            manage SELinux settings

    -a, --all        manage all of the above:
                     equivalent to '-w -n -S --sel'

  APPLICATION
    -d, --dest WWW_DIR
                     destination directory for installation
                     Default: $WWW_DIR

    -c, --cfg-dir DIR
                     path to directory with configuration files used
                     by the application (absolute or relative to WWW_DIR)
                     Default: WWW_DIR/$CFG_DIR

    -l, --log-dir DIR
                     directory for (application) log files
                     Default: $LOG_DIR

    -r, --run-dir DIR
                     directory for .pid file
                     Default: $RUN_DIR

    -s, --sock       socket name for communication between web-server
                     and API application
                     Default: $SOCK

    -u, --user       owner of application process
                     Default: $APP_USER

  WEB-SERVER
    --nginx-dir DIR         Nginx home directory
                            Default: $NGINX_DIR

    -l, --listen HOST:PORT  address for Nginx to listen
                            Default: $ADDR

  INSTALLATION PARAMETERS

    --restore-defaults
                     restore default configuration parameters
                     (remove stored configuration) and exit

    --restore-cfg    restore previous configuration and exit

    --show-cfg       show stored configuration and exit
"
}

load_cfg() {
  [ -f "$cfg_file" ] || return 1
  . "$cfg_file"
}

save_cfg() {
  vars=`set -o posix; set | grep ^[A-Z] | grep -v -e ^BASH -e ^FUNCNAME -e ^PIPESTATUS`
  new_cfg=`echo "$vars" | grep -v -f "$env_vars"`
  echo "$new_cfg" > "$cfg_file.tmp"
  # If config has changed -- save previous in .old file
  [ -f "$cfg_file" ] && diff "$cfg_file"{,.tmp} &> /dev/null && mv "$cfg_file"{,.old}
  mv "$cfg_file"{.tmp,}
}

[[ "$*" =~ "--help" ]] || load_cfg

while [ $# -gt 0 ]; do
  case "$1" in
    -h|--help)
      usage >&2
      exit 0
      ;;
    -d|--dest)
      WWW_DIR=`readlink -m "$2"`
      shift
      ;;
    -c|--cfg-dir)
      [[ "$2" == /* || "$2" == .* ]] \
        && CFG_DIR=`readlink -m "$2"` \
        || CFG_DIR="$2"
      shift
      ;;
    -l|--log-dir)
      LOG_DIR=`readlink -m "$2"`
      shift
      ;;
    -r|--run-dir)
      RUN_DIR=`readlink -m "$2"`
      shift
      ;;
    -s|--sock)
      SOCK="$2"
      shift
      ;;
    -u|--user)
      APP_USER="$2"
      shift
      ;;
    -w|--www)
      MANAGE_WWW=1
      ;;
    -W|--no-www)
      MANAGE_WWW=
      ;;
    -n|--nginx)
      MANAGE_NGINX=1
      ;;
    --nginx-dir)
      NGINX_DIR=`readlink -m "$2"`
      shift
      ;;
    -l|--listen)
      ADDR="$2"
      shift
      ;;
    -S|--service)
      MANAGE_SERVICE=1
      ;;
    --sel)
      MANAGE_SEL=1
      ;;
    -a|--all)
      MANAGE_WWW=1
      MANAGE_NGINX=1
      MANAGE_SEL=1
      MANAGE_SERVICE=1
      ;;
    --restore-defaults)
      [ -f "$cfg_file" ] && mv "$cfg_file"{,.old}
      exit
      ;;
    --restore-cfg)
      [ -f "${cfg_file}.old" ] \
        || { echo "No configuration file found to restore." >&2 && exit 1; }
      [ -f "$cfg_file" ] && mv "$cfg_file"{,.tmp}
      mv "$cfg_file"{.old,}
      [ -f "${cfg_file}.tmp" ] && mv "$cfg_file"{.tmp,.old}
      exit
      ;;
    --show-cfg)
      [ -f "$cfg_file" ] \
        && cat "$cfg_file" \
        || echo "No stored configuration found ($cfg_file)." >&2
      exit
      ;;
    --)
      break
      ;;
    -*)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
    *)
      break
      ;;
  esac
  shift
done

save_cfg

get_nginx_user() {
  conf=$NGINX_DIR/nginx.conf
  [ ! -f "$conf" ] \
    && echo "Failed to find Nginx main config file ($conf)" >&2 \
    && exit 1
  NGINX_USER=$(grep -E "^ *user" "$conf" | awk '{print $2}' | tr -d ';')
  echo "Nginx user: $NGINX_USER" >&2
  NGINX_GROUP=`id -gn "$NGINX_USER"`
  echo "Nginx group: $NGINX_USER" >&2
}

ensure_user() {
  [ -z "$APP_USER" ] && APP_USER=$NGINX_USER
  [ "$APP_USER" == "$NGINX_USER" ] && return 0
  id "$APP_USER" &>/dev/null
  [ ! $? -eq 0 ] && echo "User does not exists ($APP_USER)" >&2 && exit 2
  return 0
}

ensure_dirs() {
  dirs="$WWW_DIR $LOG_DIR $RUN_DIR"
  for dir in $dirs; do
    echo "Creating dir: $dir" >&2
    [ -d "$dir" ] && continue
    mkdir -p "$dir" &>/dev/null
    chown "$APP_USER:$NGINX_GROUP" "$dir"
    chmod 2750 "$dir"
  done
}

build_file() {
  [ -z "$1" ] && echo "build_file(): filename not passed." >&2 && exit 1
  echo "Building file: $1" >&2
  [[ "$CFG_DIR" == /* ]] || CFG_DIR="${WWW_DIR}/${CFG_DIR}"
  sed -e "s#%%SOCK%%#${SOCK_DIR}/${SOCK}#g" \
      -e "s#%%ADDR%%#$ADDR#g" \
      -e "s#%%WWW_DIR%%#$WWW_DIR#g" \
      -e "s#%%CFG_DIR%%#$CFG_DIR#g" \
      "$1"
}

build_nginx_cfg() {
  cfg="nginx.dkb.conf"
  file="$base_dir/$cfg"
  [ ! -f "$file" ] \
    && echo "Source file with Nginx configuration not found ($file)." >&2 \
    && exit 1
  build_file "$file" > "$build_dir/$cfg"
}

install_nginx_cfg() {
  tgt_dir="$NGINX_DIR/conf.d"
  [ ! -d "$tgt_dir" ] \
    && echo "Failed to find Nginx config directory ($tgt_dir)." >&2 \
    && exit 1
  src_file="$build_dir/nginx.dkb.conf"
  tgt_file="$tgt_dir/dkb.conf"
  [ ! -f "$src_file" ] && _build
  [ -f "$tgt_file" ] \
    && cp "$tgt_file" "${tgt_file}.bak"
  echo "Installing Nginx config file..." >&2
  echo "> $tgt_file" >&2
  cp "$src_file" "$tgt_file"
  chown "$NGINX_USER:$NGINX_GROUP" "$tgt_file"
  chmod 0400 "$tgt_file"
  echo "...done." >&2
  echo "Restarting Nginx service..."
  service nginx restart
  echo "...done." >&2
}

uninstall_nginx_cfg() {
  cfg="$NGINX_DIR/conf.d/dkb.conf"
  echo "Removing Nginx config file..." >&2
  echo "> $cfg" >&2
  rm -f "$cfg"
  echo "...done." >&2
  echo "Restarting Nginx service..."
  service nginx restart
  echo "...done." >&2
}

check_systemd() {
  first=`ps -o comm= 1`
  [[ "$first" =~ "systemd" ]] && return 0 || return 1
}

build_service_cfg() {
  old_dir=`pwd`
  cd "$base_dir"
  file="dkb-api.service"
  [ ! -f "$file" ] \
    && echo "Source file with service configuration not found ($file)." >&2 \
    && exit 1
  build_dir="$base_dir/build"
  build_file "$base_dir/$file" > "$build_dir/$file"
  cd "$old_dir"
}

install_service() {
  check_systemd \
    || { echo "Can not install service (not systemd-managed OS)." >&2; return 1; }
  build_dir="$base_dir/build"
  file="dkb-api.service"
  src_file="$build_dir/$file"
  tgt_file="/etc/systemd/system/$file"
  echo "Installing service file..." >&2
  echo "> $tgt_file" >&2
  cp "$src_file" "$tgt_file"
  chown "root:root" "$tgt_file"
  chmod 0400 "$tgt_file"
  echo "...done." >&2
  echo "Enabling service..." >&2
  systemctl reenable ${file%.service} && echo "...done." >&2
}

uninstall_service() {
  check_systemd || return 0
  echo "Stopping and disabling service..." >&2
  systemctl stop dkb-api
  systemctl disable dkb-api
  echo "...done." >&2
  echo "Removing service file..." >&2
  rm -f /etc/systemd/system/dkb-api.service
  systemctl daemon-reload
  echo "...done." >&2
}

build_www() {
  [ ! -r "$base_dir/.files" ] \
    && echo "Config file '.files' is missed in '$base_dir'." \
    && exit 1
  old_pwd=`pwd`
  cd "$base_dir"
  files=`cat .files`
  for f in $files; do
    [ -e "$f" ] || continue
    if [ -d "$f" ]; then
      mkdir -p "$build_dir/$f"
    else
      build_file "$f" > "$build_dir/$f"
    fi
  done
  cd "$old_pwd"
}

install_www() {
  old_dir=`pwd`
  cd "$base_dir"
  [ ! -d "$build_dir" ] && _build
  [ ! -r ".files" ] \
    && echo "Config file '.files' is missed in '$base_dir'." \
    && cd "$old_dir" && exit 1
  files=`cat .files`
  echo "Installing www files..." >&2
  for f in $files; do
    [ -e "$build_dir/$f" ] || continue
    echo "> $WWW_DIR/$f" >&2
    if [ -d "$build_dir/$f" ]; then
      mkdir -p "$WWW_DIR/$f"
      chown "$APP_USER:$NGINX_GROUP" "$WWW_DIR/$f"
      chmod 0744 "$WWW_DIR/$f"
    else
      cp "$build_dir/$f" -T "$WWW_DIR/$f"
      [[ "$f" =~ "bin/" ]] \
        && chmod 500 "$WWW_DIR/$f" \
        || chmod 400 "$WWW_DIR/$f"
    fi
    chown "$APP_USER" "$WWW_DIR/$f"
  done
  sudo -u "$APP_USER" python -m compileall "$WWW_DIR/lib"
  echo "Compiling $WWW_DIR/cgi-bin/dkb.fcgi ..." >&2
  sudo -u "$APP_USER" python -m py_compile "$WWW_DIR/cgi-bin/dkb.fcgi"
  echo "...done." >&2
  cd "$old_dir"
}

uninstall_www() {
  old_dir=`pwd`
  cd "$base_dir"
  [ ! -r ".files" ] \
    && echo "Config file '.files' is missed in '$base_dir'." \
    && cd "$old_dir" && exit 1
  files=`cat .files`
  dirs=""
  echo "Removing www files..." >&2
  for f in $files; do
    if [ -d "$WWW_DIR/$f" ]; then
      dirs="$dirs $WWW_DIR/$f"
    else
      echo "> $WWW_DIR/$f" >&2
      rm -f "$WWW_DIR/$f"
      rm -f "$WWW_DIR/${f%.py}.pyc"
    fi
  done
  for d in $dirs; do
    # Remove directory if it still exists...
    if [ -d "$d" ]; then
      # ...and does not contain files.
      nfiles=`find $d -type f ! -name "*.pyc" | wc -l`
      [ $nfiles -eq 0 ] \
        && echo "> $d" >&2 \
        && rm -rf "$d" >/dev/null \
        || echo "> $d (skipped: not empty)" >&2
    fi
  done
  echo "...done." >&2
  cd "$old_dir"
}

status_www_service() {
  check_systemd && systemctl status dkb-api
}

customize_sel() {
  f="$WWW_DIR/cgi-bin/dkb.fcgi"
  echo "Adding and applying SELinux rule..." >&2
  semanage fcontext -a -t httpd_exec_t "$f"
  restorecon "$f"
  echo "...done" >&2
}

clean_sel() {
  f="$WWW_DIR/cgi-bin/dkb.fcgi"
  echo "Removing SELinux rule..." >&2
  semanage fcontext -d -t httpd_exec_t "$f" &>/dev/null
  restorecon "$f" &>/dev/null
  echo "...done" >&2
}

status_www() {
  if [ -n "$MANAGE_SERVICE" ]; then
    status_www_service
    ret=$?
    [ $ret -ne -1 ] && return $ret
  fi
  pidfile="$RUN_DIR/.pid"
  if ! [ -f "$pidfile" ]; then
    echo "API server is not running."
    return 3
  fi
  pid=`cat "$pidfile"`
  ps p $pid &>/dev/null
  if [ $? -ne 0 ]; then
    echo "API server is not running, but pid file exists."
    return 1
  fi
  echo "API server is running."
  return 0
}

stop_www_service() {
  check_systemd && systemctl stop dkb-api
}

stop_www() {
  if [ -n "$MANAGE_SERVICE" ]; then
    stop_www_service
    ret=$?
    [ $ret -ne -1 ] && return $ret
  fi
  pidfile="$RUN_DIR/.pid"
  [ -f "$pidfile" ] \
    && pid=`cat "$pidfile"` \
    && kill "$pid" &>/dev/null
  rm -f "$pidfile"
}

start_www_service() {
  check_systemd && systemctl start dkb-api
}

start_www() {
  if [ -n "$MANAGE_SERVICE" ]; then
    start_www_service
    ret=$?
    [ $ret -ne -1 ] && return $ret
  fi
  pidfile="$RUN_DIR/.pid"
  logfile="$LOG_DIR/api-server.log"
  app_file="$WWW_DIR/cgi-bin/dkb.fcgi"
  status_www &>/dev/null \
    && echo "Application is already running." >&2 \
    && exit 1
  [ ! -f "$app_file" ] \
    && echo "Failed to restart application: file not found ($app_file)." >&2 \
    && exit 1
  su "$APP_USER" -c \
    "nohup '$app_file' >> '$logfile' &
     echo \$! > '$pidfile'"
}

_clean() {
  rm -rf "$build_dir"
}

_build() {
  _clean
  mkdir -p "$build_dir"
  [ -n "$MANAGE_WWW" ] && build_www
  [ -n "$MANAGE_NGINX" ] && build_nginx_cfg
  [ -n "$MANAGE_SERVICE" ] && build_service_cfg
}

_install() {
  get_nginx_user \
  && ensure_user \
  && ensure_dirs || exit 1
  [ -n "$MANAGE_WWW" ] && install_www
  [ -n "$MANAGE_NGINX" ] && install_nginx_cfg
  [ -n "$MANAGE_SERVICE" ] && install_service
  [ -n "$MANAGE_SEL" ] && customize_sel
}

_uninstall() {
  [ -n "$MANAGE_WWW" ] && uninstall_www
  [ -n "$MANAGE_NGINX" ] && uninstall_nginx_cfg
  [ -n "$MANAGE_SERVICE" ] && uninstall_service
  [ -n "$MANAGE_SEL" ] && clean_sel
}

[ -z "$1" ] && usage >&2 && exit 1

[ -z "$MANAGE_WWW" ] \
  && [ -z "$MANAGE_NGINX" ] \
  && [ -z "$MANAGE_SERVICE" ] \
  && [ -z "$MANAGE_SEL" ] \
  && echo "Nothing to do." >&2 \
  && exit 1

while [ $# -gt 0 ]; do
  case "$1" in
    clean)
      _clean
      ;;
    build)
      _build
      ;;
    install)
      _install
      ;;
    uninstall)
      _uninstall
      ;;
    start)
      start_www
      ;;
    stop)
      stop_www
      ;;
    restart)
      stop_www
      start_www
      ;;
    status)
      status_www
      exit $?
      ;;
    *)
      echo "Unknown command: $1" >&2
      exit 1
      ;;
  esac
  shift
done
