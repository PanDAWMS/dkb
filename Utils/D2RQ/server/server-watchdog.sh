#!/bin/bash
until ./d2r-server mapping.ttl; do
    echo "Server 'myserver' crashed with exit code $?.  Respawning.." >&2
    sleep 30
done