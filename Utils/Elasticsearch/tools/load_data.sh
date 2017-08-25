#!/bin/sh

curl 'http://127.0.0.1:9200/_bulk?pretty' --data-binary @${1}
