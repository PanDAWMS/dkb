#!/bin/sh
curl -XDELETE "http://localhost:9200/${1}?pretty"
