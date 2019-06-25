#!/bin/bash
. $(dirname $0)/../functions.sh

function doTest() {
  if [ "$1" != "$2" ];then
    echo "$1 dont match expected $2"
    exit 1
  fi
}

doTest "$(echo "http://example.com"                       | getURL)" "http://example.com"
doTest "$(echo "http://example.com?k=v"                   | getURL)" "http://example.com"
doTest "$(echo "http://example.com/"                      | getURL)" "http://example.com/"
doTest "$(echo "http://example.com/?k=v"                  | getURL)" "http://example.com/?k=v"
doTest "$(echo "http://example.com/q"                     | getURL)" "http://example.com/q"
doTest "$(echo "http://example.com/q&"                    | getURL)" "http://example.com/q"
doTest "$(echo "http://example.com/q?"                    | getURL)" "http://example.com/q"
doTest "$(echo "http://example.com/q?k=v"                 | getURL)" "http://example.com/q?k=v"
doTest "$(echo "http://example.com/q?k=v&"                | getURL)" "http://example.com/q?k=v"
doTest "$(echo "http://example.com/q?k=v&k2=v2"           | getURL)" "http://example.com/q?k=v&k2=v2"
doTest "$(echo "http://userid@example.com"                | getURL)" "http://userid@example.com"
doTest "$(echo "http://userid:password@example.com:8080/" | getURL)" "http://userid:password@example.com:8080/"
exit 0
