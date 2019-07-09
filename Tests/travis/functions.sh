#!/bin/sh
getChangedFiles() {
  git diff --name-only HEAD^
}

getAddedLines() {
  file="$1"

  git diff HEAD^ -- "$file" | grep -v "^+++ b/$file" | grep -e '^+' | sed -e 's/^+//'
}

getLinesFromFile() {
  file="$1"
  lines="$2"

  echo "$lines" | sort -u | while IFS= read -r l; do
      grep -n "^$l\$" -- "$file"
  done | sort -n
}

getURL() {
  grep -oP '[a-zA-Z]+://[^/?& ]+(/[^&? ]*(\?[^& ]+(&[^& ]+)*)?)?'
}
