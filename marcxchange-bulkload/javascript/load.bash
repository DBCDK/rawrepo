#!/bin/bash

DB="$1"
shift

SCRIPT_BASE="${0%/*}"


mkdir -p logs

for file in "$@"; do
    echo "${file/*\//}"
    (time jsinputtool -p plugin:/home/bogeskov/jssql/src -f xml -F "$SCRIPT_BASE"/records.js "$DB" <"$file") 2>&1 | tee logs/"${file/*\//}".records-log
done

for file in "$@"; do
    echo "${file/*\//}"
    (time jsinputtool -f xml -F "$SCRIPT_BASE"/relations.js "$DB" <"$file") 2>&1 | tee logs/"${file/*\//}".relations-log
done
