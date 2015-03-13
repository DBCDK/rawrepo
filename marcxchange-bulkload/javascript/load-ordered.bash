#!/bin/bash

declare args=1
declare files=2
declare -i i=2
while [ $i -lt "$#" ]; do
	if [ "${@:$i:1}" = '--' ]; then
		args=$i-1
		files=$i+1
		break
	fi
	i+=1
done

#echo args "${@:1:$args}"
#echo files "${@:$files}"
#exit 1

mkdir -p logs
for file in "${@:$files}"; do
    (time jsinputtool -f xml -l logs/"${file/*\//}".log -F "${0%/*}"/load.js "${@:1:$args}" <"$file")
done
