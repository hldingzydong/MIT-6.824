#!/bin/sh
cd /Users/maxie/MIT-6.824/Lab/src/kvraft
clear
go build
if [ "$?" == 0 ]; then
	COUNT=0
	while [ ${COUNT} -le 19 ]; do
		echo "-------------------  run test in cycle ${COUNT}  -------------------"
		go test -run $1
		let COUNT++
	done
fi
