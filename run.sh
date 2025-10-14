#!/bin/bash

ulimit -c unlimited

while [ true ]
do
	./test/alternator/run test_streams.py >> /tmp/log.txt
	V="$?"
	echo "Exited with $V" >> /tmp/log.txt
	if [[ $V != "0" ]]
	then
		exit
	fi
done
