#!/bin/sh

killall postgres

dir="$1/test_db"
rm -rf $dir
mkdir $dir
if [ $? -ne 0 ]; then
	echo "Directory $dir already exists"
	exit 1
fi

initdb $dir
if [ $? -ne 0 ]; then
	echo "Can't initdb"
	exit 1
fi

postgres -D $dir &

# Wait for postgres to spin up
sleep 5

createuser -sdr Administrator
if [ $? -ne 0 ]; then
	echo "Can't create user"
	killall postgres
	exit 1
fi

