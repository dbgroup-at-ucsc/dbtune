#!/bin/sh

TOP_DIR=${HOME}/school/benchmark
SCRIPT_DIR=${TOP_DIR}/scripts
PG_DIR=${HOME}/school/pg/dev

set -x

if  [ $# -ne 1 ]; then
	echo "Usage: $0 [results directory]"
	exit 2
fi

RESULT_DIR=$1

# get the configuration info
. ${RESULT_DIR}/config.sh

echo $WORKLOAD_FILE

# start the database, clearing old indices first
pg_ctl stop
pg_ctl start -l /Users/karlsch/School/pg/dev/logfile -o "--selftuning_enable=on --explain_only_mode=off"
sleep 5
echo | psql benchmark
pg_ctl stop

rm $PG_DIR/logfile

if [ $ALGORITHM == 'normal' ]; then
	pg_ctl start -l /Users/karlsch/School/pg/dev/logfile -o "--selftuning_enable=off --explain_only_mode=on"
elif [ $ALGORITHM == 'colt' ]; then
	pg_ctl start -l /Users/karlsch/School/pg/dev/logfile -o "--selftuning_enable=on --selftuning_method=colt --explain_only_mode=on --selftuning_wait_for_index=on"
elif [ $ALGORITHM == 'bc' ]; then
	pg_ctl start -l /Users/karlsch/School/pg/dev/logfile -o "--selftuning_enable=on --selftuning_method=bc --explain_only_mode=on --selftuning_wait_for_index=on"
else
	echo "Unknown algorithm: " $ALGORITHM
	exit 2
fi

sleep 5
psql benchmark < $WORKLOAD_FILE
cp $PG_DIR/logfile $RESULT_DIR/log.txt
cp ${PG_DIR}/data/postgresql.conf $RESULT_DIR/postgresql.conf 

