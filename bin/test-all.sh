#!/bin/bash
#
# runs the tests under several configurations
#
# it must be run from the dbtune root folder

DB2_CONFIG="jdbc.url=jdbc:db2://128.114.59.9:50000/test\n\
workloads.dir=resources/test-workloads/db2/\n\
username=db2inst1\n\
password=db2inst1admin\n"

MYSQL_CONFIG="jdbc.url=jdbc:mysql://128.114.59.9:3306/\n\
workloads.dir=resources/test-workloads/mysql/\n\
username=dbtune\n\
password=dbtuneadmin\n"

POSTGRES_CONFIG="jdbc.url=jdbc:postgresql://128.114.59.9:5432/\n\
workloads.dir=resources/test-workloads/postgres/\n\
username=dbtune\n\
password=dbtuneadmin\n"

OPT_CONFIG="optimizer=dbms"
IBG_CONFIG="optimizer=inum"
INUM_CONFIG="optimizer=ibg"
IBG_ON_INUM_CONFIG="optimizer=inum,ibg"
INUM_ON_IBG_CONFIG="optimizer=ibg,inum"

export DBTUNECONFIG=`pwd`/config/dbtune-test.cfg

ant -lib lib clean
ant -lib lib compile.test.all
ant -lib lib unit.all

echo "started at: " `date`

# DB2
cat bin/base.cfg > $DBTUNECONFIG
echo -e $DB2_CONFIG >> $DBTUNECONFIG
echo -e $OPT_CONFIG >> $DBTUNECONFIG
ant -lib lib functional.all

ANTRETURNCODE=$?
 
if [ $ANTRETURNCODE -ne 0 ];then
    echo "BUILD ERROR"
    exit 1;
fi

cat bin/base.cfg > $DBTUNECONFIG
echo -e $DB2_CONFIG >> $DBTUNECONFIG
echo -e $IBG_CONFIG >> $DBTUNECONFIG
ant -lib lib functional.all

ANTRETURNCODE=$?
 
if [ $ANTRETURNCODE -ne 0 ];then
    echo "BUILD ERROR"
    exit 1;
fi

cat bin/base.cfg > $DBTUNECONFIG
echo -e $DB2_CONFIG >> $DBTUNECONFIG
echo -e $INUM_CONFIG >> $DBTUNECONFIG
ant -lib lib functional.all

ANTRETURNCODE=$?

if [ $ANTRETURNCODE -ne 0 ];then
    echo "BUILD ERROR"
    exit 1;
fi

cat bin/base.cfg > $DBTUNECONFIG
echo -e $DB2_CONFIG >> $DBTUNECONFIG
echo -e $IBG_ON_INUM_CONFIG >> $DBTUNECONFIG
ant -lib lib functional.all

ANTRETURNCODE=$?

if [ $ANTRETURNCODE -ne 0 ];then
    echo "BUILD ERROR"
    exit 1;
fi

# POSTGRES

# MYSQL

echo "finished at: " `date`
