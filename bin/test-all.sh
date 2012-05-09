#!/bin/bash
#
# runs the tests under several configurations
#
# it must be run from the dbtune root folder

DB2_CONFIG="jdbc.url=jdbc:db2://192.168.56.101:50000/test\n\
workloads.dir=resources/test-workloads/db2/\n\
username=db2inst1\n\
password=db2inst1admin\n"

MYSQL_CONFIG="jdbc.url=jdbc:mysql://aigaion.cse.ucsc.edu:3306/\n\
workloads.dir=resources/test-workloads/mysql/\n\
username=dbtune\n\
password=dbtuneadmin\n"

POSTGRES_CONFIG="jdbc.url=jdbc:postgresql://aigaion.cse.ucsc.edu:5432/\n\
workloads.dir=resources/test-workloads/postgres/\n\
username=dbtune\n\
password=dbtuneadmin\n"

DBMS_CONFIG="optimizer=dbms"
IBG_CONFIG="optimizer=ibg"
INUM_CONFIG="optimizer=inum"
IBG_ON_INUM_CONFIG="optimizer=inum,ibg"

export DBTUNECONFIG=`pwd`/config/dbtune-test.cfg

echo "testing began at: " `date`

###############
# UNIT
###############
ant -lib lib clean
ant -lib lib compile.test.all
echo "executing all unit tests"
ant -lib lib unit.all

###############
# DB2
###############
echo "executing $DB2_CONFIG and $DBMS_CONFIG"

cat bin/base.cfg > $DBTUNECONFIG
echo -e $DB2_CONFIG >> $DBTUNECONFIG
echo -e $DBMS_CONFIG >> $DBTUNECONFIG
ant -lib lib functional.all

if [ $? -ne 0 ];then
    echo "BUILD ERROR"
    exit 1;
fi

################
echo "executing $DB2_CONFIG and $INUM_CONFIG"

cat bin/base.cfg > $DBTUNECONFIG
echo -e $DB2_CONFIG >> $DBTUNECONFIG
echo -e $INUM_CONFIG >> $DBTUNECONFIG
ant -lib lib functional.all

if [ $? -ne 0 ];then
    echo "BUILD ERROR"
    exit 1;
fi

############### 
echo "executing $DB2_CONFIG and $IBG_CONFIG"

cat bin/base.cfg > $DBTUNECONFIG
echo -e $DB2_CONFIG >> $DBTUNECONFIG
echo -e $IBG_CONFIG >> $DBTUNECONFIG
ant -lib lib functional.all

if [ $? -ne 0 ];then
    echo "BUILD ERROR"
    exit 1;
fi

###############
echo "executing $DB2_CONFIG and $IBG_ON_INUM_CONFIG"

cat bin/base.cfg > $DBTUNECONFIG
echo -e $DB2_CONFIG >> $DBTUNECONFIG
echo -e $IBG_ON_INUM_CONFIG >> $DBTUNECONFIG
ant -lib lib functional.all

if [ $? -ne 0 ];then
    echo "BUILD ERROR"
    exit 1;
fi

# POSTGRES

# MYSQL

echo "finished at: " `date`
