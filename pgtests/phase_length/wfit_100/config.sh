#!/bin/sh

# set required variables
WORKLOAD_FILE=${TOP_DIR}/workloads/workload_SIMPLE_MULTICOLUMN_QUERIES_p100_w2_1234.sql
ALGORITHM=wfit

# wfit-specific variables
HOT=40
STATES=2000
