-- phase 1
SELECT a FROM one_table.tbl WHERE a = 2;
SELECT a FROM one_table.tbl WHERE a = 2;

-- phase 2
UPDATE one_table.tbl set a = a+1 WHERE a BETWEEN        0 AND   500000;
UPDATE one_table.tbl set a = a+2 WHERE a BETWEEN   500000 AND   900000;
UPDATE one_table.tbl set a = a+3 WHERE a BETWEEN   900000 AND  1100000;
UPDATE one_table.tbl set a = a+4 WHERE a BETWEEN 11000000 AND 15000000;

-- phase 3
SELECT a FROM one_table.tbl WHERE a = 2;
SELECT a FROM one_table.tbl WHERE b = 2;
SELECT a FROM one_table.tbl WHERE b = 2;
SELECT a FROM one_table.tbl WHERE a = 2;
