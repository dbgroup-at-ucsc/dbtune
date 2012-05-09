SELECT a FROM one_table.tbl WHERE a = 2;
SELECT a FROM one_table.tbl WHERE a = 2;

UPDATE one_table.tbl set a = a+1 WHERE a BETWEEN        0 AND  5000000;
UPDATE one_table.tbl set a = a+2 WHERE a BETWEEN  5000000 AND  9000000;
UPDATE one_table.tbl set a = a+3 WHERE a BETWEEN  9000000 AND 11000000;
UPDATE one_table.tbl set a = a+4 WHERE a BETWEEN 11000000 AND 15000000;

SELECT a FROM one_table.tbl WHERE a = 2;
SELECT a FROM one_table.tbl WHERE b = 2;
SELECT a FROM one_table.tbl WHERE b = 2;
SELECT a FROM one_table.tbl WHERE a = 2;

