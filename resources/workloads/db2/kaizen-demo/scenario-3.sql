-- ########################
-- GOOD negative vote
-- ########################
SELECT a FROM one_table.tbl WHERE a = 2;
SELECT a FROM one_table.tbl WHERE a = 2;
-- vote a down
UPDATE one_table.tbl set a = a+1 WHERE a BETWEEN        0 AND  500000;
UPDATE one_table.tbl set a = a+2 WHERE a BETWEEN  500000 AND  900000;
UPDATE one_table.tbl set a = a+3 WHERE a BETWEEN  9000000 AND 11000000;
UPDATE one_table.tbl set a = a+4 WHERE a BETWEEN 11000000 AND 15000000;

-- ########################
-- GOOD positive vote
-- ########################
SELECT a FROM one_table.tbl WHERE a = 2;
SELECT a FROM one_table.tbl WHERE a = 2;
UPDATE one_table.tbl set a = a+1 WHERE a BETWEEN        0 AND  500000;
UPDATE one_table.tbl set a = a+2 WHERE a BETWEEN  500000 AND  900000;
UPDATE one_table.tbl set a = a+3 WHERE a BETWEEN  9000000 AND 11000000;
-- vote A up so that it doesn't get dropped
UPDATE one_table.tbl set a = a+4 WHERE a BETWEEN 11000000 AND 15000000;
SELECT a FROM one_table.tbl WHERE a = 2;
SELECT a FROM one_table.tbl WHERE a = 2;
SELECT a FROM one_table.tbl WHERE a = 2;
SELECT a FROM one_table.tbl WHERE a = 2;
SELECT a FROM one_table.tbl WHERE a = 2;
SELECT a FROM one_table.tbl WHERE a = 2;
SELECT a FROM one_table.tbl WHERE a = 2;
SELECT a FROM one_table.tbl WHERE a = 2;
SELECT a FROM one_table.tbl WHERE a = 2;
SELECT a FROM one_table.tbl WHERE a = 2;

-- ########################
-- Recovery from BAD advice
-- ########################
SELECT a FROM one_table.tbl WHERE a = 2;
SELECT a FROM one_table.tbl WHERE a = 2;

-- vote a down
SELECT a FROM one_table.tbl WHERE a = 2; -- A gets dropped at this step

SELECT a FROM one_table.tbl WHERE a = 2;
SELECT a FROM one_table.tbl WHERE a = 2;
