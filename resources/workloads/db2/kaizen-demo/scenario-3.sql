SELECT col1 FROM one_table.tbl WHERE col2 = 2;
SELECT col1 FROM one_table.tbl WHERE col2 = 2;
SELECT col1 FROM one_table.tbl WHERE col1 = 2;
SELECT col1 FROM one_table.tbl WHERE col1 = 2;
-- vote (col1,col2) down so it doesn't affect the updates

UPDATE one_table.tbl set col1 = col1+1 WHERE col1 BETWEEN        0 AND   500000;
UPDATE one_table.tbl set col1 = col1+2 WHERE col1 BETWEEN   500000 AND   900000;
UPDATE one_table.tbl set col1 = col1+3 WHERE col1 BETWEEN  9000000 AND 11000000;

SELECT col1 FROM one_table.tbl WHERE col1 = 2;
SELECT col1 FROM one_table.tbl WHERE col1 = 2;

-- vote col1 down (index gets dropped)
SELECT col1 FROM one_table.tbl WHERE col1 = 2;
SELECT col1 FROM one_table.tbl WHERE col1 = 2;

-- col1 gets restored

SELECT col1 FROM one_table.tbl WHERE col1 = 2;
