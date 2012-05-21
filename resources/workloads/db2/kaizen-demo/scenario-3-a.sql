SELECT col1 FROM one_table.tbl WHERE col1 = 2;
SELECT col1 FROM one_table.tbl WHERE col1 = 2;
SELECT col1 FROM one_table.tbl WHERE col2 = 2;
SELECT col1 FROM one_table.tbl WHERE col2 = 2;
-- vote (col1,col2) down so it doesn't affect the updates

UPDATE one_table.tbl set col1 = col1+1 WHERE col1 BETWEEN       0 AND  500000;
UPDATE one_table.tbl set col1 = col1+2 WHERE col1 BETWEEN  500000 AND  900000;
UPDATE one_table.tbl set col1 = col1+3 WHERE col1 BETWEEN  900000 AND 1100000;
UPDATE one_table.tbl set col1 = col1+4 WHERE col1 BETWEEN 1100000 AND 1500000;
UPDATE one_table.tbl set col1 = col1+5 WHERE col1 BETWEEN 1500000 AND 2000000;
UPDATE one_table.tbl set col1 = col1+5 WHERE col1 BETWEEN 2000000 AND 2500000;
