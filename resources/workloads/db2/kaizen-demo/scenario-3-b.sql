SELECT col1 FROM one_table.tbl WHERE col1 = 2;
SELECT col1 FROM one_table.tbl WHERE col1 = 2;

-- vote col1 down (index gets dropped)
SELECT col1 FROM one_table.tbl WHERE col1 = 2;
SELECT col1 FROM one_table.tbl WHERE col1 = 2;

-- col1 gets restored

SELECT col1 FROM one_table.tbl WHERE col1 = 2;
