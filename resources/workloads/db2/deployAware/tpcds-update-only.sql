


-- insert new date record
INSERT INTO tpcds.date_dim
(
	D_DATE_SK,
	D_DATE_ID,
	D_DATE,
	D_MONTH_SEQ,
	D_WEEK_SEQ,
	D_QUARTER_SEQ,
	D_YEAR,
	D_DOW,
	D_MOY,
	D_DOM,
	D_QOY,
	D_FY_YEAR,
	D_FY_QUARTER_SEQ,
	D_FY_WEEK_SEQ,
	D_DAY_NAME,
	D_QUARTER_NAME,
	D_HOLIDAY,
	D_WEEKEND,
	D_FOLLOWING_HOLIDAY,
	D_FIRST_DOM,
	D_LAST_DOM,
	D_SAME_DAY_LY,
	D_SAME_DAY_LQ,
	D_CURRENT_DAY,
	D_CURRENT_WEEK,
	D_CURRENT_MONTH,
	D_CURRENT_QUARTER,
	D_CURRENT_YEAR
) values (
	2500001,
	'AAAAAAAAOKJNECAB',
	'2012-12-21',
	0,
	1,
	1,
	2012,
	1,
	12,
	21,
	1,
	2012,
	1,
	1,
	'Monday   ',
	'2012Q4',
	'N',
	'N',
	'Y',
	2415021,
	2415020,
	2414657,
	2414930,
	'N',
	'N',
	'N',
	'N',
	'N'
);

