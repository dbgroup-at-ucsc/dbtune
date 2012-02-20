--query
select
	sum(l_extendedprice * l_discount) as revenue
from
	tpch.lineitem
where
	l_shipdate >= '1993-01-01'
	and l_shipdate < cast('1993-01-01' as date) + 1 year
	and l_discount between 0.07 - 0.01 and 0.07 + 0.01
	and l_quantity < 25
