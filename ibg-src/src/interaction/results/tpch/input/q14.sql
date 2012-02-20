--query
select
	100.00 * sum(case
		when p_type like 'PROMO%'
			then l_extendedprice * (1 - l_discount)
		else 0
	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
	tpch.lineitem,
	tpch.part
where
	l_partkey = p_partkey
	and l_shipdate >= '1993-05-01'
	and l_shipdate < cast('1993-05-01' as date) + 1 month
