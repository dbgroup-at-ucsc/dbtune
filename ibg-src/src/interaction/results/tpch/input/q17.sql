--query
select
	sum(l_extendedprice) / 7.0 as avg_yearly
from
	tpch.lineitem,
	tpch.part
where
	p_partkey = l_partkey
	and p_brand = 'Brand#12'
	and p_container = 'SM BAG'
	and l_quantity < (
		select
			0.2 * avg(l_quantity)
		from
			tpch.lineitem
		where
			l_partkey = p_partkey
	)
