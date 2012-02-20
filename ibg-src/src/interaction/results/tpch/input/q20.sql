--query
select
	s_name,
	s_address
from
	tpch.supplier,
	tpch.nation
where
	s_suppkey in (
		select
			ps_suppkey
		from
			tpch.partsupp
		where
			ps_partkey in (
				select
					p_partkey
				from
					tpch.part
				where
					p_name like 'ivory%'
			)
			and ps_availqty > (
				select
					0.5 * sum(l_quantity)
				from
					tpch.lineitem
				where
					l_partkey = ps_partkey
					and l_suppkey = ps_suppkey
					and l_shipdate >= '1996-01-01'
					and l_shipdate < cast('1996-01-01' as date) + 1 year
			)
	)
	and s_nationkey = n_nationkey
	and n_name = 'KENYA'
order by
	s_name
