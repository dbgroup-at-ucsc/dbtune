-- query
select
				sum(ps_supplycost * ps_availqty) * 0.0001000000
			from
				tpch.partsupp,
				tpch.supplier,
				tpch.nation
			where
				ps_suppkey = s_suppkey
				and s_nationkey = n_nationkey
				and n_name = 'JAPAN';
