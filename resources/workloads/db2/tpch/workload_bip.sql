-- query
select

	p_brand,
	p_size,
	p_type,
	p_name, 
	ps_supplycost
from
	tpch.partsupp,
	tpch.part
where
	p_brand <> 'Brand#41'
	and p_type not like 'MEDIUM BURNISHED%'
	and p_size in (4, 21, 15, 41, 49, 43, 27, 47)
	and p_partkey = ps_partkey ;


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

