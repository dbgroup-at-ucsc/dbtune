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



