--query
select
	ps_suppkey 
from
	tpch.partsupp,
	tpch.supplier
where
	ps_suppkey = s_suppkey AND ps_suppkey < 10 AND s_suppkey < 10 ; 

