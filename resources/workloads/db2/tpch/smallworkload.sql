--query
select
	ps_suppkey,
         s_suppkey 
from
	tpch.partsupp,
	tpch.supplier
where
	ps_suppkey = s_suppkey AND ps_suppkey < 10 AND s_suppkey < 10

ORDER BY
        ps_suppkey ,
        s_suppkey ;

