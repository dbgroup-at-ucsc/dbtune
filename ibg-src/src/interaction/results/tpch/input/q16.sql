--query
select
	p_brand,
	p_type,
	p_size,
	count(distinct ps_suppkey) as supplier_cnt
from
	tpch.partsupp,
	tpch.part
where
	p_partkey = ps_partkey
	and p_brand <> 'Brand#41'
	and p_type not like 'MEDIUM BURNISHED%'
	and p_size in (4, 21, 15, 41, 49, 43, 27, 47)
	and ps_suppkey not in (
		select
			s_suppkey
		from
			tpch.supplier
		where
			s_comment like '%Customer%Complaints%'
	)
group by
	p_brand,
	p_type,
	p_size
order by
	supplier_cnt desc,
	p_brand,
	p_type,
	p_size
