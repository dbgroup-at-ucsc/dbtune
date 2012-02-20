--query
select
	s_suppkey,
	s_name,
	s_address,
	s_phone,
	total_revenue
from
	tpch.supplier,
	tpch.revenue999
where
	s_suppkey = supplier_no
	and total_revenue = (
		select
			max(total_revenue)
		from
			tpch.revenue999
	)
order by
	s_suppkey
