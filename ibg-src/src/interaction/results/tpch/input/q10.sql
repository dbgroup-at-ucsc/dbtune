--query
select
	c_custkey,
	c_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	c_acctbal,
	n_name,
	c_address,
	c_phone,
	c_comment
from
	tpch.customer,
	tpch.orders,
	tpch.lineitem,
	tpch.nation
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate >= cast('1993-11-01' as date)
	and o_orderdate < cast('1993-11-01' as date) + 3 months
	and l_returnflag = 'R'
	and c_nationkey = n_nationkey
group by
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
order by
	revenue desc
