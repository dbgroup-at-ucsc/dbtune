--query
select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
from
	tpch.lineitem
where
	l_shipdate <= cast('1998-09-22' as date) 
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus;
--query
--select
--	s_acctbal,
--	s_name,
--	n_name,
--	p_partkey,
--	p_mfgr,
--	s_address,
--	s_phone,
--	s_comment
--from
--	tpch.part,
--	tpch.supplier,
--	tpch.partsupp,
--	tpch.nation,
--	tpch.region
--where
--	p_partkey = ps_partkey
--	and s_suppkey = ps_suppkey
--	and p_size = 38
--	and p_type like '%STEEL'
--	and s_nationkey = n_nationkey
--	and n_regionkey = r_regionkey
--	and r_name = 'ASIA'
--	and ps_supplycost = (
--		select
--			min(ps_supplycost)
--		from
--			tpch.partsupp,
--			tpch.supplier,
--			tpch.nation,
--			tpch.region
--		where
--			p_partkey = ps_partkey
--			and s_suppkey = ps_suppkey
--			and s_nationkey = n_nationkey
--			and n_regionkey = r_regionkey
--			and r_name = 'ASIA'
--	)
--order by
--	s_acctbal desc,
--	n_name,
--	s_name,
--	p_partkey;
--query
select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
	tpch.customer,
	tpch.orders,
	tpch.lineitem
where
	c_mktsegment = 'FURNITURE'
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate < '1995-03-17'
	and l_shipdate > '1995-03-17'
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate;
--query
select
	o_orderpriority,
	count(*) as order_count
from
	tpch.orders
where
	o_orderdate >= '1995-08-01'
	and o_orderdate < cast('1995-11-01' as date)
	and exists (
		select
			*
		from
			tpch.lineitem
		where
			l_orderkey = o_orderkey
			and l_commitdate < l_receiptdate
	)
group by
	o_orderpriority
order by
	o_orderpriority;
--query
select
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	tpch.customer,
	tpch.orders,
	tpch.lineitem,
	tpch.supplier,
	tpch.nation,
	tpch.region
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and l_suppkey = s_suppkey
	and c_nationkey = s_nationkey
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'AMERICA'
	and o_orderdate >= '1993-01-01'
	and o_orderdate < cast('1994-01-01' as date)
group by
	n_name
order by
	revenue desc;
