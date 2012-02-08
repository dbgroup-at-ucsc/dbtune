--====================================
--query 00
-- not actually part of the benchmark
select
	ps_suppkey,
        s_suppkey, s_acctbal
from
	tpch.partsupp,
	tpch.supplier
where
	ps_suppkey = s_suppkey AND ps_suppkey < 10 AND s_suppkey < 10

ORDER BY
        ps_suppkey,
        s_suppkey ;
--====================================


--query 01
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
--query 02
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

--query 03
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

--query 04
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


--query 06
select
	sum(l_extendedprice * l_discount) as revenue
from
	tpch.lineitem
where
	l_shipdate >= '1993-01-01'
	and l_shipdate < cast('1994-01-01' as date) 
	and l_discount between 0.06 and 0.08 
	and l_quantity < 25;

--query 07
-- contain two instances of nation relation
--select
--	supp_nation,
--	cust_nation,
--	l_year,
--	sum(volume) as revenue
--from
--	(
--		select
--			n1.n_name as supp_nation,
--			n2.n_name as cust_nation,
--			year(l_shipdate) as l_year,
--			l_extendedprice * (1 - l_discount) as volume
--		from
--			tpch.supplier,
--			tpch.lineitem,
--			tpch.orders,
--			tpch.customer,
--			tpch.nation n1,
--			tpch.nation n2
--		where
--			s_suppkey = l_suppkey
--			and o_orderkey = l_orderkey
--			and c_custkey = o_custkey
--			and s_nationkey = n1.n_nationkey
--			and c_nationkey = n2.n_nationkey
--			and (
--				(n1.n_name = 'MOZAMBIQUE' and n2.n_name = 'UNITED KINGDOM')
--				or (n1.n_name = 'UNITED KINGDOM' and n2.n_name = 'MOZAMBIQUE')
--			)
--			and l_shipdate between '1995-01-01' and '1996-12-31'
--	) as shipping
--group by
--	supp_nation,
--	cust_nation,
--	l_year
--order by
--	supp_nation,
--	cust_nation,
--	l_year;

--query 08
-- contains two instances of nation
--select
--	o_year,
--	sum(case
--		when nation = 'MOZAMBIQUE' then volume
--		else 0
--	end) / sum(volume) as mkt_share
--from
--	(
--		select
--			year(o_orderdate) as o_year,
--			l_extendedprice * (1 - l_discount) as volume,
--			n2.n_name as nation
--		from
--			tpch.part,
--			tpch.supplier,
--			tpch.lineitem,
--			tpch.orders,
--			tpch.customer,
--			tpch.nation n1,
--			tpch.nation n2,
--			tpch.region
--		where
--			p_partkey = l_partkey
--			and s_suppkey = l_suppkey
--			and l_orderkey = o_orderkey
--			and o_custkey = c_custkey
--			and c_nationkey = n1.n_nationkey
--			and n1.n_regionkey = r_regionkey
--			and r_name = 'AFRICA'
--			and s_nationkey = n2.n_nationkey
--			and o_orderdate between '1995-01-01' and '1996-12-31'
--			and p_type = 'PROMO POLISHED TIN'
--	) as all_nations
--group by
--	o_year
--order by
--	o_year;

--query 09
select
	nation,
	o_year,
	sum(amount) as sum_profit
from
	(
		select
			n_name as nation,
			year(o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
		from
			tpch.part,
			tpch.supplier,
			tpch.lineitem,
			tpch.partsupp,
			tpch.orders,
			tpch.nation
		where
			s_suppkey = l_suppkey
			and ps_suppkey = l_suppkey
			and ps_partkey = l_partkey
			and p_partkey = l_partkey
			and o_orderkey = l_orderkey
			and s_nationkey = n_nationkey
			and p_name like '%thistle%'
	) as profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc;

--query 14
select
	100.00 * sum(case
		when p_type like 'PROMO%'
			then l_extendedprice * (1 - l_discount)
		else 0
	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
	tpch.lineitem,
	tpch.part
where
	l_partkey = p_partkey
	and l_shipdate >= '1993-05-01'
	and l_shipdate < cast('1993-06-01' as date);

--query 15
--select
--	s_suppkey,
--	s_name,
--	s_address,
--	s_phone,
--	total_revenue
--from
--	tpch.supplier,
--	tpch.revenue999
--where
--	s_suppkey = supplier_no
--	and total_revenue = (
--		select
--			max(total_revenue)
--		from
--			tpch.revenue999
--	)
--order by
--	s_suppkey;

