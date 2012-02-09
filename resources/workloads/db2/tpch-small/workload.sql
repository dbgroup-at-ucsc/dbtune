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

--query 12
select
	l_shipmode,
	sum(case
		when o_orderpriority = '1-URGENT'
			or o_orderpriority = '2-HIGH'
			then 1
		else 0
	end) as high_line_count,
	sum(case
		when o_orderpriority <> '1-URGENT'
			and o_orderpriority <> '2-HIGH'
			then 1
		else 0
	end) as low_line_count
from
	tpch.orders,
	tpch.lineitem
where
	o_orderkey = l_orderkey
	and l_shipmode in ('FOB', 'REG AIR')
	and l_commitdate < l_receiptdate
	and l_shipdate < l_commitdate
	and l_receiptdate >= '1993-01-01'
	and l_receiptdate < cast('1994-01-01' as date) 
group by
	l_shipmode
order by
	l_shipmode;


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

