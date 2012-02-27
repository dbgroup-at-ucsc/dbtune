select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<2;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<500000;
select count(L_ORDERKEY) from tpch.lineitem where l_orderkey<1000000;
select L_ORDERKEY from tpch.lineitem where l_orderkey<1000000;
select L_ORDERKEY from tpch.lineitem where l_orderkey<2000000;
select L_ORDERKEY from tpch.lineitem where l_orderkey<3000000;
select L_ORDERKEY from tpch.lineitem where l_orderkey<4000000;
select L_ORDERKEY from tpch.lineitem where l_orderkey<5000000;
exit;
update tpch.lineitem set l_orderkey=l_orderkey+1 where l_orderkey>0;
update tpch.lineitem set l_orderkey=l_orderkey+1 where l_orderkey>100000;
update tpch.lineitem set l_orderkey=l_orderkey+1 where l_orderkey>5000000;
update tpch.lineitem set l_orderkey=l_orderkey+1 where l_orderkey<10;

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
	and o_orderdate < cast('1994-2-01' as date)
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
	revenue desc;
	
exit;
[ category=SELECT text="SELECT O_SHIPPRIORITY, O_ORDERDATE, O_ORDERKEY, O_CUSTKEY  FROM TPCH.ORDERS WHERE (O_ORDERDATE < '1995-03-17') "]
select count(*) from tpch.lineitem where l_orderkey<2;
select count(*) from tpch.lineitem where l_orderkey<500000;
select count(*) from tpch.lineitem where l_orderkey<1000000;
select * from tpch.lineitem where l_orderkey<1000000;
select * from tpch.lineitem where l_orderkey<2000000;
select * from tpch.lineitem where l_orderkey<3000000;
select * from tpch.lineitem where l_orderkey<4000000;
select * from tpch.lineitem where l_orderkey<5000000;
create index lineitem on tpch.lineitem (l_orderkey);
drop index lineitem on tpch.lineitem (l_orderkey);
update tpch.lineitem set l_orderkey=l_orderkey+1 where l_orderkey>0;
update tpch.lineitem set l_orderkey=l_orderkey+1 where l_orderkey>100000;
update tpch.lineitem set l_orderkey=l_orderkey+1 where l_orderkey>5000000;
update tpch.lineitem set l_orderkey=l_orderkey+1 where l_orderkey<10;
