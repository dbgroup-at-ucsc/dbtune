--query 02
select
  s_acctbal,
  s_name,
  n_name,
  p_partkey,
  p_mfgr,
  s_address,
  s_phone,
  s_comment
from
  tpch.part,
  tpch.supplier,
  tpch.partsupp,
  tpch.nation,
  tpch.region
where
  p_partkey = ps_partkey
  and s_suppkey = ps_suppkey
  and p_size = 38
  and p_type like '%STEEL'
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'ASIA'
  and ps_supplycost = (
      select
	  min(ps_supplycost)
      from
	  tpch.partsupp,
	  tpch.supplier,
	  tpch.nation,
	  tpch.region
      where
	  p_partkey = ps_partkey
	  and s_suppkey = ps_suppkey
	  and s_nationkey = n_nationkey
	  and n_regionkey = r_regionkey
	  and r_name = 'ASIA'
  )
order by
  s_acctbal desc,
  n_name,
  s_name,
  p_partkey;

--query 07
select
  supp_nation,
  cust_nation,
  l_year,
  sum(volume) as revenue
from
  (
      select
	  n1.n_name as supp_nation,
	  n2.n_name as cust_nation,
	  year(l_shipdate) as l_year,
	  l_extendedprice * (1 - l_discount) as volume
      from
	  tpch.supplier,
	  tpch.lineitem,
	  tpch.orders,
	  tpch.customer,
	  tpch.nation n1,
	  tpch.nation n2
      where
	  s_suppkey = l_suppkey
	  and o_orderkey = l_orderkey
	  and c_custkey = o_custkey
	  and s_nationkey = n1.n_nationkey
	  and c_nationkey = n2.n_nationkey
	  and (
	      (n1.n_name = 'MOZAMBIQUE' and n2.n_name = 'UNITED KINGDOM')
	      or (n1.n_name = 'UNITED KINGDOM' and n2.n_name = 'MOZAMBIQUE')
	  )
	  and l_shipdate between '1995-01-01' and '1996-12-31'
  ) as shipping
group by
  supp_nation,
  cust_nation,
  l_year
order by
  supp_nation,
  cust_nation,
  l_year;


--query 08
select
  o_year,
  sum(case
      when nation = 'MOZAMBIQUE' then volume
      else 0
  end) / sum(volume) as mkt_share
from
  (
      select
	  year(o_orderdate) as o_year,
	  l_extendedprice * (1 - l_discount) as volume,
	  n2.n_name as nation
      from
	  tpch.part,
	  tpch.supplier,
	  tpch.lineitem,
	  tpch.orders,
	  tpch.customer,
	  tpch.nation n1,
	  tpch.nation n2,
	  tpch.region
      where
	  p_partkey = l_partkey
	  and s_suppkey = l_suppkey
	  and l_orderkey = o_orderkey
	  and o_custkey = c_custkey
	  and c_nationkey = n1.n_nationkey
	  and n1.n_regionkey = r_regionkey
	  and r_name = 'AFRICA'
	  and s_nationkey = n2.n_nationkey
	  and o_orderdate between '1995-01-01' and '1996-12-31'
	  and p_type = 'PROMO POLISHED TIN'
  ) as all_nations
group by
  o_year
order by
  o_year;

--query 11
select
  ps_partkey,
  sum(ps_supplycost * ps_availqty) as value
from
  tpch.partsupp,
  tpch.supplier,
  tpch.nation
where
  ps_suppkey = s_suppkey
  and s_nationkey = n_nationkey
  and n_name = 'JAPAN'
group by
  ps_partkey having
      sum(ps_supplycost * ps_availqty) > (
	  select
	      sum(ps_supplycost * ps_availqty) * 0.0001000000
	  from
	      tpch.partsupp,
	      tpch.supplier,
	      tpch.nation
	  where
	      ps_suppkey = s_suppkey
	      and s_nationkey = n_nationkey
	      and n_name = 'JAPAN'
      )
order by
  value desc;

--query 15
select
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
from
    tpch.supplier,
	(
     select
	 l_suppkey,
	 sum(l_extendedprice * (1 - l_discount)) as total_revenue
     from
	 tpch.lineitem
     where
	 l_shipdate >= '1995-08-01'
	 and l_shipdate <  '1995-11-01'
	 group by
	     l_suppkey
	)
where
	s_suppkey = l_suppkey
    and total_revenue = (
	select
		      max(total_revenue)
		from (
	     select
		 l_suppkey,
		 sum(l_extendedprice * (1 - l_discount)) as total_revenue
	     from
		 tpch.lineitem
	     where
		 l_shipdate >= '1995-08-01'
		 and l_shipdate <  '1995-11-01'
	     group by
		 l_suppkey
	 ))
order by
    s_suppkey;

--query 16
select
    p_brand,
    p_type,
    p_size,
    count(distinct ps_suppkey) as supplier_cnt
from tpch.partsupp, tpch.part where
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
    p_size;

--query 17
select
  sum(l_extendedprice) / 7.0 as avg_yearly
from
  tpch.lineitem,
  tpch.part
where
  p_partkey = l_partkey
  and p_brand = 'Brand#12'
  and p_container = 'SM BAG'
  and l_quantity < (
      select
	  0.2 * avg(l_quantity)
      from
	  tpch.lineitem
      where
	  l_partkey = p_partkey
  );

--query 18
select
  c_name,
  c_custkey,
  o_orderkey,
  o_orderdate,
  o_totalprice,
  sum(l_quantity)
from
  tpch.customer,
  tpch.orders,
  tpch.lineitem
where
  o_orderkey in (
      select
	  l_orderkey
      from
	  tpch.lineitem
      group by
	  l_orderkey having
	      sum(l_quantity) > 313
  )
  and c_custkey = o_custkey
  and o_orderkey = l_orderkey
group by
  c_name,
  c_custkey,
  o_orderkey,
  o_orderdate,
  o_totalprice
order by
  o_totalprice desc,
  o_orderdate;

--query 21
select
  s_name,
  count(*) as numwait
from
  tpch.supplier,
  tpch.lineitem l1,
  tpch.orders,
  tpch.nation
where
  s_suppkey = l1.l_suppkey
  and o_orderkey = l1.l_orderkey
  and o_orderstatus = 'F'
  and l1.l_receiptdate > l1.l_commitdate
  and exists (
      select
	  *
      from
	  tpch.lineitem l2
      where
	  l2.l_orderkey = l1.l_orderkey
	  and l2.l_suppkey <> l1.l_suppkey
  )
  and not exists (
      select
	  *
      from
	  tpch.lineitem l3
      where
	  l3.l_orderkey = l1.l_orderkey
	  and l3.l_suppkey <> l1.l_suppkey
	  and l3.l_receiptdate > l3.l_commitdate
  )
  and s_nationkey = n_nationkey
  and n_name = 'PERU'
group by
  s_name
order by
  numwait desc,
  s_name;

--query 22
select
  cntrycode,
  count(*) as numcust,
  sum(c_acctbal) as totacctbal
from
  (
      select
	  substring(c_phone from 1 for 2 using codeunits16) as cntrycode,
	  c_acctbal
      from
	  tpch.customer
      where
	  substring(c_phone from 1 for 2 using codeunits16) in
	      ('24', '11', '14', '23', '31', '26', '10')
	  and c_acctbal > (
	      select
		  avg(c_acctbal)
	      from
		  tpch.customer
	      where
		  c_acctbal > 0.00
		  and substring(c_phone from 1 for 2 using codeunits16) in
		      ('24', '11', '14', '23', '31', '26', '10')
	  )
	  and not exists (
	      select
		  *
	      from
		  tpch.orders
	      where
		  o_custkey = c_custkey
	  )
  ) as custsale
group by
  cntrycode
order by
  cntrycode;
