--Query 1
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
        l_shipdate <=  '1998-12-01'
group by
        l_returnflag,
        l_linestatus
order by
        l_returnflag,
        l_linestatus;

--Query 2
-- TODO: relation appearing more than once
--select
        --s_acctbal,
        --s_name,
        --n_name,
        --p_partkey,
        --p_mfgr,
        --s_address,
        --s_phone,
        --s_comment
--from
        --tpch.partsupp,
        --tpch.part,
        --tpch.supplier,
        --tpch.nation,
        --tpch.region
--where
        --p_partkey = ps_partkey
        --and s_suppkey = ps_suppkey
        --and p_size = 44
        --and p_type like '%BRASS'
        --and s_nationkey = n_nationkey
        ----and n_regionkey = r_regionkey
        --and r_name = 'AFRICA'
        --and ps_supplycost = (
                --select
                        --min(ps_supplycost)
                --from
                        --tpch.partsupp,
                        --tpch.supplier,
                        --tpch.nation,
                        --tpch.region
                --where
                        --p_partkey = ps_partkey
                        --and s_suppkey = ps_suppkey
                        --and s_nationkey = n_nationkey
                        --and n_regionkey = r_regionkey
                        --and r_name = 'AFRICA'
        --)
--order by
        --s_acctbal desc,
        --n_name,
        --s_name,
        --p_partkey;

--Query 3
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
        c_mktsegment = 'MACHINERY'
        and c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate <  '1995-03-06'
        and l_shipdate >  '1995-03-06'
group by
        l_orderkey,
        o_orderdate,
        o_shippriority
order by
        sum(l_extendedprice * (1 - l_discount)) desc,
        o_orderdate;

--Query 4
select
        o_orderpriority,
        count(*) as order_count
from
        tpch.orders,
        tpch.lineitem
where
        o_orderdate <=  '1993-12-01'
        and o_orderdate >  '1993-3-01'
        and l_orderkey = o_orderkey
        and l_commitdate < l_receiptdate
group by
        o_orderpriority
order by
        o_orderpriority;

--Query 5
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
        and o_orderdate >=  '1995-01-01'
        and o_orderdate <  '1996-01-01'
group by
        n_name
order by
        sum(l_extendedprice * (1 - l_discount)) desc;


--Query 6
select
        sum(l_extendedprice * l_discount) as revenue
from
        tpch.lineitem
where
        l_shipdate >=  '1995-01-01'
        and l_shipdate <  '1996-01-01'
        and l_discount between 0.02 - 0.01 and 0.02 + 0.01
        and l_quantity < 25;

--Query 7
-- problem with additional indexes on lineitem
-- TODO: relation appearing more than once
-- select
--         n1.n_name,
--         n2.n_name,
--         datepart(year,l_shipdate),
--         sum(l_extendedprice * (1 - l_discount)) as revenue
-- from
--     supplier,
--     lineitem,
--     orders,
--     customer,
--     nation n1,
--     nation n2
-- where
--     s_suppkey = l_suppkey
--     and o_orderkey = l_orderkey
--     and c_custkey = o_custkey
--     and s_nationkey = n1.n_nationkey
--     and c_nationkey = n2.n_nationkey
--     and (
--         (n1.n_name = 'ALGERIA' and n2.n_name = 'RUSSIA')
--         or (n1.n_name = 'RUSSIA' and n2.n_name = 'ALGERIA')
--     )
--     and l_shipdate between  '1995-01-01' and  '1996-12-31'
-- group by
--         n1.n_name,
--         n2.n_name,
--         datepart(year,l_shipdate)
-- order by
--         n1.n_name,
--         n2.n_name,
--         datepart(year,l_shipdate);


--Query 8
-- TODO: relation appearing more than once
--select
--        datepart(year,o_orderdate),
--        sum(l_extendedprice * (1 - l_discount))
--from
--    part,
--    supplier,
--    lineitem,
--    orders,
--    customer,
--    nation n1,
--    nation n2,
--    region
--where
--    p_partkey = l_partkey
--    and s_suppkey = l_suppkey
--    and l_orderkey = o_orderkey
--    and o_custkey = c_custkey
--    and c_nationkey = n1.n_nationkey
--    and n1.n_regionkey = r_regionkey
--    and r_name = 'EUROPE'
--    and s_nationkey = n2.n_nationkey
--    and o_orderdate between  '1995-01-01' and  '1996-12-31'
--    and p_type = 'LARGE POLISHED BRASS'
--group by
--        datepart(year,o_orderdate)
--order by
--        datepart(year,o_orderdate);

-- query 09
select
        n_name,
        year(o_orderdate),
        sum(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) as sum_profit
from
    tpch.lineitem,
    tpch.partsupp,
    tpch.part,
    tpch.supplier,
    tpch.orders,
    tpch.nation
where
    s_suppkey = l_suppkey
    and ps_suppkey = l_suppkey
    and ps_partkey = l_partkey
    and p_partkey = l_partkey
    and o_orderkey = l_orderkey
    and s_nationkey = n_nationkey
    and p_name like '%navy%'
group by
        n_name,
        year(o_orderdate)
order by
        n_name,
        year(o_orderdate) desc;


--Query 10
-- result kindof affected by the clustered index on the customer.
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
        and o_orderdate >=  '1994-11-01'
        and o_orderdate <  '1995-02-01'
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
        sum(l_extendedprice * (1 - l_discount)) desc;
--#SET ROWS_FETCH 20


--Query 12
select
        l_shipmode,
        count(o_orderpriority)
from
        tpch.orders,
        tpch.lineitem
where
        o_orderkey = l_orderkey
        and l_shipmode in ('AIR', 'TRUCK')
        and l_commitdate < l_receiptdate
        and l_shipdate < l_commitdate
        and l_receiptdate >=  '1996-01-01'
        and l_receiptdate <  '1997-01-01'
group by
        l_shipmode
order by
        l_shipmode;



-- Q13 inner query ... 
select
    c_custkey,
    count(o_orderkey)
from
    tpch.customer,
    tpch.orders
where
    c_custkey = o_custkey
    and o_comment not like '%unusual%accounts%'
group by
    c_custkey;


--Query 14
-- p_type is the right index, cause it is implicitly a covering index ...
select
        sum(l_extendedprice * (1 - l_discount)) as promo_revenue,
        count(p_type)
from
        tpch.lineitem,
        tpch.part
where
        l_partkey = p_partkey
        and l_shipdate >=  '1996-09-01'
        and l_shipdate <  '1996-10-01';
--#SET ROWS_FETCH -1


--Query 16
-- TODO this plan contains many IXSCANs and TABLESCANs, not only 3; we need to figure out why
--select
        --p_brand,
        --p_type,
        --p_size,
        --count(distinct ps_suppkey) as supplier_cnt
--from
        --tpch.partsupp,
        --tpch.part,
        --tpch.supplier
--where
        --p_partkey = ps_partkey
        --and p_brand <> 'Brand#35'
        --and p_type not like 'ECONOMY PLATED%'
        --and p_size in (44, 17, 29, 43, 20, 19, 35, 45)
        --and ps_suppkey <> s_suppkey
        --and ps_suppkey = s_suppkey
        --and s_comment not like '%Customer%Complaints%'
--
--group by
        --p_brand,
        --p_type,
        --p_size
--order by
        --count(distinct ps_suppkey) desc,
        --p_brand,
        --p_type,
        --p_size;



--Query 17
-- TODO: relation appearing more than once
--select
        --sum(l_extendedprice) as avg_yearly
--from
        --tpch.lineitem,
        --tpch.part
--where
        --p_partkey = l_partkey
        --and p_brand = 'Brand#54'
        --and p_container = 'SM CAN'
        --and l_quantity < (
                --select
                        --0.2 * avg(l_quantity)
                --from
                        --tpch.lineitem
                --where
                        --l_partkey = p_partkey
        --);
--#SET ROWS_FETCH -1


--Query 18
-- TODO: more than one reference to lineitem
--select
        --c_name,
        --c_custkey,
        --o_orderkey,
        --o_orderdate,
        --o_totalprice,
        --sum(l_quantity)
--from
        --tpch.customer,
        --tpch.orders,
        --tpch.lineitem
--where
        --o_orderkey in (
                --select
                        --l_orderkey
                --from
                        --tpch.lineitem
                --group by
                        --l_orderkey having
                                --sum(l_quantity) > 314
        --)
        --and c_custkey = o_custkey
        --and o_orderkey = l_orderkey
--group by
        --c_name,
        --c_custkey,
        --o_orderkey,
        --o_orderdate,
        --o_totalprice
--order by
        --o_totalprice desc,
        --o_orderdate;


--Query 19
select
        sum(l_extendedprice* (1 - l_discount)) as revenue
from
        tpch.lineitem,
        tpch.part
where
        (
                p_partkey = l_partkey
                and p_brand = 'Brand#11'
                and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                and l_quantity >= 7 and l_quantity <= 7 + 10
                and p_size between 1 and 5
                and l_shipmode in ('AIR', 'AIR REG')
                and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
                p_partkey = l_partkey
                and p_brand = 'Brand#35'
                and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                and l_quantity >= 12 and l_quantity <= 12 + 10
                and p_size between 1 and 10
                and l_shipmode in ('AIR', 'AIR REG')
                and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
                p_partkey = l_partkey
                and p_brand = 'Brand#22'
                and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                and l_quantity >= 27 and l_quantity <= 27 + 10
                and p_size between 1 and 15
                and l_shipmode in ('AIR', 'AIR REG')
                and l_shipinstruct = 'DELIVER IN PERSON'
        );





 DELETE FROM TPCH.ORDERS WHERE O_ORDERKEY = 197;
 
  DELETE FROM TPCH.LINEITEM WHERE L_ORDERKEY = 197;
 


 INSERT INTO tpch.lineitem VALUES(9,127857,5394,1,45,84818.25,0.09,0.05,'N','O','1998-10-20','1998-09-10','1998-11-15','COLLECT COD','SHIP','es haggle blithely above the silent ac');
 
 INSERT INTO tpch.orders VALUES(10,38197,'O',130153.51,'1996-09-10','1-URGENT','Clerk#000000145',0,'ironic, even requests');
 


