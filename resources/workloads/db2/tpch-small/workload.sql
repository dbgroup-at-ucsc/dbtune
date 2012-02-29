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
    l_shipdate >= '1998-08-01' and
    l_shipdate <  '1998-11-13'
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus;

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
    and o_orderdate < '1995-11-01'
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

--query 05
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
    and o_orderdate < '1994-01-01'
group by
    n_name
order by
    revenue desc;

--query 10
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
    and o_orderdate >= '1993-11-01'
    and o_orderdate < '1994-2-01'
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

--query 13
select
    c_count,
    count(*) as custdist
from
    (
        select
            c_custkey,
            count(o_orderkey)
        from
            tpch.customer left outer join tpch.orders on
                c_custkey = o_custkey
                and o_comment not like '%special%packages%'
        group by
            c_custkey
    ) as c_orders (c_custkey, c_count)
group by
    c_count
order by
    custdist desc,
    c_count desc;
