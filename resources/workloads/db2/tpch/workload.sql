-- TPC TPC-H Parameter Substitution (Version 2.14.0 build 0)
-- using 1326918973 as a seed to the RNG
-- $ID$
-- TPC-H/TPC-R Returned Item Reporting Query (Q10)
-- Functional Query Definition
-- Approved February 1998


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
    and o_orderdate >= date '1994-09-01'
    and o_orderdate < date '1994-09-01' + interval '3' month
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

--#SET ROWS_FETCH 20
-- TPC TPC-H Parameter Substitution (Version 2.14.0 build 0)
-- using 1326918973 as a seed to the RNG
-- $ID$
-- TPC-H/TPC-R Important Stock Identification Query (Q11)
-- Functional Query Definition
-- Approved February 1998


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
    and n_name = 'UNITED STATES'
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
                and n_name = 'UNITED STATES'
        )
order by
    value desc;

--#SET ROWS_FETCH -1
-- TPC TPC-H Parameter Substitution (Version 2.14.0 build 0)
-- using 1326918973 as a seed to the RNG
-- $ID$
-- TPC-H/TPC-R Shipping Modes and Order Priority Query (Q12)
-- Functional Query Definition
-- Approved February 1998

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
    and l_shipmode in ('MAIL', 'RAIL')
    and l_commitdate < l_receiptdate
    and l_shipdate < l_commitdate
    and l_receiptdate >= date '1997-01-01'
    and l_receiptdate < date '1997-01-01' + interval '1' year
group by
    l_shipmode
order by
    l_shipmode;
--#SET ROWS_FETCH -1
-- TPC TPC-H Parameter Substitution (Version 2.14.0 build 0)
-- using 1326918973 as a seed to the RNG
-- $ID$
-- TPC-H/TPC-R Customer Distribution Query (Q13)
-- Functional Query Definition
-- Approved February 1998


select
    c_count,
    count(*) as custdist
from
    (
        select
            c_custkey,
            count(o_orderkey)
        from
            tpch.customer left outer tpch.join orders on
                c_custkey = o_custkey
                and o_comment not like '%pending%deposits%'
        group by
            c_custkey
    ) as c_orders (c_custkey, c_count)
group by
    c_count
order by
    custdist desc,
    c_count desc;
--#SET ROWS_FETCH -1
-- TPC TPC-H Parameter Substitution (Version 2.14.0 build 0)
-- using 1326918973 as a seed to the RNG
-- $ID$
-- TPC-H/TPC-R Promotion Effect Query (Q14)
-- Functional Query Definition
-- Approved February 1998


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
    and l_shipdate >= date '1997-05-01'
    and l_shipdate < date '1997-05-01' + interval '1' month;
--#SET ROWS_FETCH -1
-- TPC TPC-H Parameter Substitution (Version 2.14.0 build 0)
-- using 1326918973 as a seed to the RNG
-- $ID$
-- TPC-H/TPC-R Top Supplier Query (Q15)
-- Functional Query Definition
-- Approved February 1998

create view revenue0 (supplier_no, total_revenue) as
    select
        l_suppkey,
        sum(l_extendedprice * (1 - l_discount))
    from
        tpch.lineitem
    where
        l_shipdate >= date '1995-08-01'
        and l_shipdate < date '1995-08-01' + interval '3' month
    group by
        l_suppkey;


select
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
from
    tpch.supplier,
    tpch.revenue0
where
    s_suppkey = supplier_no
    and total_revenue = (
        select
            max(total_revenue)
        from
            tpch.revenue0
    )
order by
    s_suppkey;

drop view revenue0;
--#SET ROWS_FETCH -1
-- TPC TPC-H Parameter Substitution (Version 2.14.0 build 0)
-- using 1326918973 as a seed to the RNG
-- $ID$
-- TPC-H/TPC-R Parts/Supplier Relationship Query (Q16)
-- Functional Query Definition
-- Approved February 1998


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
    and p_brand <> 'Brand#32'
    and p_type not like 'LARGE ANODIZED%'
    and p_size in (9, 25, 11, 6, 4, 21, 24, 2)
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
--#SET ROWS_FETCH -1
-- TPC TPC-H Parameter Substitution (Version 2.14.0 build 0)
-- using 1326918973 as a seed to the RNG
-- $ID$
-- TPC-H/TPC-R Small-Quantity-Order Revenue Query (Q17)
-- Functional Query Definition
-- Approved February 1998


select
    sum(l_extendedprice) / 7.0 as avg_yearly
from
    tpch.lineitem,
    tpch.part
where
    p_partkey = l_partkey
    and p_brand = 'Brand#23'
    and p_container = 'SM CAN'
    and l_quantity < (
        select
            0.2 * avg(l_quantity)
        from
            tpch.lineitem
        where
            l_partkey = p_partkey
    );
--#SET ROWS_FETCH -1
-- TPC TPC-H Parameter Substitution (Version 2.14.0 build 0)
-- using 1326918973 as a seed to the RNG
-- $ID$
-- TPC-H/TPC-R Large Volume Customer Query (Q18)
-- Function Query Definition
-- Approved February 1998


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
                sum(l_quantity) > 314
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
--#SET ROWS_FETCH 100
-- TPC TPC-H Parameter Substitution (Version 2.14.0 build 0)
-- using 1326918973 as a seed to the RNG
-- $ID$
-- TPC-H/TPC-R Discounted Revenue Query (Q19)
-- Functional Query Definition
-- Approved February 1998


select
    sum(l_extendedprice* (1 - l_discount)) as revenue
from
    tpch.lineitem,
    tpch.part
where
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#13'
        and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        and l_quantity >= 4 and l_quantity <= 4 + 10
        and p_size between 1 and 5
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#11'
        and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        and l_quantity >= 14 and l_quantity <= 14 + 10
        and p_size between 1 and 10
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
        p_partkey = l_partkey
        and p_brand = 'Brand#12'
        and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        and l_quantity >= 29 and l_quantity <= 29 + 10
        and p_size between 1 and 15
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    );
--#SET ROWS_FETCH -1
-- TPC TPC-H Parameter Substitution (Version 2.14.0 build 0)
-- using 1326918973 as a seed to the RNG
-- $ID$
-- TPC-H/TPC-R Pricing Summary Report Query (Q1)
-- Functional Query Definition
-- Approved February 1998


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
    l_shipdate <= date '1998-12-01' - interval '105' day (3)
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus;
--#SET ROWS_FETCH -1
-- TPC TPC-H Parameter Substitution (Version 2.14.0 build 0)
-- using 1326918973 as a seed to the RNG
-- $ID$
-- TPC-H/TPC-R Potential Part Promotion Query (Q20)
-- Function Query Definition
-- Approved February 1998


select
    s_name,
    s_address
from
    tpch.supplier,
    tpch.nation
where
    s_suppkey in (
        select
            ps_suppkey
        from
            tpch.partsupp
        where
            ps_partkey in (
                select
                    p_partkey
                from
                    tpch.part
                where
                    p_name like 'lemon%'
            )
            and ps_availqty > (
                select
                    0.5 * sum(l_quantity)
                from
                    tpch.lineitem
                where
                    l_partkey = ps_partkey
                    and l_suppkey = ps_suppkey
                    and l_shipdate >= date '1993-01-01'
                    and l_shipdate < date '1993-01-01' + interval '1' year
            )
    )
    and s_nationkey = n_nationkey
    and n_name = 'ARGENTINA'
order by
    s_name;
--#SET ROWS_FETCH -1
-- TPC TPC-H Parameter Substitution (Version 2.14.0 build 0)
-- using 1326918973 as a seed to the RNG
-- $ID$
-- TPC-H/TPC-R Suppliers Who Kept Orders Waiting Query (Q21)
-- Functional Query Definition
-- Approved February 1998


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
    and n_name = 'EGYPT'
group by
    s_name
order by
    numwait desc,
    s_name;
--#SET ROWS_FETCH 100
-- TPC TPC-H Parameter Substitution (Version 2.14.0 build 0)
-- using 1326918973 as a seed to the RNG
-- $ID$
-- TPC-H/TPC-R Global Sales Opportunity Query (Q22)
-- Functional Query Definition
-- Approved February 1998


select
    cntrycode,
    count(*) as numcust,
    sum(c_acctbal) as totacctbal
from
    (
        select
            substring(c_phone from 1 for 2) as cntrycode,
            c_acctbal
        from
            tpch.customer
        where
            substring(c_phone from 1 for 2) in
                ('21', '20', '29', '31', '32', '42', '24')
            and c_acctbal > (
                select
                    avg(c_acctbal)
                from
                    tpch.customer
                where
                    c_acctbal > 0.00
                    and substring(c_phone from 1 for 2) in
                        ('21', '20', '29', '31', '32', '42', '24')
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
--#SET ROWS_FETCH -1
-- TPC TPC-H Parameter Substitution (Version 2.14.0 build 0)
-- using 1326918973 as a seed to the RNG
-- $ID$
-- TPC-H/TPC-R Minimum Cost Supplier Query (Q2)
-- Functional Query Definition
-- Approved February 1998


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
    and p_size = 7
    and p_type like '%BRASS'
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
--#SET ROWS_FETCH 100
-- TPC TPC-H Parameter Substitution (Version 2.14.0 build 0)
-- using 1326918973 as a seed to the RNG
-- $ID$
-- TPC-H/TPC-R Shipping Priority Query (Q3)
-- Functional Query Definition
-- Approved February 1998


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
    c_mktsegment = 'AUTOMOBILE'
    and c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate < date '1995-03-16'
    and l_shipdate > date '1995-03-16'
group by
    l_orderkey,
    o_orderdate,
    o_shippriority
order by
    revenue desc,
    o_orderdate;
--#SET ROWS_FETCH 10
-- TPC TPC-H Parameter Substitution (Version 2.14.0 build 0)
-- using 1326918973 as a seed to the RNG
-- $ID$
-- TPC-H/TPC-R Order Priority Checking Query (Q4)
-- Functional Query Definition
-- Approved February 1998


select
    o_orderpriority,
    count(*) as order_count
from
    tpch.orders
where
    o_orderdate >= date '1995-07-01'
    and o_orderdate < date '1995-07-01' + interval '3' month
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
--#SET ROWS_FETCH -1
-- TPC TPC-H Parameter Substitution (Version 2.14.0 build 0)
-- using 1326918973 as a seed to the RNG
-- $ID$
-- TPC-H/TPC-R Local Supplier Volume Query (Q5)
-- Functional Query Definition
-- Approved February 1998


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
    and r_name = 'EUROPE'
    and o_orderdate >= date '1997-01-01'
    and o_orderdate < date '1997-01-01' + interval '1' year
group by
    n_name
order by
    revenue desc;
--#SET ROWS_FETCH -1
-- TPC TPC-H Parameter Substitution (Version 2.14.0 build 0)
-- using 1326918973 as a seed to the RNG
-- $ID$
-- TPC-H/TPC-R Forecasting Revenue Change Query (Q6)
-- Functional Query Definition
-- Approved February 1998


select
    sum(l_extendedprice * l_discount) as revenue
from
    tpch.lineitem
where
    l_shipdate >= date '1997-01-01'
    and l_shipdate < date '1997-01-01' + interval '1' year
    and l_discount between 0.09 - 0.01 and 0.09 + 0.01
    and l_quantity < 24;
--#SET ROWS_FETCH -1
-- TPC TPC-H Parameter Substitution (Version 2.14.0 build 0)
-- using 1326918973 as a seed to the RNG
-- $ID$
-- TPC-H/TPC-R Volume Shipping Query (Q7)
-- Functional Query Definition
-- Approved February 1998


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
            extract(year from tpch.l_shipdate) as l_year,
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
                (n1.n_name = 'RUSSIA' and n2.n_name = 'EGYPT')
                or (n1.n_name = 'EGYPT' and n2.n_name = 'RUSSIA')
            )
            and l_shipdate between date '1995-01-01' and date '1996-12-31'
    ) as shipping
group by
    supp_nation,
    cust_nation,
    l_year
order by
    supp_nation,
    cust_nation,
    l_year;
--#SET ROWS_FETCH -1
-- TPC TPC-H Parameter Substitution (Version 2.14.0 build 0)
-- using 1326918973 as a seed to the RNG
-- $ID$
-- TPC-H/TPC-R National Market Share Query (Q8)
-- Functional Query Definition
-- Approved February 1998


select
    o_year,
    sum(case
        when nation = 'EGYPT' then volume
        else 0
    end) / sum(volume) as mkt_share
from
    (
        select
            extract(year from tpch.o_orderdate) as o_year,
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
            and r_name = 'MIDDLE EAST'
            and s_nationkey = n2.n_nationkey
            and o_orderdate between date '1995-01-01' and date '1996-12-31'
            and p_type = 'LARGE BURNISHED BRASS'
    ) as all_nations
group by
    o_year
order by
    o_year;
--#SET ROWS_FETCH -1
-- TPC TPC-H Parameter Substitution (Version 2.14.0 build 0)
-- using 1326918973 as a seed to the RNG
-- $ID$
-- TPC-H/TPC-R Product Type Profit Measure Query (Q9)
-- Functional Query Definition
-- Approved February 1998


select
    nation,
    o_year,
    sum(amount) as sum_profit
from
    (
        select
            n_name as nation,
            extract(year from tpch.o_orderdate) as o_year,
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
            and p_name like '%metallic%'
    ) as profit
group by
    nation,
    o_year
order by
    nation,
    o_year desc;
--#SET ROWS_FETCH -1
