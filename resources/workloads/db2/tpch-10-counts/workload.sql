SELECT 17,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  c_acctbal BETWEEN 6677.25489709987 AND 8763.599323234928
       AND o_totalprice BETWEEN 161409.62125961325 AND 161978.7926355417
       AND o_orderdate BETWEEN 'Sun Jan 18 01:09:19 PST 1998' AND 'Sat Jan 30 01:09:19 PST 1999'
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND o_orderkey = l_orderkey
       AND c_custkey = o_custkey;

SELECT 70,
       COUNT(*)
FROM   tpch.orders
WHERE  o_orderdate BETWEEN 'Fri Feb 25 22:04:22 PST 1994' AND 'Fri Jan 20 22:04:22 PST 1995'
       AND o_totalprice BETWEEN 37365.68396464411 AND 106698.2608989657;

SELECT 238,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_shipdate BETWEEN 'Fri Mar 29 18:13:51 PST 1996' AND 'Thu May 22 19:13:51 PDT 1997'
       AND l_discount BETWEEN 0.0865187031064013 AND 0.10205372833216683;

SELECT 299,
       COUNT(*)
FROM   tpch.customer,
       tpch.orders
WHERE  o_totalprice BETWEEN 258507.55638880268 AND 359145.14704425284
       AND c_custkey = o_custkey;

SELECT 308,
       COUNT(*)
FROM   tpch.customer
WHERE  c_acctbal BETWEEN 312.82135226880155 AND 1442.9677544071765;

SELECT 320,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  ps_availqty BETWEEN 1815 AND 1884
       AND l_receiptdate BETWEEN 'Sun Nov 17 21:24:24 PST 1996' AND 'Tue Dec 03 21:24:24 PST 1996'
       AND l_commitdate BETWEEN 'Thu Mar 14 13:11:42 PST 1996' AND 'Mon Apr 14 14:11:42 PDT 1997'
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 381,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.part
WHERE  p_size BETWEEN 30 AND 35
       AND p_retailprice BETWEEN 2087.6441738108497 AND 2221.1038477240363
       AND p_partkey = ps_partkey;

SELECT 475,
       COUNT(*)
FROM   tpch.partsupp
WHERE  ps_availqty BETWEEN 1988 AND 2006;

SELECT 676,
       COUNT(*)
FROM   tpch.part
WHERE  p_retailprice BETWEEN 1012.9358143247314 AND 1252.0589225503832;

SELECT 990,
       COUNT(*)
FROM   tpch.orders
WHERE  o_orderdate BETWEEN 'Tue May 12 00:11:45 PDT 1992' AND 'Mon Jun 01 00:11:45 PDT 1992'
       AND o_totalprice BETWEEN 151738.82293264213 AND 240597.6420256541;

SELECT 17,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  c_acctbal BETWEEN 6677.25489709987 AND 8763.599323234928
       AND o_totalprice BETWEEN 161409.62125961325 AND 161978.7926355417
       AND o_orderdate BETWEEN 'Sun Jan 18 01:09:19 PST 1998' AND 'Sat Jan 30 01:09:19 PST 1999'
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND o_orderkey = l_orderkey
       AND c_custkey = o_custkey;

SELECT 70,
       COUNT(*)
FROM   tpch.orders
WHERE  o_orderdate BETWEEN 'Fri Feb 25 22:04:22 PST 1994' AND 'Fri Jan 20 22:04:22 PST 1995'
       AND o_totalprice BETWEEN 37365.68396464411 AND 106698.2608989657;

SELECT 238,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_shipdate BETWEEN 'Fri Mar 29 18:13:51 PST 1996' AND 'Thu May 22 19:13:51 PDT 1997'
       AND l_discount BETWEEN 0.0865187031064013 AND 0.10205372833216683;

SELECT 299,
       COUNT(*)
FROM   tpch.customer,
       tpch.orders
WHERE  o_totalprice BETWEEN 258507.55638880268 AND 359145.14704425284
       AND c_custkey = o_custkey;

SELECT 308,
       COUNT(*)
FROM   tpch.customer
WHERE  c_acctbal BETWEEN 312.82135226880155 AND 1442.9677544071765;
