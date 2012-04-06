SELECT 1,
       COUNT(*)
FROM   tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  l_quantity BETWEEN 46.79928318619356 AND 54.084424197386845
       AND l_receiptdate BETWEEN 'Sun Oct 15 11:04:29 PDT 1995' AND 'Thu Oct 19 11:04:29 PDT 1995'
       AND l_extendedprice BETWEEN 92965.82889495825 AND 93931.85215396482
       AND o_orderkey = l_orderkey
       AND c_custkey = o_custkey;

SELECT 3,
       COUNT(*)
FROM   tpch.supplier,
       tpch.partsupp,
       tpch.part
WHERE  s_acctbal BETWEEN 1697.1037510223762 AND 3529.066531923416
       AND ps_supplycost BETWEEN 425.75447107608784 AND 609.213188094174
       AND ps_availqty BETWEEN 1356 AND 2833
       AND p_retailprice BETWEEN 1261.0674604281671 AND 1404.4468989518643
       AND p_size BETWEEN 28 AND 35
       AND s_suppkey = ps_suppkey
       AND p_partkey = ps_partkey;

SELECT 5,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_extendedprice BETWEEN 79861.36753562656 AND 80707.89082898502
       AND l_commitdate BETWEEN 'Sun Dec 12 04:29:36 PST 1993' AND 'Mon Oct 31 04:29:36 PST 1994';

SELECT 6,
       COUNT(*)
FROM   tpch.part
WHERE  p_size BETWEEN 3 AND 3;

SELECT 8,
       COUNT(*)
FROM   tpch.orders
WHERE  o_totalprice BETWEEN 88840.85427847636 AND 174191.8333814839
       AND o_orderdate BETWEEN 'Thu Feb 19 04:09:56 PST 1998' AND 'Thu Apr 22 05:09:56 PDT 1999';

SELECT 9,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_commitdate BETWEEN 'Wed Dec 10 23:27:01 PST 1997' AND 'Sun Feb 21 23:27:01 PST 1999';

SELECT 10,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_shipdate BETWEEN 'Wed Dec 20 20:19:41 PST 1995' AND 'Tue Feb 04 20:19:41 PST 1997'
       AND l_quantity BETWEEN 9.790619482640395 AND 10.101984369450374
       AND l_receiptdate BETWEEN 'Sun Mar 12 08:54:07 PST 1995' AND 'Fri May 03 09:54:07 PDT 1996';

SELECT 13,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_commitdate BETWEEN 'Sun Jan 11 20:15:03 PST 1998' AND 'Mon Feb 15 20:15:03 PST 1999'
       AND l_tax BETWEEN 0.05540154857564852 AND 0.06806450598302848;

SELECT 14,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_receiptdate BETWEEN 'Mon Nov 07 02:35:54 PST 1994' AND 'Fri Dec 08 02:35:54 PST 1995';

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

SELECT 23,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_commitdate BETWEEN 'Wed Dec 10 23:27:01 PST 1997' AND 'Sun Feb 21 23:27:01 PST 1999';

SELECT 24,
       COUNT(*)
FROM   tpch.orders
WHERE  o_totalprice BETWEEN 51265.464079043435 AND 112628.82025791754
       AND o_orderdate BETWEEN 'Thu Apr 06 05:53:23 PDT 1995' AND 'Sat Dec 16 04:53:23 PST 1995';

SELECT 27,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  l_receiptdate BETWEEN 'Wed Jul 14 10:16:19 PDT 1993' AND 'Fri Jun 17 10:16:19 PDT 1994'
       AND l_commitdate BETWEEN 'Sat Dec 18 06:24:40 PST 1993' AND 'Thu Feb 23 06:24:40 PST 1995'
       AND o_orderkey = l_orderkey;

SELECT 30,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.part,
       tpch.lineitem,
       tpch.orders
WHERE  p_retailprice BETWEEN 1304.6957402590888 AND 1307.7796820431902
       AND p_size BETWEEN 18 AND 23
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND p_partkey = ps_partkey;

SELECT 31,
       COUNT(*)
FROM   tpch.customer
WHERE  c_acctbal BETWEEN 312.82135226880155 AND 1442.9677544071765;

SELECT 35,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_shipdate BETWEEN 'Wed Aug 19 02:14:59 PDT 1992' AND 'Tue Jul 27 02:14:59 PDT 1993';

SELECT 55,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  o_orderdate BETWEEN 'Tue Nov 22 14:03:25 PST 1994' AND 'Sun Jul 30 15:03:25 PDT 1995'
       AND o_totalprice BETWEEN 164784.46295508684 AND 256069.87878254591
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 56,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  l_commitdate BETWEEN 'Sat Oct 24 02:16:56 PDT 1992' AND 'Mon Jan 24 01:16:56 PST 1994'
       AND l_quantity BETWEEN 6.067721881448492 AND 12.98122019260068
       AND o_totalprice BETWEEN 150188.80381220076 AND 217951.59444053945
       AND o_orderkey = l_orderkey;

SELECT 57,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.part,
       tpch.lineitem,
       tpch.orders
WHERE  p_size BETWEEN 29 AND 29
       AND p_retailprice BETWEEN 1269.472447746158 AND 1440.8117400932254
       AND o_orderdate BETWEEN 'Tue Jun 16 15:58:45 PDT 1998' AND 'Sun Apr 04 15:58:45 PDT 1999'
       AND o_totalprice BETWEEN 39533.306503719294 AND 44662.347871495906
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND o_orderkey = l_orderkey
       AND p_partkey = ps_partkey;

SELECT 62,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.part,
       tpch.lineitem,
       tpch.orders
WHERE  p_retailprice BETWEEN 1876.2270944246504 AND 1883.84086242501
       AND l_receiptdate BETWEEN 'Sun Sep 21 14:23:49 PDT 1997' AND 'Wed Jan 06 13:23:49 PST 1999'
       AND l_shipdate BETWEEN 'Tue Oct 14 12:39:17 PDT 1997' AND 'Fri Feb 05 11:39:17 PST 1999'
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND p_partkey = ps_partkey;

SELECT 70,
       COUNT(*)
FROM   tpch.orders
WHERE  o_orderdate BETWEEN 'Fri Feb 25 22:04:22 PST 1994' AND 'Fri Jan 20 22:04:22 PST 1995'
       AND o_totalprice BETWEEN 37365.68396464411 AND 106698.2608989657;

SELECT 73,
       COUNT(*)
FROM   tpch.nation,
       tpch.customer,
       tpch.orders
WHERE  o_orderdate BETWEEN 'Sat Oct 07 21:58:04 PDT 1995' AND 'Sat Dec 28 20:58:04 PST 1996'
       AND o_totalprice BETWEEN 366689.6662007361 AND 468947.0794495564
       AND c_custkey = o_custkey
       AND n_nationkey = c_nationkey;

SELECT 74,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_shipdate BETWEEN 'Wed Jan 04 18:06:57 PST 1995' AND 'Mon Mar 04 18:06:57 PST 1996'
       AND l_tax BETWEEN 0.026251812127526176 AND 0.041359376193486855;

SELECT 75,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.part,
       tpch.lineitem,
       tpch.orders
WHERE  p_size BETWEEN 26 AND 33
       AND o_totalprice BETWEEN 416287.9048999934 AND 420400.2249158094
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND p_partkey = ps_partkey
       AND o_orderkey = l_orderkey;

SELECT 76,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  l_extendedprice BETWEEN 22898.55557537188 AND 36682.317071133846
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 83,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  ps_availqty BETWEEN 9609 AND 9676
       AND ps_supplycost BETWEEN 656.2181096779609 AND 853.1264542767201
       AND l_commitdate BETWEEN 'Mon Jul 13 22:29:23 PDT 1998' AND 'Tue Apr 27 22:29:23 PDT 1999'
       AND l_extendedprice BETWEEN 82133.3178949841 AND 93560.50903247872
       AND l_discount BETWEEN 0.03206797027658611 AND 0.044944241397877754
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 85,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_quantity BETWEEN 37.05799160857271 AND 42.9896659990098;

SELECT 98,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  l_receiptdate BETWEEN 'Fri Aug 19 06:31:14 PDT 1994' AND 'Fri Nov 10 05:31:14 PST 1995'
       AND l_tax BETWEEN 0.009224182225769458 AND 0.02282724558028025
       AND o_orderkey = l_orderkey;

SELECT 101,
       COUNT(*)
FROM   tpch.supplier,
       tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  ps_availqty BETWEEN 2295 AND 2309
       AND ps_supplycost BETWEEN 850.5471573411701 AND 1029.0154826222245
       AND o_orderdate BETWEEN 'Fri Jun 12 04:04:51 PDT 1992' AND 'Sat Jun 19 04:04:51 PDT 1993'
       AND o_totalprice BETWEEN 145734.98234268453 AND 233202.77418591047
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND s_suppkey = ps_suppkey;

SELECT 103,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  l_commitdate BETWEEN 'Fri Oct 09 23:31:28 PDT 1992' AND 'Mon Oct 26 22:31:28 PST 1992'
       AND l_discount BETWEEN 0.015671817495096243 AND 0.030657312880907815
       AND o_orderkey = l_orderkey;

SELECT 107,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_commitdate BETWEEN 'Tue Apr 09 12:28:00 PDT 1996' AND 'Thu Apr 18 12:28:00 PDT 1996'
       AND l_discount BETWEEN 0.09522435794735352 AND 0.10625840244251666;

SELECT 110,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_receiptdate BETWEEN 'Wed Oct 27 15:57:19 PDT 1993' AND 'Wed Nov 03 14:57:19 PST 1993';

SELECT 112,
       COUNT(*)
FROM   tpch.nation,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  o_totalprice BETWEEN 230879.12916992704 AND 333315.3073925446
       AND o_orderdate BETWEEN 'Wed Jan 26 11:27:15 PST 1994' AND 'Sun Sep 25 12:27:15 PDT 1994'
       AND o_orderkey = l_orderkey
       AND c_custkey = o_custkey
       AND n_nationkey = c_nationkey;

SELECT 119,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_commitdate BETWEEN 'Fri Oct 30 13:50:56 PST 1998' AND 'Sat Jul 24 14:50:56 PDT 1999';

SELECT 122,
       COUNT(*)
FROM   tpch.customer,
       tpch.orders
WHERE  c_acctbal BETWEEN 9229.57776560095 AND 11025.372821043098
       AND o_orderdate BETWEEN 'Wed Jun 30 14:05:29 PDT 1993' AND 'Wed Jun 30 14:05:29 PDT 1993'
       AND c_custkey = o_custkey;

SELECT 123,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_discount BETWEEN 0.06007189104601875 AND 0.07696281143968974;

SELECT 125,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.part
WHERE  p_size BETWEEN 30 AND 35
       AND p_retailprice BETWEEN 2087.6441738108497 AND 2221.1038477240363
       AND p_partkey = ps_partkey;

SELECT 129,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_extendedprice BETWEEN 11000.971739330713 AND 27075.2052042465
       AND l_shipdate BETWEEN 'Wed Feb 11 14:05:46 PST 1998' AND 'Fri Feb 20 14:05:46 PST 1998'
       AND l_tax BETWEEN 0.0716690599060162 AND 0.08213518539176037;

SELECT 132,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_receiptdate BETWEEN 'Fri Aug 19 06:31:14 PDT 1994' AND 'Fri Nov 10 05:31:14 PST 1995'
       AND l_tax BETWEEN 0.009224182225769458 AND 0.02282724558028025;

SELECT 134,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  l_discount BETWEEN 0.07045942642411705 AND 0.08842602477416042
       AND o_orderkey = l_orderkey;

SELECT 137,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  l_quantity BETWEEN 10.146153800606317 AND 19.474468615602497
       AND l_discount BETWEEN 0.02528449124236334 AND 0.03711682509859327
       AND o_orderkey = l_orderkey;

SELECT 139,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_receiptdate BETWEEN 'Wed Oct 27 15:57:19 PDT 1993' AND 'Wed Nov 03 14:57:19 PST 1993';

SELECT 140,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_quantity BETWEEN 33.855766685738445 AND 34.087630661524855;

SELECT 143,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_receiptdate BETWEEN 'Tue Jan 07 16:25:58 PST 1992' AND 'Fri Jan 17 16:25:58 PST 1992';

SELECT 148,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_shipdate BETWEEN 'Sat Jul 03 10:13:43 PDT 1993' AND 'Mon Jul 05 10:13:43 PDT 1993'
       AND l_extendedprice BETWEEN 49060.8638210462 AND 49143.03209270037;

SELECT 155,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_commitdate BETWEEN 'Sat Dec 25 17:30:04 PST 1993' AND 'Wed Nov 30 17:30:04 PST 1994'
       AND l_quantity BETWEEN 46.7016629561088 AND 47.062516879845965;

SELECT 164,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_quantity BETWEEN 9.5809564417915 AND 16.84929953526894
       AND l_shipdate BETWEEN 'Sat Dec 25 15:00:02 PST 1993' AND 'Tue Feb 07 15:00:02 PST 1995';

SELECT 165,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  ps_availqty BETWEEN 1570 AND 1638
       AND ps_supplycost BETWEEN 333.76864027027574 AND 496.7074421507018
       AND l_extendedprice BETWEEN 40468.9203889098 AND 41026.35864495058
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND o_orderkey = l_orderkey
       AND c_custkey = o_custkey;

SELECT 169,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_commitdate BETWEEN 'Tue Apr 09 12:28:00 PDT 1996' AND 'Thu Apr 18 12:28:00 PDT 1996'
       AND l_discount BETWEEN 0.09522435794735352 AND 0.10625840244251666;

SELECT 176,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.part,
       tpch.lineitem,
       tpch.orders
WHERE  ps_availqty BETWEEN 4068 AND 5296
       AND ps_supplycost BETWEEN 331.905351166803 AND 524.6317436732967
       AND p_retailprice BETWEEN 1371.8101931586873 AND 1537.8622120719838
       AND l_quantity BETWEEN 7.35688970824695 AND 13.311235058971361
       AND p_partkey = ps_partkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND o_orderkey = l_orderkey;

SELECT 179,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_shipdate BETWEEN 'Fri Oct 30 02:32:54 PST 1992' AND 'Tue Nov 30 02:32:54 PST 1993'
       AND l_tax BETWEEN 0.04108884928769393 AND 0.05011421932192828
       AND l_extendedprice BETWEEN 59142.99081361058 AND 71934.40933506633;

SELECT 184,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_quantity BETWEEN 33.855766685738445 AND 34.087630661524855;

SELECT 185,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_receiptdate BETWEEN 'Tue Mar 07 05:49:47 PST 1995' AND 'Fri Dec 22 05:49:47 PST 1995'
       AND l_tax BETWEEN 1.0878679001300462E-4 AND 0.010796731927829225;

SELECT 187,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_discount BETWEEN 0.0727439445357074 AND 0.0905724430384972
       AND l_receiptdate BETWEEN 'Mon Sep 11 01:53:19 PDT 1995' AND 'Fri Sep 15 01:53:19 PDT 1995'
       AND l_tax BETWEEN 0.011610299825874072 AND 0.02142563462032334;

SELECT 188,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_receiptdate BETWEEN 'Mon Jul 01 22:57:32 PDT 1996' AND 'Mon Nov 03 21:57:32 PST 1997'
       AND l_quantity BETWEEN 31.003740123476465 AND 40.635132400532854;

SELECT 190,
       COUNT(*)
FROM   tpch.nation,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  l_discount BETWEEN 0.07028091622115855 AND 0.08126612370324302
       AND c_custkey = o_custkey
       AND o_orderkey = l_orderkey
       AND n_nationkey = c_nationkey;

SELECT 193,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  ps_supplycost BETWEEN 950.926281604935 AND 960.091306941197
       AND ps_availqty BETWEEN 777 AND 875
       AND l_shipdate BETWEEN 'Wed Jul 15 22:26:41 PDT 1992' AND 'Fri May 28 22:26:41 PDT 1993'
       AND l_discount BETWEEN 0.06848268504343441 AND 0.08577124127557031
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 204,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_extendedprice BETWEEN 33579.18531992014 AND 33925.73625403036;

SELECT 205,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.part,
       tpch.lineitem
WHERE  ps_supplycost BETWEEN 825.3753441312214 AND 1019.0719891803034
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND p_partkey = ps_partkey;

SELECT 208,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_tax BETWEEN 0.03938465765238244 AND 0.05267698532660164
       AND l_shipdate BETWEEN 'Sat Apr 13 03:17:30 PDT 1996' AND 'Sun Apr 14 03:17:30 PDT 1996';

SELECT 213,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_tax BETWEEN 0.03817878630947157 AND 0.04775873764957771
       AND l_receiptdate BETWEEN 'Sat Oct 01 05:10:19 PDT 1994' AND 'Sat Oct 08 05:10:19 PDT 1994';

SELECT 216,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_extendedprice BETWEEN 3042.653732743923 AND 20735.148027259504
       AND l_commitdate BETWEEN 'Tue May 05 03:44:37 PDT 1992' AND 'Thu May 14 03:44:37 PDT 1992';

SELECT 221,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  l_shipdate BETWEEN 'Fri Sep 08 20:49:24 PDT 1995' AND 'Sun Oct 01 20:49:24 PDT 1995'
       AND l_commitdate BETWEEN 'Fri Jul 14 07:00:40 PDT 1995' AND 'Fri Jul 28 07:00:40 PDT 1995'
       AND o_orderkey = l_orderkey;

SELECT 223,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_shipdate BETWEEN 'Sun Oct 25 04:33:54 PST 1998' AND 'Tue Nov 17 04:33:54 PST 1998';

SELECT 229,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_extendedprice BETWEEN 45145.176819492575 AND 65836.71176580846
       AND l_commitdate BETWEEN 'Wed Feb 23 16:09:07 PST 1994' AND 'Sat Mar 12 16:09:07 PST 1994'
       AND l_tax BETWEEN 0.01169657582094306 AND 0.025459364889956884;

SELECT 230,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.part
WHERE  p_retailprice BETWEEN 1068.5782220465414 AND 1292.8896177366923
       AND p_partkey = ps_partkey;

SELECT 232,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  ps_supplycost BETWEEN 153.53678518617266 AND 258.79027139210916
       AND ps_availqty BETWEEN 784 AND 1900
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 233,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  ps_supplycost BETWEEN 271.5106237547423 AND 441.86646809462786
       AND ps_availqty BETWEEN 1918 AND 3418
       AND o_orderkey = l_orderkey
       AND c_custkey = o_custkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 238,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_shipdate BETWEEN 'Fri Mar 29 18:13:51 PST 1996' AND 'Thu May 22 19:13:51 PDT 1997'
       AND l_discount BETWEEN 0.0865187031064013 AND 0.10205372833216683;

SELECT 240,
       COUNT(*)
FROM   tpch.orders
WHERE  o_totalprice BETWEEN 101771.07346208688 AND 182578.43132383245
       AND o_orderdate BETWEEN 'Thu Jul 14 17:04:18 PDT 1994' AND 'Wed Jun 07 17:04:18 PDT 1995';

SELECT 241,
       COUNT(*)
FROM   tpch.orders
WHERE  o_totalprice BETWEEN 98895.7880238376 AND 193775.37797943834
       AND o_orderdate BETWEEN 'Fri Nov 08 15:58:56 PST 1996' AND 'Tue Nov 18 15:58:56 PST 1997';

SELECT 243,
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

SELECT 246,
       COUNT(*)
FROM   tpch.supplier,
       tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  ps_supplycost BETWEEN 932.1476588017267 AND 933.606332579753
       AND l_extendedprice BETWEEN 30312.429244477506 AND 47153.08228529323
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND o_orderkey = l_orderkey
       AND s_suppkey = ps_suppkey;

SELECT 247,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_quantity BETWEEN 18.066317354892615 AND 23.83295473718668
       AND l_tax BETWEEN 0.025091593078469598 AND 0.035695332579979894;

SELECT 248,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_commitdate BETWEEN 'Sat Aug 01 14:45:24 PDT 1992' AND 'Mon Aug 24 14:45:24 PDT 1992';

SELECT 255,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  l_commitdate BETWEEN 'Thu Mar 18 23:02:01 PST 1993' AND 'Mon Jan 17 23:02:01 PST 1994'
       AND l_quantity BETWEEN 41.18213025470975 AND 50.28155048405537
       AND l_shipdate BETWEEN 'Thu Jul 08 07:00:39 PDT 1993' AND 'Sun Aug 07 07:00:39 PDT 1994'
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 258,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_tax BETWEEN 0.02651887513511066 AND 0.03689247696523512;

SELECT 261,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  ps_availqty BETWEEN 7260 AND 7299
       AND o_totalprice BETWEEN 138869.64529159525 AND 225113.26095612906
       AND o_orderdate BETWEEN 'Tue Aug 12 11:41:09 PDT 1997' AND 'Wed Sep 16 11:41:09 PDT 1998'
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND o_orderkey = l_orderkey;

SELECT 264,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  l_extendedprice BETWEEN 63796.50647155328 AND 75863.66829316047
       AND l_shipdate BETWEEN 'Mon Oct 19 23:46:48 PDT 1992' AND 'Fri Nov 12 22:46:48 PST 1993'
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 266,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem
WHERE  ps_availqty BETWEEN 5447 AND 7277
       AND ps_supplycost BETWEEN 772.9779489467965 AND 782.2793836411959
       AND l_tax BETWEEN 0.0777377997601259 AND 0.08632922005539853
       AND l_extendedprice BETWEEN 73654.79142298072 AND 74133.90678554328
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 268,
       COUNT(*)
FROM   tpch.orders
WHERE  o_totalprice BETWEEN 243623.15670543376 AND 306373.32313111384
       AND o_orderdate BETWEEN 'Sat Apr 06 20:57:14 PST 1996' AND 'Thu Apr 18 21:57:14 PDT 1996';

SELECT 269,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_discount BETWEEN 0.01216811186405743 AND 0.023933446456747158;

SELECT 270,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem
WHERE  l_extendedprice BETWEEN 52974.57899132226 AND 70119.13535019066
       AND l_quantity BETWEEN 46.064526835866424 AND 55.66263575814256
       AND l_receiptdate BETWEEN 'Fri Jan 10 13:40:56 PST 1992' AND 'Sun Jan 19 13:40:56 PST 1992'
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 272,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.part,
       tpch.lineitem,
       tpch.orders
WHERE  ps_availqty BETWEEN 9109 AND 9147
       AND ps_supplycost BETWEEN 409.68935964203575 AND 605.3503148965875
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND p_partkey = ps_partkey;

SELECT 274,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem
WHERE  ps_supplycost BETWEEN 204.24917726653322 AND 209.67469741038593
       AND l_extendedprice BETWEEN 20676.33722869721 AND 21109.070314787175
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 279,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem
WHERE  l_commitdate BETWEEN 'Fri Jan 22 00:52:07 PST 1993' AND 'Fri Feb 05 00:52:07 PST 1993'
       AND l_discount BETWEEN 0.07432087689432272 AND 0.08680461637733822
       AND l_tax BETWEEN 0.0558519136801173 AND 0.06964268151128664
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 284,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  l_receiptdate BETWEEN 'Mon Jul 11 22:40:56 PDT 1994' AND 'Mon Jul 31 22:40:56 PDT 1995'
       AND l_tax BETWEEN 0.06511187226474754 AND 0.07730064171780554
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND c_custkey = o_custkey;

SELECT 286,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  ps_availqty BETWEEN 2832 AND 4588
       AND ps_supplycost BETWEEN 211.90703941751514 AND 410.2431624071234
       AND l_receiptdate BETWEEN 'Sun Mar 01 09:09:49 PST 1998' AND 'Wed Mar 18 09:09:49 PST 1998'
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND c_custkey = o_custkey;

SELECT 287,
       COUNT(*)
FROM   tpch.nation,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  c_acctbal BETWEEN -559.8278259312665 AND 1606.8700961951813
       AND o_orderkey = l_orderkey
       AND c_custkey = o_custkey
       AND n_nationkey = c_nationkey;

SELECT 288,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_discount BETWEEN 0.06056335881478199 AND 0.07288247267726085
       AND l_quantity BETWEEN 26.97856846205527 AND 27.122150743151632;

SELECT 289,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_quantity BETWEEN 11.337374243195312 AND 17.410912697704465
       AND l_commitdate BETWEEN 'Wed Jun 15 21:21:42 PDT 1994' AND 'Tue Apr 04 21:21:42 PDT 1995';

SELECT 292,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_commitdate BETWEEN 'Sat Sep 14 13:37:47 PDT 1996' AND 'Fri Oct 04 13:37:47 PDT 1996'
       AND l_tax BETWEEN 0.056270118607508864 AND 0.06832089772341621;

SELECT 293,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_discount BETWEEN 0.031374891672292536 AND 0.0415377374213934
       AND l_shipdate BETWEEN 'Mon Jul 21 08:21:34 PDT 1997' AND 'Mon Aug 24 08:21:34 PDT 1998'
       AND l_extendedprice BETWEEN 82093.22287008258 AND 82350.62009683302;

SELECT 294,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_quantity BETWEEN 11.083664612427638 AND 16.911716439833775
       AND l_tax BETWEEN 0.03535341328245775 AND 0.04775187547189291
       AND l_receiptdate BETWEEN 'Thu Aug 27 23:39:36 PDT 1992' AND 'Wed Sep 09 23:39:36 PDT 1992';

SELECT 296,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem
WHERE  ps_availqty BETWEEN 9955 AND 11184
       AND l_receiptdate BETWEEN 'Tue Mar 10 12:07:50 PST 1998' AND 'Wed Dec 16 12:07:50 PST 1998'
       AND l_tax BETWEEN 0.07810416526621412 AND 0.08910786431604856
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 297,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_tax BETWEEN 0.02764599719643102 AND 0.04038600795497383
       AND l_commitdate BETWEEN 'Fri Apr 26 21:58:04 PDT 1996' AND 'Mon Mar 31 20:58:04 PST 1997';

SELECT 299,
       COUNT(*)
FROM   tpch.customer,
       tpch.orders
WHERE  o_totalprice BETWEEN 258507.55638880268 AND 359145.14704425284
       AND c_custkey = o_custkey;

SELECT 301,
       COUNT(*)
FROM   tpch.orders
WHERE  o_orderdate BETWEEN 'Tue Sep 30 03:39:30 PDT 1997' AND 'Thu Oct 15 03:39:30 PDT 1998'
       AND o_totalprice BETWEEN 63080.89040179305 AND 63688.927218812954;

SELECT 304,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  l_discount BETWEEN 0.07045942642411705 AND 0.08842602477416042
       AND o_orderkey = l_orderkey;

SELECT 306,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_discount BETWEEN 0.09443235914228801 AND 0.10801422375234973
       AND l_shipdate BETWEEN 'Tue Jan 14 21:35:51 PST 1997' AND 'Tue Jan 20 21:35:51 PST 1998';

SELECT 308,
       COUNT(*)
FROM   tpch.customer
WHERE  c_acctbal BETWEEN 312.82135226880155 AND 1442.9677544071765;

SELECT 317,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  l_commitdate BETWEEN 'Sun Aug 01 08:11:20 PDT 1993' AND 'Wed Nov 09 07:11:20 PST 1994'
       AND l_extendedprice BETWEEN 65723.50758129585 AND 66425.11213337944
       AND l_discount BETWEEN 0.03703944541780762 AND 0.049217790731849086
       AND o_orderdate BETWEEN 'Thu Jun 30 08:47:30 PDT 1994' AND 'Sun Mar 05 07:47:30 PST 1995'
       AND o_totalprice BETWEEN 102003.63908810145 AND 159257.02513434074
       AND o_orderkey = l_orderkey
       AND c_custkey = o_custkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 319,
       COUNT(*)
FROM   tpch.supplier,
       tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  s_acctbal BETWEEN 3024.6113378949194 AND 3038.5894137290247
       AND ps_supplycost BETWEEN 693.3614334428524 AND 834.3300051073338
       AND ps_availqty BETWEEN 4058 AND 5310
       AND o_totalprice BETWEEN 47746.1195349966 AND 53289.839459109295
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND o_orderkey = l_orderkey
       AND s_suppkey = ps_suppkey;

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

SELECT 321,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_shipdate BETWEEN 'Sat Oct 18 17:50:27 PDT 1997' AND 'Thu Aug 13 17:50:27 PDT 1998';

SELECT 323,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem
WHERE  ps_supplycost BETWEEN 420.3814998300111 AND 592.9637009837609
       AND ps_availqty BETWEEN 1388 AND 2604
       AND l_discount BETWEEN 0.06540345960983467 AND 0.08479255297663582
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 326,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  l_quantity BETWEEN 10.146153800606317 AND 19.474468615602497
       AND l_discount BETWEEN 0.02528449124236334 AND 0.03711682509859327
       AND o_orderkey = l_orderkey;

SELECT 335,
       COUNT(*)
FROM   tpch.nation,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  c_acctbal BETWEEN 495.22398160893545 AND 524.9737231077037
       AND l_receiptdate BETWEEN 'Fri Jan 07 17:58:27 PST 1994' AND 'Sun Jan 09 17:58:27 PST 1994'
       AND o_orderkey = l_orderkey
       AND c_custkey = o_custkey
       AND n_nationkey = c_nationkey;

SELECT 336,
       COUNT(*)
FROM   tpch.orders
WHERE  o_totalprice BETWEEN 42463.86187560678 AND 126391.41898786716;

SELECT 338,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_quantity BETWEEN 16.870090688648162 AND 22.6921534733475
       AND l_commitdate BETWEEN 'Mon Dec 26 07:43:52 PST 1994' AND 'Wed Jan 10 07:43:52 PST 1996';

SELECT 340,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_discount BETWEEN 0.06007189104601875 AND 0.07696281143968974;

SELECT 343,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_tax BETWEEN 0.05365973424643771 AND 0.06674818715447284
       AND l_receiptdate BETWEEN 'Thu Apr 15 09:36:22 PDT 1993' AND 'Tue Mar 29 08:36:22 PST 1994';

SELECT 345,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  ps_supplycost BETWEEN 226.00519842575503 AND 417.1478986963226
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 347,
       COUNT(*)
FROM   tpch.customer
WHERE  c_acctbal BETWEEN 6494.78495527637 AND 6576.996392510281;

SELECT 349,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_shipdate BETWEEN 'Wed Oct 21 21:44:29 PDT 1992' AND 'Tue Nov 10 20:44:29 PST 1992';

SELECT 352,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.part,
       tpch.lineitem,
       tpch.orders
WHERE  p_retailprice BETWEEN 1791.1232870159545 AND 2000.1831950425428
       AND p_size BETWEEN 37 AND 43
       AND o_orderdate BETWEEN 'Mon Sep 26 19:50:29 PDT 1994' AND 'Mon Sep 26 19:50:29 PDT 1994'
       AND o_totalprice BETWEEN 297163.3644814657 AND 301624.7578066849
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND o_orderkey = l_orderkey
       AND p_partkey = ps_partkey;

SELECT 353,
       COUNT(*)
FROM   tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  l_extendedprice BETWEEN 19366.633175295377 AND 33981.556572542424
       AND l_tax BETWEEN 0.00490138596486621 AND 0.020120968892820935
       AND o_orderkey = l_orderkey
       AND c_custkey = o_custkey;

SELECT 354,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_discount BETWEEN 0.01929043373207571 AND 0.035499977263507496
       AND l_extendedprice BETWEEN 14105.69770597044 AND 29396.13317291627;

SELECT 356,
       COUNT(*)
FROM   tpch.supplier,
       tpch.nation,
       tpch.partsupp,
       tpch.part
WHERE  s_acctbal BETWEEN -742.6118789548984 AND 919.5398461858072
       AND ps_supplycost BETWEEN 459.932312530632 AND 460.97903855024805
       AND ps_availqty BETWEEN 6654 AND 7763
       AND s_suppkey = ps_suppkey
       AND p_partkey = ps_partkey
       AND n_nationkey = s_nationkey;

SELECT 362,
       COUNT(*)
FROM   tpch.supplier,
       tpch.nation,
       tpch.partsupp,
       tpch.lineitem
WHERE  ps_availqty BETWEEN 231 AND 1403
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND s_suppkey = ps_suppkey
       AND n_nationkey = s_nationkey;

SELECT 363,
       COUNT(*)
FROM   tpch.partsupp
WHERE  ps_availqty BETWEEN 2861 AND 2936
       AND ps_supplycost BETWEEN 653.8172132017106 AND 814.7820362574486;

SELECT 373,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  c_acctbal BETWEEN 4834.424620355739 AND 6201.481923462319
       AND o_totalprice BETWEEN 162172.2265012656 AND 226853.00806842186
       AND o_orderdate BETWEEN 'Fri Feb 11 00:51:36 PST 1994' AND 'Tue Feb 22 00:51:36 PST 1994'
       AND c_custkey = o_custkey
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 377,
       COUNT(*)
FROM   tpch.supplier,
       tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  s_acctbal BETWEEN 3024.6113378949194 AND 3038.5894137290247
       AND ps_supplycost BETWEEN 693.3614334428524 AND 834.3300051073338
       AND ps_availqty BETWEEN 4058 AND 5310
       AND o_totalprice BETWEEN 47746.1195349966 AND 53289.839459109295
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND o_orderkey = l_orderkey
       AND s_suppkey = ps_suppkey;

SELECT 381,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.part
WHERE  p_size BETWEEN 30 AND 35
       AND p_retailprice BETWEEN 2087.6441738108497 AND 2221.1038477240363
       AND p_partkey = ps_partkey;

SELECT 382,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  ps_availqty BETWEEN 838 AND 1905
       AND c_acctbal BETWEEN 6431.504042582555 AND 6485.537122579889
       AND o_orderkey = l_orderkey
       AND c_custkey = o_custkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 384,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_receiptdate BETWEEN 'Sat Jul 25 19:24:38 PDT 1992' AND 'Sat Jul 25 19:24:38 PDT 1992';

SELECT 388,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_extendedprice BETWEEN 55643.216698798744 AND 75838.24946490163
       AND l_commitdate BETWEEN 'Tue Dec 23 17:32:44 PST 1997' AND 'Wed Mar 10 17:32:44 PST 1999'
       AND l_shipdate BETWEEN 'Mon Dec 22 04:38:45 PST 1997' AND 'Tue Feb 02 04:38:45 PST 1999';

SELECT 393,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_quantity BETWEEN 33.547077857767356 AND 34.004171601441534
       AND l_shipdate BETWEEN 'Mon Feb 23 20:26:12 PST 1998' AND 'Thu Feb 26 20:26:12 PST 1998'
       AND l_tax BETWEEN 0.01871520158095227 AND 0.0282032306708488;

SELECT 395,
       COUNT(*)
FROM   tpch.customer
WHERE  c_acctbal BETWEEN 503.67370200488176 AND 561.1916001103834;

SELECT 402,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  ps_availqty BETWEEN 838 AND 1905
       AND c_acctbal BETWEEN 6431.504042582555 AND 6485.537122579889
       AND o_orderkey = l_orderkey
       AND c_custkey = o_custkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 409,
       COUNT(*)
FROM   tpch.orders
WHERE  o_orderdate BETWEEN 'Tue Nov 19 15:32:42 PST 1996' AND 'Mon Feb 09 15:32:42 PST 1998';

SELECT 410,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.part
WHERE  ps_supplycost BETWEEN 730.5229863228158 AND 736.3722511325504
       AND p_partkey = ps_partkey;

SELECT 423,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_discount BETWEEN 0.008539934804225768 AND 0.021620984875006383
       AND l_receiptdate BETWEEN 'Mon Jan 02 18:57:13 PST 1995' AND 'Wed Mar 20 18:57:13 PST 1996';

SELECT 426,
       COUNT(*)
FROM   tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  l_extendedprice BETWEEN 38603.191231316756 AND 39072.57958922852
       AND c_custkey = o_custkey
       AND o_orderkey = l_orderkey;

SELECT 428,
       COUNT(*)
FROM   tpch.supplier,
       tpch.partsupp,
       tpch.part,
       tpch.lineitem
WHERE  l_quantity BETWEEN 2.3479553984081174 AND 10.601453687029533
       AND l_commitdate BETWEEN 'Tue Sep 08 23:08:09 PDT 1992' AND 'Tue Jul 20 23:08:09 PDT 1993'
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND p_partkey = ps_partkey
       AND s_suppkey = ps_suppkey;

SELECT 431,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_quantity BETWEEN 37.05799160857271 AND 42.9896659990098;

SELECT 432,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_receiptdate BETWEEN 'Sat Jul 25 19:24:38 PDT 1992' AND 'Sat Jul 25 19:24:38 PDT 1992';

SELECT 438,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  o_orderdate BETWEEN 'Fri Sep 02 02:56:52 PDT 1994' AND 'Mon Aug 14 02:56:52 PDT 1995'
       AND o_totalprice BETWEEN 172053.2325744397 AND 246632.4490773052
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 442,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_receiptdate BETWEEN 'Sat Jul 25 19:24:38 PDT 1992' AND 'Sat Jul 25 19:24:38 PDT 1992';

SELECT 445,
       COUNT(*)
FROM   tpch.supplier,
       tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  s_acctbal BETWEEN 9168.350791516117 AND 11266.72133619578
       AND ps_availqty BETWEEN 6952 AND 8133
       AND o_orderdate BETWEEN 'Thu Dec 16 23:33:33 PST 1993' AND 'Mon Feb 06 23:33:33 PST 1995'
       AND o_totalprice BETWEEN 29808.550402714165 AND 96138.73364216473
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND s_suppkey = ps_suppkey
       AND o_orderkey = l_orderkey;

SELECT 460,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_commitdate BETWEEN 'Thu Aug 12 15:29:32 PDT 1993' AND 'Tue Oct 04 15:29:32 PDT 1994';

SELECT 468,
       COUNT(*)
FROM   tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  c_acctbal BETWEEN -68.54598047371599 AND 1984.226691843602
       AND l_tax BETWEEN 0.0705442474556441 AND 0.08480066913521951
       AND o_orderkey = l_orderkey
       AND c_custkey = o_custkey;

SELECT 475,
       COUNT(*)
FROM   tpch.partsupp
WHERE  ps_availqty BETWEEN 1988 AND 2006;

SELECT 478,
       COUNT(*)
FROM   tpch.customer
WHERE  c_acctbal BETWEEN 4265.0888755771875 AND 4266.092774617749;

SELECT 485,
       COUNT(*)
FROM   tpch.partsupp
WHERE  ps_supplycost BETWEEN 310.88071250949133 AND 476.37971188008987
       AND ps_availqty BETWEEN 3747 AND 3793;

SELECT 494,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  ps_availqty BETWEEN 4251 AND 4291
       AND o_orderdate BETWEEN 'Sat Oct 02 19:51:18 PDT 1993' AND 'Mon Oct 31 18:51:18 PST 1994'
       AND o_totalprice BETWEEN 146822.8489020799 AND 225118.2605626132
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 498,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_tax BETWEEN 0.06773241887881469 AND 0.07573556116928813
       AND l_discount BETWEEN 0.07786949287250791 AND 0.09113192314000346;

SELECT 512,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  o_totalprice BETWEEN 16603.41553699638 AND 102226.13520319841
       AND o_orderdate BETWEEN 'Sat Apr 04 01:27:41 PST 1998' AND 'Sun Apr 26 02:27:41 PDT 1998'
       AND o_orderkey = l_orderkey;

SELECT 513,
       COUNT(*)
FROM   tpch.orders
WHERE  o_totalprice BETWEEN 42463.86187560678 AND 126391.41898786716;

SELECT 519,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_receiptdate BETWEEN 'Thu Aug 03 07:27:11 PDT 1995' AND 'Fri Aug 09 07:27:11 PDT 1996'
       AND l_discount BETWEEN 0.03692292890241279 AND 0.04976639203757548;

SELECT 520,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.part,
       tpch.lineitem,
       tpch.orders
WHERE  ps_availqty BETWEEN 4068 AND 5296
       AND ps_supplycost BETWEEN 331.905351166803 AND 524.6317436732967
       AND p_retailprice BETWEEN 1371.8101931586873 AND 1537.8622120719838
       AND l_quantity BETWEEN 7.35688970824695 AND 13.311235058971361
       AND p_partkey = ps_partkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND o_orderkey = l_orderkey;

SELECT 523,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_tax BETWEEN 0.00975969681828734 AND 0.023846437264969295;

SELECT 524,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  l_commitdate BETWEEN 'Thu Mar 18 23:02:01 PST 1993' AND 'Mon Jan 17 23:02:01 PST 1994'
       AND l_quantity BETWEEN 41.18213025470975 AND 50.28155048405537
       AND l_shipdate BETWEEN 'Thu Jul 08 07:00:39 PDT 1993' AND 'Sun Aug 07 07:00:39 PDT 1994'
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 529,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_quantity BETWEEN 14.800052833053496 AND 15.127610635807432
       AND l_receiptdate BETWEEN 'Sat Jan 04 02:43:21 PST 1997' AND 'Fri Nov 21 02:43:21 PST 1997'
       AND l_tax BETWEEN 0.019978676608192282 AND 0.029285899575620347;

SELECT 532,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_tax BETWEEN 0.05740179354588497 AND 0.06646325104947436;

SELECT 537,
       COUNT(*)
FROM   tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  o_totalprice BETWEEN 449972.7236302865 AND 453935.43212330574
       AND c_custkey = o_custkey
       AND o_orderkey = l_orderkey;

SELECT 538,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.part,
       tpch.lineitem,
       tpch.orders
WHERE  p_retailprice BETWEEN 1791.1232870159545 AND 2000.1831950425428
       AND p_size BETWEEN 37 AND 43
       AND o_orderdate BETWEEN 'Mon Sep 26 19:50:29 PDT 1994' AND 'Mon Sep 26 19:50:29 PDT 1994'
       AND o_totalprice BETWEEN 297163.3644814657 AND 301624.7578066849
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND o_orderkey = l_orderkey
       AND p_partkey = ps_partkey;

SELECT 545,
       COUNT(*)
FROM   tpch.orders
WHERE  o_totalprice BETWEEN 256306.63921089325 AND 329572.2738984181
       AND o_orderdate BETWEEN 'Thu Jan 09 20:43:50 PST 1997' AND 'Sat May 02 21:43:50 PDT 1998';

SELECT 546,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.part,
       tpch.lineitem,
       tpch.orders
WHERE  ps_availqty BETWEEN 3101 AND 5056
       AND ps_supplycost BETWEEN 878.4347356615707 AND 879.956720871681
       AND p_retailprice BETWEEN 1772.3938841206937 AND 1952.2354714720968
       AND p_size BETWEEN 10 AND 15
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND p_partkey = ps_partkey;

SELECT 549,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_quantity BETWEEN 2.3479553984081174 AND 10.601453687029533
       AND l_commitdate BETWEEN 'Tue Sep 08 23:08:09 PDT 1992' AND 'Tue Jul 20 23:08:09 PDT 1993';

SELECT 553,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  o_orderdate BETWEEN 'Fri Mar 11 12:37:30 PST 1994' AND 'Fri Mar 18 12:37:30 PST 1994'
       AND o_totalprice BETWEEN 299055.2071392352 AND 301841.18832094065
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 560,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.part,
       tpch.lineitem,
       tpch.orders
WHERE  p_retailprice BETWEEN 1182.7314360244366 AND 1192.947790967495
       AND l_receiptdate BETWEEN 'Sun Sep 20 11:05:11 PDT 1998' AND 'Mon Oct 05 11:05:11 PDT 1998'
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND p_partkey = ps_partkey
       AND o_orderkey = l_orderkey;

SELECT 562,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_quantity BETWEEN 12.449858085556823 AND 17.361094215355656
       AND l_discount BETWEEN 0.04039881970289373 AND 0.05728105492022001
       AND l_commitdate BETWEEN 'Sat Aug 12 20:47:37 PDT 1995' AND 'Sat Jun 08 20:47:37 PDT 1996';

SELECT 563,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  ps_supplycost BETWEEN 780.1494324314234 AND 928.2523591583858
       AND ps_availqty BETWEEN 4942 AND 6927
       AND l_quantity BETWEEN 33.82122036147058 AND 34.271922780076785
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 564,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  o_totalprice BETWEEN 15516.222987710396 AND 81891.45384614971
       AND o_orderkey = l_orderkey;

SELECT 565,
       COUNT(*)
FROM   tpch.orders
WHERE  o_orderdate BETWEEN 'Tue May 12 00:11:45 PDT 1992' AND 'Mon Jun 01 00:11:45 PDT 1992'
       AND o_totalprice BETWEEN 151738.82293264213 AND 240597.6420256541;

SELECT 569,
       COUNT(*)
FROM   tpch.partsupp
WHERE  ps_supplycost BETWEEN 678.6391635613542 AND 840.9142807504037;

SELECT 574,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.part,
       tpch.lineitem,
       tpch.orders
WHERE  ps_supplycost BETWEEN 173.44351500719142 AND 353.29380340229363
       AND ps_availqty BETWEEN 2139 AND 3424
       AND p_retailprice BETWEEN 1841.1190915057955 AND 1999.8992992758258
       AND l_tax BETWEEN 0.008340179746847705 AND 0.019634139215712824
       AND l_discount BETWEEN 0.012356322761971295 AND 0.028260995713682457
       AND p_partkey = ps_partkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND o_orderkey = l_orderkey;

SELECT 577,
       COUNT(*)
FROM   tpch.partsupp
WHERE  ps_availqty BETWEEN 6923 AND 8732;

SELECT 580,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  l_extendedprice BETWEEN 5799.932871571947 AND 6129.922067775129
       AND o_orderdate BETWEEN 'Fri Jan 27 03:28:20 PST 1995' AND 'Sat Jan 27 03:28:20 PST 1996'
       AND o_totalprice BETWEEN 151345.9506308513 AND 216877.36327113307
       AND o_orderkey = l_orderkey;

SELECT 583,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_extendedprice BETWEEN 15847.463439757354 AND 16482.404385961825
       AND l_quantity BETWEEN 14.647334965856341 AND 15.024782268992618;

SELECT 589,
       COUNT(*)
FROM   tpch.partsupp
WHERE  ps_availqty BETWEEN 3924 AND 5318
       AND ps_supplycost BETWEEN 254.7599316510568 AND 261.6019511310062;

SELECT 590,
       COUNT(*)
FROM   tpch.partsupp
WHERE  ps_supplycost BETWEEN 627.4878351266818 AND 728.3950492764249
       AND ps_availqty BETWEEN 1368 AND 2939;

SELECT 591,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_tax BETWEEN 0.0778078035142845 AND 0.08950372289741823
       AND l_shipdate BETWEEN 'Sun Feb 02 12:08:02 PST 1997' AND 'Wed Jun 10 13:08:02 PDT 1998';

SELECT 592,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.part,
       tpch.lineitem,
       tpch.orders
WHERE  ps_availqty BETWEEN 9109 AND 9147
       AND ps_supplycost BETWEEN 409.68935964203575 AND 605.3503148965875
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND p_partkey = ps_partkey;

SELECT 593,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_extendedprice BETWEEN 101588.55311358026 AND 121773.30069605698;

SELECT 598,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_tax BETWEEN 0.057269242474584925 AND 0.06775597723578122
       AND l_discount BETWEEN 0.07560528971913694 AND 0.08912902128421672
       AND l_commitdate BETWEEN 'Wed Jun 25 06:51:12 PDT 1997' AND 'Fri Jun 27 06:51:12 PDT 1997';

SELECT 599,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  o_orderdate BETWEEN 'Sat Jan 28 10:10:10 PST 1995' AND 'Sat Nov 11 10:10:10 PST 1995'
       AND o_totalprice BETWEEN 116304.71307766865 AND 213640.26879261085
       AND o_orderkey = l_orderkey;

SELECT 602,
       COUNT(*)
FROM   tpch.supplier,
       tpch.partsupp,
       tpch.part,
       tpch.lineitem
WHERE  l_receiptdate BETWEEN 'Sat Oct 23 16:51:21 PDT 1993' AND 'Mon Nov 01 15:51:21 PST 1993'
       AND p_partkey = ps_partkey
       AND s_suppkey = ps_suppkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 605,
       COUNT(*)
FROM   tpch.customer
WHERE  c_acctbal BETWEEN 503.67370200488176 AND 561.1916001103834;

SELECT 606,
       COUNT(*)
FROM   tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  c_acctbal BETWEEN 6563.076976922762 AND 6622.60975498421
       AND o_orderdate BETWEEN 'Sat Mar 06 05:36:10 PST 1993' AND 'Tue Mar 08 05:36:10 PST 1994'
       AND o_totalprice BETWEEN 82983.25683230953 AND 150157.7509317818
       AND o_orderkey = l_orderkey
       AND c_custkey = o_custkey;

SELECT 610,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_tax BETWEEN 0.049168944323960115 AND 0.0636291210033138
       AND l_quantity BETWEEN 37.675691870329636 AND 46.620490824134805;

SELECT 613,
       COUNT(*)
FROM   tpch.supplier,
       tpch.partsupp,
       tpch.part,
       tpch.lineitem
WHERE  s_acctbal BETWEEN -930.6459391225465 AND -900.3640438332797
       AND ps_availqty BETWEEN 5695 AND 5732
       AND ps_supplycost BETWEEN 193.69080923147104 AND 201.91171025428355
       AND l_commitdate BETWEEN 'Mon Apr 17 03:36:40 PDT 1995' AND 'Sat Jan 06 02:36:40 PST 1996'
       AND p_partkey = ps_partkey
       AND s_suppkey = ps_suppkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 619,
       COUNT(*)
FROM   tpch.partsupp
WHERE  ps_supplycost BETWEEN 310.88071250949133 AND 476.37971188008987
       AND ps_availqty BETWEEN 3747 AND 3793;

SELECT 620,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_quantity BETWEEN 12.449858085556823 AND 17.361094215355656
       AND l_discount BETWEEN 0.04039881970289373 AND 0.05728105492022001
       AND l_commitdate BETWEEN 'Sat Aug 12 20:47:37 PDT 1995' AND 'Sat Jun 08 20:47:37 PDT 1996';

SELECT 625,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_extendedprice BETWEEN 55643.216698798744 AND 75838.24946490163
       AND l_commitdate BETWEEN 'Tue Dec 23 17:32:44 PST 1997' AND 'Wed Mar 10 17:32:44 PST 1999'
       AND l_shipdate BETWEEN 'Mon Dec 22 04:38:45 PST 1997' AND 'Tue Feb 02 04:38:45 PST 1999';

SELECT 626,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  l_discount BETWEEN 0.09755056347571728 AND 0.11046799695880877
       AND o_totalprice BETWEEN 230879.12916992704 AND 333315.3073925446
       AND o_orderdate BETWEEN 'Wed Jan 26 11:27:15 PST 1994' AND 'Sun Sep 25 12:27:15 PDT 1994'
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND c_custkey = o_custkey;

SELECT 635,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_discount BETWEEN 0.04362610549727319 AND 0.06302995723643443;

SELECT 644,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  o_orderdate BETWEEN 'Sun Apr 23 07:23:19 PDT 1995' AND 'Mon May 08 07:23:19 PDT 1995'
       AND o_totalprice BETWEEN 424696.6725527435 AND 427493.28640145843
       AND o_orderkey = l_orderkey;

SELECT 651,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.part,
       tpch.lineitem,
       tpch.orders
WHERE  ps_availqty BETWEEN 3101 AND 5056
       AND ps_supplycost BETWEEN 878.4347356615707 AND 879.956720871681
       AND p_retailprice BETWEEN 1772.3938841206937 AND 1952.2354714720968
       AND p_size BETWEEN 10 AND 15
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND p_partkey = ps_partkey;

SELECT 669,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_discount BETWEEN 0.020679258364533815 AND 0.031159014391135626;

SELECT 676,
       COUNT(*)
FROM   tpch.part
WHERE  p_retailprice BETWEEN 1012.9358143247314 AND 1252.0589225503832;

SELECT 678,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_tax BETWEEN 0.008714843053446098 AND 0.020790418189023836
       AND l_discount BETWEEN 0.03241252194277052 AND 0.04358939603465213;

SELECT 680,
       COUNT(*)
FROM   tpch.customer
WHERE  c_acctbal BETWEEN 6494.78495527637 AND 6576.996392510281;

SELECT 682,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  l_receiptdate BETWEEN 'Thu Oct 31 02:39:50 PST 1996' AND 'Sat Nov 09 02:39:50 PST 1996'
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 693,
       COUNT(*)
FROM   tpch.orders
WHERE  o_orderdate BETWEEN 'Mon Dec 11 12:36:28 PST 1995' AND 'Sun Feb 16 12:36:28 PST 1997'
       AND o_totalprice BETWEEN 254937.79722936152 AND 260087.9047894202;

SELECT 694,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem
WHERE  ps_supplycost BETWEEN 204.24917726653322 AND 209.67469741038593
       AND l_extendedprice BETWEEN 20676.33722869721 AND 21109.070314787175
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 701,
       COUNT(*)
FROM   tpch.supplier,
       tpch.partsupp,
       tpch.part,
       tpch.lineitem
WHERE  s_acctbal BETWEEN 3299.062197700375 AND 5112.897138179407
       AND p_size BETWEEN 3 AND 11
       AND p_partkey = ps_partkey
       AND s_suppkey = ps_suppkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 702,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  ps_availqty BETWEEN 1498 AND 3286
       AND ps_supplycost BETWEEN 333.514430649815 AND 335.14776670753525
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 706,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_shipdate BETWEEN 'Sat Apr 16 17:11:52 PDT 1994' AND 'Sat May 07 17:11:52 PDT 1994'
       AND l_extendedprice BETWEEN 22710.507820744002 AND 22982.023544148804;

SELECT 708,
       COUNT(*)
FROM   tpch.nation,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  c_acctbal BETWEEN 9411.061842898474 AND 9460.345503464692
       AND c_custkey = o_custkey
       AND o_orderkey = l_orderkey
       AND n_nationkey = c_nationkey;

SELECT 733,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_commitdate BETWEEN 'Sat Jun 06 04:20:43 PDT 1992' AND 'Thu Jun 11 04:20:43 PDT 1992'
       AND l_extendedprice BETWEEN 67986.18183079733 AND 68762.79551058913
       AND l_tax BETWEEN 0.02236616240876386 AND 0.03195029958369945;

SELECT 750,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_shipdate BETWEEN 'Tue Jul 05 05:44:57 PDT 1994' AND 'Tue Jun 06 05:44:57 PDT 1995'
       AND l_extendedprice BETWEEN 63742.50528968195 AND 82679.5516992607;

SELECT 751,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_quantity BETWEEN 36.88008620355679 AND 37.35492505916053
       AND l_commitdate BETWEEN 'Wed Aug 02 10:47:01 PDT 1995' AND 'Tue Apr 09 10:47:01 PDT 1996'
       AND l_tax BETWEEN 0.06159561224773003 AND 0.07407029566056333;

SELECT 754,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  o_orderdate BETWEEN 'Sat Dec 21 10:45:12 PST 1996' AND 'Sun Nov 02 10:45:12 PST 1997'
       AND o_totalprice BETWEEN 260995.83835022585 AND 349119.3359266861
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND c_custkey = o_custkey;

SELECT 759,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_shipdate BETWEEN 'Tue Jul 01 02:08:02 PDT 1997' AND 'Wed Jul 16 02:08:02 PDT 1997'
       AND l_commitdate BETWEEN 'Sun May 11 01:12:03 PDT 1997' AND 'Fri Aug 07 01:12:03 PDT 1998'
       AND l_extendedprice BETWEEN 101545.56508482285 AND 119167.05443846797;

SELECT 769,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  l_shipdate BETWEEN 'Fri Mar 24 11:49:12 PST 1995' AND 'Mon Apr 17 12:49:12 PDT 1995'
       AND o_orderkey = l_orderkey;

SELECT 779,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_tax BETWEEN 0.049168944323960115 AND 0.0636291210033138
       AND l_quantity BETWEEN 37.675691870329636 AND 46.620490824134805;

SELECT 782,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_commitdate BETWEEN 'Sat Jun 06 04:20:43 PDT 1992' AND 'Thu Jun 11 04:20:43 PDT 1992'
       AND l_extendedprice BETWEEN 67986.18183079733 AND 68762.79551058913
       AND l_tax BETWEEN 0.02236616240876386 AND 0.03195029958369945;

SELECT 797,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_commitdate BETWEEN 'Thu Jun 05 10:23:40 PDT 1997' AND 'Mon Feb 23 09:23:40 PST 1998'
       AND l_quantity BETWEEN 27.794540546567198 AND 34.29439890493532;

SELECT 803,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_receiptdate BETWEEN 'Sat Sep 28 09:16:57 PDT 1996' AND 'Tue Oct 22 09:16:57 PDT 1996'
       AND l_tax BETWEEN 0.05468807671943223 AND 0.06587897961237348;

SELECT 809,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  o_totalprice BETWEEN 303650.8468007094 AND 360830.8906457936
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 812,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  l_receiptdate BETWEEN 'Tue Mar 16 17:00:07 PST 1993' AND 'Fri Apr 09 18:00:07 PDT 1993'
       AND l_quantity BETWEEN 42.724387119566686 AND 43.197552181888504
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND c_custkey = o_custkey;

SELECT 816,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.part,
       tpch.lineitem,
       tpch.orders
WHERE  ps_availqty BETWEEN 3101 AND 5056
       AND ps_supplycost BETWEEN 878.4347356615707 AND 879.956720871681
       AND p_retailprice BETWEEN 1772.3938841206937 AND 1952.2354714720968
       AND p_size BETWEEN 10 AND 15
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND p_partkey = ps_partkey;

SELECT 823,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_extendedprice BETWEEN 23642.18073334687 AND 23921.770407957025
       AND l_shipdate BETWEEN 'Wed Feb 17 21:52:06 PST 1993' AND 'Tue Apr 26 22:52:06 PDT 1994'
       AND l_discount BETWEEN 0.014303293103721016 AND 0.0289020312254759;

SELECT 825,
       COUNT(*)
FROM   tpch.supplier,
       tpch.nation,
       tpch.customer,
       tpch.orders
WHERE  c_acctbal BETWEEN 4500.873227707883 AND 6351.645136741093
       AND o_orderdate BETWEEN 'Fri Nov 05 07:39:06 PST 1993' AND 'Sat Nov 20 07:39:06 PST 1993'
       AND c_custkey = o_custkey
       AND n_nationkey = c_nationkey
       AND n_nationkey = s_nationkey;

SELECT 833,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem
WHERE  ps_supplycost BETWEEN 108.63912460787023 AND 112.84715298334459
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 836,
       COUNT(*)
FROM   tpch.customer,
       tpch.orders
WHERE  c_acctbal BETWEEN 7141.205698422944 AND 8516.065401365024
       AND c_custkey = o_custkey;

SELECT 847,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_commitdate BETWEEN 'Tue Sep 02 11:42:33 PDT 1997' AND 'Tue Sep 09 11:42:33 PDT 1997';

SELECT 849,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_extendedprice BETWEEN 49092.914175750906 AND 49900.36849508131
       AND l_commitdate BETWEEN 'Mon Apr 04 01:30:59 PDT 1994' AND 'Mon Mar 27 00:30:59 PST 1995';

SELECT 853,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  ps_availqty BETWEEN 6330 AND 8077
       AND ps_supplycost BETWEEN 134.977561578446 AND 249.63062969273741
       AND o_totalprice BETWEEN 168973.6857482012 AND 249436.47866374816
       AND o_orderdate BETWEEN 'Mon Jan 16 09:02:35 PST 1995' AND 'Mon Jan 16 09:02:35 PST 1995'
       AND o_orderkey = l_orderkey
       AND c_custkey = o_custkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 862,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_extendedprice BETWEEN 38373.93617735771 AND 38998.76722069536;

SELECT 863,
       COUNT(*)
FROM   tpch.partsupp
WHERE  ps_supplycost BETWEEN 536.0049767511973 AND 679.3916031397391
       AND ps_availqty BETWEEN 9159 AND 9191;

SELECT 875,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  l_quantity BETWEEN 40.94568173647773 AND 41.38803202364948
       AND o_totalprice BETWEEN 98927.207532111 AND 176261.45147683244
       AND o_orderdate BETWEEN 'Fri Jul 15 03:00:34 PDT 1994' AND 'Fri Mar 17 02:00:34 PST 1995'
       AND o_orderkey = l_orderkey;

SELECT 886,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_quantity BETWEEN 21.927108692842463 AND 22.151709279853684
       AND l_discount BETWEEN 0.04350024739618207 AND 0.06201302147865029;

SELECT 890,
       COUNT(*)
FROM   tpch.supplier,
       tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  s_acctbal BETWEEN 9020.259238496172 AND 9114.255758171981
       AND ps_supplycost BETWEEN 819.13497268515 AND 826.2378019677275
       AND ps_availqty BETWEEN 4002 AND 4073
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND o_orderkey = l_orderkey
       AND s_suppkey = ps_suppkey;

SELECT 897,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_discount BETWEEN 0.009566597641386133 AND 0.021633384803186058;

SELECT 903,
       COUNT(*)
FROM   tpch.orders
WHERE  o_totalprice BETWEEN 379184.61103240185 AND 454411.25105625007;

SELECT 917,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_commitdate BETWEEN 'Fri Nov 22 15:33:52 PST 1996' AND 'Mon Nov 25 15:33:52 PST 1996';

SELECT 924,
       COUNT(*)
FROM   tpch.customer,
       tpch.orders
WHERE  c_acctbal BETWEEN 2827.8060294038987 AND 4537.135052170672
       AND c_custkey = o_custkey;

SELECT 926,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_tax BETWEEN 1.1417632866243643E-4 AND 0.010151030695495365;

SELECT 931,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  o_orderdate BETWEEN 'Wed Nov 15 10:57:53 PST 1995' AND 'Mon Nov 11 10:57:53 PST 1996'
       AND o_totalprice BETWEEN 22855.643095777486 AND 95372.31888765944
       AND o_orderkey = l_orderkey;

SELECT 940,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_receiptdate BETWEEN 'Fri Apr 19 11:50:30 PDT 1996' AND 'Fri May 09 11:50:30 PDT 1997'
       AND l_tax BETWEEN 0.07426194499564115 AND 0.0876560494427383
       AND l_commitdate BETWEEN 'Thu Jan 18 08:07:00 PST 1996' AND 'Fri Jan 26 08:07:00 PST 1996';

SELECT 947,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  ps_availqty BETWEEN 5382 AND 6801
       AND ps_supplycost BETWEEN 269.21389806227813 AND 430.6072676973851
       AND l_tax BETWEEN 0.07287355640921774 AND 0.08359500127159673
       AND o_orderdate BETWEEN 'Mon May 22 19:54:49 PDT 1995' AND 'Thu Feb 08 18:54:49 PST 1996'
       AND o_totalprice BETWEEN 84596.37718816921 AND 188208.3650706844
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 958,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  ps_supplycost BETWEEN 950.926281604935 AND 960.091306941197
       AND ps_availqty BETWEEN 777 AND 875
       AND l_shipdate BETWEEN 'Wed Jul 15 22:26:41 PDT 1992' AND 'Fri May 28 22:26:41 PDT 1993'
       AND l_discount BETWEEN 0.06848268504343441 AND 0.08577124127557031
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 959,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_discount BETWEEN 0.020679258364533815 AND 0.031159014391135626;

SELECT 970,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_receiptdate BETWEEN 'Tue Dec 29 14:11:19 PST 1998' AND 'Fri Jan 01 14:11:19 PST 1999'
       AND l_extendedprice BETWEEN 56682.18033854847 AND 67816.73506451154;

SELECT 971,
       COUNT(*)
FROM   tpch.orders
WHERE  o_orderdate BETWEEN 'Mon Jul 10 10:45:53 PDT 1995' AND 'Fri Jul 28 10:45:53 PDT 1995'
       AND o_totalprice BETWEEN 457886.2804467981 AND 543800.3098217448;

SELECT 973,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_quantity BETWEEN 31.884363185588573 AND 32.28331787976867
       AND l_extendedprice BETWEEN 24595.513892142262 AND 37675.65336788587;

SELECT 979,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_extendedprice BETWEEN 81785.86622606753 AND 92524.24061846834
       AND l_shipdate BETWEEN 'Thu Jan 23 17:07:22 PST 1997' AND 'Tue Feb 04 17:07:22 PST 1997';

SELECT 980,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_commitdate BETWEEN 'Tue Apr 09 12:28:00 PDT 1996' AND 'Thu Apr 18 12:28:00 PDT 1996'
       AND l_discount BETWEEN 0.09522435794735352 AND 0.10625840244251666;

SELECT 982,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_extendedprice BETWEEN 15374.915268575309 AND 27259.681907970975
       AND l_discount BETWEEN 0.0063308680698899505 AND 0.02545897659433837;

SELECT 990,
       COUNT(*)
FROM   tpch.orders
WHERE  o_orderdate BETWEEN 'Tue May 12 00:11:45 PDT 1992' AND 'Mon Jun 01 00:11:45 PDT 1992'
       AND o_totalprice BETWEEN 151738.82293264213 AND 240597.6420256541;

SELECT 995,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_commitdate BETWEEN 'Wed Jan 18 07:18:09 PST 1995' AND 'Thu Jan 25 07:18:09 PST 1996'
       AND l_extendedprice BETWEEN 35132.22931912532 AND 53788.91783876711;

SELECT 1001,
       COUNT(*)
FROM   tpch.supplier,
       tpch.partsupp,
       tpch.lineitem
WHERE  ps_availqty BETWEEN 8661 AND 8688
       AND ps_supplycost BETWEEN 1.8193597584559442 AND 11.37671717525115
       AND l_discount BETWEEN 0.08278451834222422 AND 0.1006724348389656
       AND l_receiptdate BETWEEN 'Sun Apr 23 05:35:51 PDT 1995' AND 'Thu Jan 11 04:35:51 PST 1996'
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND s_suppkey = ps_suppkey;

SELECT 1010,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.part,
       tpch.lineitem,
       tpch.orders
WHERE  ps_availqty BETWEEN 3101 AND 5056
       AND ps_supplycost BETWEEN 878.4347356615707 AND 879.956720871681
       AND p_retailprice BETWEEN 1772.3938841206937 AND 1952.2354714720968
       AND p_size BETWEEN 10 AND 15
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND p_partkey = ps_partkey;

SELECT 1011,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.part,
       tpch.lineitem,
       tpch.orders
WHERE  p_size BETWEEN 26 AND 33
       AND o_totalprice BETWEEN 416287.9048999934 AND 420400.2249158094
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND p_partkey = ps_partkey
       AND o_orderkey = l_orderkey;

SELECT 1012,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_shipdate BETWEEN 'Tue Feb 02 22:14:02 PST 1993' AND 'Tue Feb 09 22:14:02 PST 1993'
       AND l_extendedprice BETWEEN 16890.78138876008 AND 28957.67527597976;

SELECT 1015,
       COUNT(*)
FROM   tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  c_acctbal BETWEEN 8059.913411901569 AND 9894.854621856914
       AND l_tax BETWEEN 0.05289892732445212 AND 0.0623852466301006
       AND l_discount BETWEEN 0.07856507303332372 AND 0.09489818990853022
       AND o_orderkey = l_orderkey
       AND c_custkey = o_custkey;

SELECT 1022,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_commitdate BETWEEN 'Sun Jan 11 20:15:03 PST 1998' AND 'Mon Feb 15 20:15:03 PST 1999'
       AND l_tax BETWEEN 0.05540154857564852 AND 0.06806450598302848;

SELECT 1024,
       COUNT(*)
FROM   tpch.orders
WHERE  o_totalprice BETWEEN 270566.07022043085 AND 370821.15675755293;

SELECT 1026,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_commitdate BETWEEN 'Sat Apr 08 16:13:16 PDT 1995' AND 'Sat Apr 15 16:13:16 PDT 1995';

SELECT 1028,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_tax BETWEEN 0.02651887513511066 AND 0.03689247696523512;

SELECT 1030,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  o_totalprice BETWEEN 199603.89595550718 AND 292160.07827685453
       AND o_orderdate BETWEEN 'Tue Mar 17 08:04:50 PST 1992' AND 'Mon Jan 25 08:04:50 PST 1993'
       AND o_orderkey = l_orderkey;

SELECT 1032,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  o_orderdate BETWEEN 'Tue Jun 28 17:29:02 PDT 1994' AND 'Sun Jul 23 17:29:02 PDT 1995'
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND c_custkey = o_custkey;

SELECT 1042,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_quantity BETWEEN 29.460649969950502 AND 35.48928551733505
       AND l_tax BETWEEN 0.0689032089603857 AND 0.08308276652524775
       AND l_shipdate BETWEEN 'Fri Oct 04 10:11:36 PDT 1996' AND 'Fri Sep 12 10:11:36 PDT 1997';

SELECT 1043,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_receiptdate BETWEEN 'Thu Jul 14 09:30:29 PDT 1994' AND 'Sun Apr 09 09:30:29 PDT 1995'
       AND l_extendedprice BETWEEN 101061.04857222732 AND 120548.47740028842
       AND l_discount BETWEEN 0.06727946056351825 AND 0.08079117575627513;

SELECT 1048,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_extendedprice BETWEEN 100175.32185982323 AND 112591.64596010136
       AND l_receiptdate BETWEEN 'Sat Mar 28 13:03:39 PST 1998' AND 'Wed Apr 15 14:03:39 PDT 1998';

SELECT 1054,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem
WHERE  l_tax BETWEEN 0.03363109233262634 AND 0.045403576277241946
       AND l_extendedprice BETWEEN 13327.263053338193 AND 24691.191828106406
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 1066,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_quantity BETWEEN 16.870090688648162 AND 22.6921534733475
       AND l_commitdate BETWEEN 'Mon Dec 26 07:43:52 PST 1994' AND 'Wed Jan 10 07:43:52 PST 1996';

SELECT 1069,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_extendedprice BETWEEN 52210.64372962707 AND 67024.63793921893
       AND l_discount BETWEEN 0.04339105154571613 AND 0.057561507744846
       AND l_tax BETWEEN 0.059006273347368125 AND 0.07248647659507333;

SELECT 1070,
       COUNT(*)
FROM   tpch.orders
WHERE  o_orderdate BETWEEN 'Thu May 14 11:08:10 PDT 1998' AND 'Sat Mar 06 10:08:10 PST 1999'
       AND o_totalprice BETWEEN 323347.4302297842 AND 323704.2805813229;

SELECT 1073,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  l_commitdate BETWEEN 'Sun Aug 01 08:11:20 PDT 1993' AND 'Wed Nov 09 07:11:20 PST 1994'
       AND l_extendedprice BETWEEN 65723.50758129585 AND 66425.11213337944
       AND l_discount BETWEEN 0.03703944541780762 AND 0.049217790731849086
       AND o_orderdate BETWEEN 'Thu Jun 30 08:47:30 PDT 1994' AND 'Sun Mar 05 07:47:30 PST 1995'
       AND o_totalprice BETWEEN 102003.63908810145 AND 159257.02513434074
       AND o_orderkey = l_orderkey
       AND c_custkey = o_custkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 1077,
       COUNT(*)
FROM   tpch.partsupp
WHERE  ps_supplycost BETWEEN 366.6269441785801 AND 372.28111769200063
       AND ps_availqty BETWEEN 2502 AND 4479;

SELECT 1079,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_shipdate BETWEEN 'Fri Aug 07 22:36:31 PDT 1992' AND 'Fri Sep 17 22:36:31 PDT 1993'
       AND l_discount BETWEEN 0.04391087386148462 AND 0.06201703864170511;

SELECT 1084,
       COUNT(*)
FROM   tpch.customer
WHERE  c_acctbal BETWEEN 312.82135226880155 AND 1442.9677544071765;

SELECT 1090,
       COUNT(*)
FROM   tpch.orders
WHERE  o_totalprice BETWEEN 209067.21429757614 AND 296890.71268375404;

SELECT 1091,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_commitdate BETWEEN 'Fri Jan 05 01:52:14 PST 1996' AND 'Fri Oct 04 02:52:14 PDT 1996'
       AND l_tax BETWEEN 0.024869610099789705 AND 0.036985078682026606;

SELECT 1098,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_commitdate BETWEEN 'Thu May 21 18:17:46 PDT 1992' AND 'Sat Mar 13 17:17:46 PST 1993'
       AND l_extendedprice BETWEEN 27575.850613386487 AND 27736.751008600484;

SELECT 1101,
       COUNT(*)
FROM   tpch.customer
WHERE  c_acctbal BETWEEN -188.41424049632235 AND -124.94348581097341;

SELECT 1103,
       COUNT(*)
FROM   tpch.nation,
       tpch.region,
       tpch.customer,
       tpch.orders
WHERE  o_orderdate BETWEEN 'Fri Jan 08 18:30:28 PST 1993' AND 'Tue Dec 28 18:30:28 PST 1993'
       AND o_totalprice BETWEEN 90164.55330888239 AND 181754.7638191052
       AND c_custkey = o_custkey
       AND n_nationkey = c_nationkey
       AND r_regionkey = n_regionkey;

SELECT 1104,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_shipdate BETWEEN 'Tue Jan 13 01:11:27 PST 1998' AND 'Wed Feb 04 01:11:27 PST 1998';

SELECT 1105,
       COUNT(*)
FROM   tpch.supplier,
       tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  l_discount BETWEEN 0.07902492285060723 AND 0.08931660828123215
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND s_suppkey = ps_suppkey
       AND o_orderkey = l_orderkey;

SELECT 1111,
       COUNT(*)
FROM   tpch.customer,
       tpch.orders
WHERE  o_totalprice BETWEEN 122888.57403174206 AND 124685.0534383582
       AND c_custkey = o_custkey;

SELECT 1112,
       COUNT(*)
FROM   tpch.orders
WHERE  o_orderdate BETWEEN 'Tue May 12 00:11:45 PDT 1992' AND 'Mon Jun 01 00:11:45 PDT 1992'
       AND o_totalprice BETWEEN 151738.82293264213 AND 240597.6420256541;

SELECT 1113,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  l_discount BETWEEN 0.09755056347571728 AND 0.11046799695880877
       AND o_totalprice BETWEEN 230879.12916992704 AND 333315.3073925446
       AND o_orderdate BETWEEN 'Wed Jan 26 11:27:15 PST 1994' AND 'Sun Sep 25 12:27:15 PDT 1994'
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND c_custkey = o_custkey;

SELECT 1114,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_quantity BETWEEN 31.884363185588573 AND 32.28331787976867
       AND l_extendedprice BETWEEN 24595.513892142262 AND 37675.65336788587;

SELECT 1116,
       COUNT(*)
FROM   tpch.part
WHERE  p_size BETWEEN 3 AND 3;

SELECT 1119,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_discount BETWEEN 0.07290781937707205 AND 0.0923417423297051;

SELECT 1122,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.part
WHERE  ps_supplycost BETWEEN 386.5948421198724 AND 390.46447213311717
       AND ps_availqty BETWEEN 2024 AND 2050
       AND p_partkey = ps_partkey;

SELECT 1124,
       COUNT(*)
FROM   tpch.orders
WHERE  o_orderdate BETWEEN 'Tue Aug 19 00:21:53 PDT 1997' AND 'Fri May 08 00:21:53 PDT 1998'
       AND o_totalprice BETWEEN 350668.6312286037 AND 441630.0465439123;

SELECT 1126,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  l_shipdate BETWEEN 'Fri Sep 08 20:49:24 PDT 1995' AND 'Sun Oct 01 20:49:24 PDT 1995'
       AND l_commitdate BETWEEN 'Fri Jul 14 07:00:40 PDT 1995' AND 'Fri Jul 28 07:00:40 PDT 1995'
       AND o_orderkey = l_orderkey;

SELECT 1134,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  l_commitdate BETWEEN 'Thu Oct 15 02:51:31 PDT 1992' AND 'Fri Aug 20 02:51:31 PDT 1993'
       AND l_extendedprice BETWEEN 54802.6344329727 AND 75591.1071929561
       AND o_totalprice BETWEEN 220555.77766623767 AND 289475.94474138244
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 1137,
       COUNT(*)
FROM   tpch.supplier,
       tpch.partsupp,
       tpch.part,
       tpch.lineitem
WHERE  s_acctbal BETWEEN 6349.626448293262 AND 6364.725542635382
       AND ps_supplycost BETWEEN 905.5020820925504 AND 1098.088707851463
       AND ps_availqty BETWEEN 647 AND 1765
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND p_partkey = ps_partkey
       AND s_suppkey = ps_suppkey;

SELECT 1140,
       COUNT(*)
FROM   tpch.partsupp
WHERE  ps_availqty BETWEEN 7095 AND 9092;

SELECT 1142,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_quantity BETWEEN 31.884363185588573 AND 32.28331787976867
       AND l_extendedprice BETWEEN 24595.513892142262 AND 37675.65336788587;

SELECT 1143,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_extendedprice BETWEEN 79762.42409145922 AND 80413.56404306872;

SELECT 1153,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_tax BETWEEN 1.1417632866243643E-4 AND 0.010151030695495365;

SELECT 1158,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_receiptdate BETWEEN 'Mon Oct 06 10:20:41 PDT 1997' AND 'Mon Oct 27 09:20:41 PST 1997';

SELECT 1159,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_extendedprice BETWEEN 7209.301557563012 AND 19711.053045686815
       AND l_discount BETWEEN 0.07493354297381243 AND 0.09187341933791866;

SELECT 1160,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  l_shipdate BETWEEN 'Sun Aug 25 19:15:28 PDT 1996' AND 'Sat May 31 19:15:28 PDT 1997'
       AND l_quantity BETWEEN 45.85045211646647 AND 54.07333706451543
       AND l_tax BETWEEN 0.07461807192704056 AND 0.08882004688963667
       AND o_orderkey = l_orderkey;

SELECT 1161,
       COUNT(*)
FROM   tpch.nation,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  c_acctbal BETWEEN 495.22398160893545 AND 524.9737231077037
       AND l_receiptdate BETWEEN 'Fri Jan 07 17:58:27 PST 1994' AND 'Sun Jan 09 17:58:27 PST 1994'
       AND o_orderkey = l_orderkey
       AND c_custkey = o_custkey
       AND n_nationkey = c_nationkey;

SELECT 1164,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_shipdate BETWEEN 'Wed Jan 04 18:06:57 PST 1995' AND 'Mon Mar 04 18:06:57 PST 1996'
       AND l_tax BETWEEN 0.026251812127526176 AND 0.041359376193486855;

SELECT 1167,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_shipdate BETWEEN 'Tue Jul 01 02:08:02 PDT 1997' AND 'Wed Jul 16 02:08:02 PDT 1997'
       AND l_commitdate BETWEEN 'Sun May 11 01:12:03 PDT 1997' AND 'Fri Aug 07 01:12:03 PDT 1998'
       AND l_extendedprice BETWEEN 101545.56508482285 AND 119167.05443846797;

SELECT 1177,
       COUNT(*)
FROM   tpch.partsupp
WHERE  ps_availqty BETWEEN 1988 AND 2006;

SELECT 1179,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_tax BETWEEN 0.05365973424643771 AND 0.06674818715447284
       AND l_receiptdate BETWEEN 'Thu Apr 15 09:36:22 PDT 1993' AND 'Tue Mar 29 08:36:22 PST 1994';

SELECT 1187,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_shipdate BETWEEN 'Thu Jun 11 02:50:42 PDT 1998' AND 'Thu Sep 23 02:50:42 PDT 1999';

SELECT 1190,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  o_orderdate BETWEEN 'Wed Nov 15 10:57:53 PST 1995' AND 'Mon Nov 11 10:57:53 PST 1996'
       AND o_totalprice BETWEEN 22855.643095777486 AND 95372.31888765944
       AND o_orderkey = l_orderkey;

SELECT 1192,
       COUNT(*)
FROM   tpch.partsupp
WHERE  ps_supplycost BETWEEN 288.2362519141849 AND 293.19079691127786;

SELECT 1194,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_discount BETWEEN 0.07290781937707205 AND 0.0923417423297051;

SELECT 1195,
       COUNT(*)
FROM   tpch.nation,
       tpch.customer,
       tpch.orders
WHERE  o_orderdate BETWEEN 'Sat Oct 07 21:58:04 PDT 1995' AND 'Sat Dec 28 20:58:04 PST 1996'
       AND o_totalprice BETWEEN 366689.6662007361 AND 468947.0794495564
       AND c_custkey = o_custkey
       AND n_nationkey = c_nationkey;

SELECT 1196,
       COUNT(*)
FROM   tpch.supplier,
       tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  l_quantity BETWEEN 23.278924876013434 AND 30.37403962801164
       AND l_receiptdate BETWEEN 'Wed Feb 19 17:31:16 PST 1997' AND 'Wed Jan 07 17:31:16 PST 1998'
       AND o_totalprice BETWEEN 365671.1434642377 AND 370567.67888193246
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND o_orderkey = l_orderkey
       AND s_suppkey = ps_suppkey;

SELECT 1198,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_extendedprice BETWEEN 100175.32185982323 AND 112591.64596010136
       AND l_receiptdate BETWEEN 'Sat Mar 28 13:03:39 PST 1998' AND 'Wed Apr 15 14:03:39 PDT 1998';

SELECT 1199,
       COUNT(*)
FROM   tpch.orders
WHERE  o_totalprice BETWEEN 290156.7504861304 AND 370125.26103653066
       AND o_orderdate BETWEEN 'Sat Apr 05 21:14:55 PST 1997' AND 'Thu Apr 30 22:14:55 PDT 1998';

SELECT 1204,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  l_shipdate BETWEEN 'Tue Jul 02 02:12:08 PDT 1996' AND 'Fri Jul 18 02:12:08 PDT 1997'
       AND l_extendedprice BETWEEN 103858.64713290597 AND 121882.83318277018
       AND o_totalprice BETWEEN 377496.5956239936 AND 483892.9458513309
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 1205,
       COUNT(*)
FROM   tpch.supplier,
       tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  s_acctbal BETWEEN 3764.810559998721 AND 3817.025145014541
       AND ps_availqty BETWEEN 8128 AND 9186
       AND ps_supplycost BETWEEN 556.7280277844884 AND 742.515945465762
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND o_orderkey = l_orderkey
       AND s_suppkey = ps_suppkey;

SELECT 1208,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_receiptdate BETWEEN 'Tue Jan 07 16:25:58 PST 1992' AND 'Fri Jan 17 16:25:58 PST 1992';

SELECT 1216,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_tax BETWEEN 0.03165065499054995 AND 0.042137924210340155
       AND l_shipdate BETWEEN 'Sun Oct 10 03:30:20 PDT 1993' AND 'Thu Nov 03 02:30:20 PST 1994';

SELECT 1232,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  l_quantity BETWEEN 41.686321328707784 AND 42.13564965741954
       AND l_receiptdate BETWEEN 'Tue Sep 22 18:39:09 PDT 1998' AND 'Tue Jun 15 18:39:09 PDT 1999'
       AND o_orderkey = l_orderkey;

SELECT 1233,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_commitdate BETWEEN 'Fri Oct 30 13:50:56 PST 1998' AND 'Sat Jul 24 14:50:56 PDT 1999';

SELECT 1247,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_shipdate BETWEEN 'Sat Jul 03 10:13:43 PDT 1993' AND 'Mon Jul 05 10:13:43 PDT 1993'
       AND l_extendedprice BETWEEN 49060.8638210462 AND 49143.03209270037;

SELECT 1255,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_extendedprice BETWEEN 52075.782728818645 AND 52974.71931883516
       AND l_receiptdate BETWEEN 'Sun Feb 18 15:03:14 PST 1996' AND 'Mon Feb 19 15:03:14 PST 1996'
       AND l_tax BETWEEN 0.07894102610717726 AND 0.0937271715113061;

SELECT 1259,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_tax BETWEEN 0.03265020855767558 AND 0.04166635421400588
       AND l_shipdate BETWEEN 'Sun Aug 10 06:20:51 PDT 1997' AND 'Sat May 23 06:20:51 PDT 1998';

SELECT 1270,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  o_orderdate BETWEEN 'Fri Feb 27 21:00:38 PST 1998' AND 'Thu Mar 11 21:00:38 PST 1999'
       AND o_totalprice BETWEEN 477819.87324786145 AND 534487.283125121
       AND o_orderkey = l_orderkey;

SELECT 1274,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem
WHERE  ps_availqty BETWEEN 4952 AND 6464
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 1287,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_discount BETWEEN 0.009566597641386133 AND 0.021633384803186058;

SELECT 1290,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_quantity BETWEEN 2.3479553984081174 AND 10.601453687029533
       AND l_commitdate BETWEEN 'Tue Sep 08 23:08:09 PDT 1992' AND 'Tue Jul 20 23:08:09 PDT 1993';

SELECT 1292,
       COUNT(*)
FROM   tpch.partsupp
WHERE  ps_availqty BETWEEN 9588 AND 10644
       AND ps_supplycost BETWEEN 804.8647032337267 AND 805.7885531340046;

SELECT 1293,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  l_commitdate BETWEEN 'Wed Mar 27 17:07:17 PST 1996' AND 'Thu Apr 17 18:07:17 PDT 1997'
       AND l_tax BETWEEN 0.07341886083755933 AND 0.0824488439607895
       AND o_orderkey = l_orderkey;

SELECT 1296,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  l_quantity BETWEEN 40.94568173647773 AND 41.38803202364948
       AND o_totalprice BETWEEN 98927.207532111 AND 176261.45147683244
       AND o_orderdate BETWEEN 'Fri Jul 15 03:00:34 PDT 1994' AND 'Fri Mar 17 02:00:34 PST 1995'
       AND o_orderkey = l_orderkey;

SELECT 1300,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_receiptdate BETWEEN 'Sat Jul 25 19:24:38 PDT 1992' AND 'Sat Jul 25 19:24:38 PDT 1992';

SELECT 1304,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_extendedprice BETWEEN 45145.176819492575 AND 65836.71176580846
       AND l_commitdate BETWEEN 'Wed Feb 23 16:09:07 PST 1994' AND 'Sat Mar 12 16:09:07 PST 1994'
       AND l_tax BETWEEN 0.01169657582094306 AND 0.025459364889956884;

SELECT 1311,
       COUNT(*)
FROM   tpch.nation,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  o_totalprice BETWEEN 230879.12916992704 AND 333315.3073925446
       AND o_orderdate BETWEEN 'Wed Jan 26 11:27:15 PST 1994' AND 'Sun Sep 25 12:27:15 PDT 1994'
       AND o_orderkey = l_orderkey
       AND c_custkey = o_custkey
       AND n_nationkey = c_nationkey;

SELECT 1312,
       COUNT(*)
FROM   tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  c_acctbal BETWEEN 6654.7440815769505 AND 8357.46579512721
       AND o_totalprice BETWEEN 239761.06891496957 AND 240878.78206342927
       AND o_orderdate BETWEEN 'Sun Oct 22 22:14:53 PDT 1995' AND 'Mon Oct 30 21:14:53 PST 1995'
       AND c_custkey = o_custkey
       AND o_orderkey = l_orderkey;

SELECT 1313,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_discount BETWEEN 0.02769912562390735 AND 0.03859923581119491
       AND l_extendedprice BETWEEN 40740.01847300819 AND 57080.859437769905;

SELECT 1314,
       COUNT(*)
FROM   tpch.orders
WHERE  o_totalprice BETWEEN 258507.55638880268 AND 359145.14704425284;

SELECT 1319,
       COUNT(*)
FROM   tpch.partsupp
WHERE  ps_availqty BETWEEN 9187 AND 10468;

SELECT 1330,
       COUNT(*)
FROM   tpch.orders
WHERE  o_totalprice BETWEEN 236134.86356445894 AND 333848.2807447487;

SELECT 1333,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_discount BETWEEN 0.020679258364533815 AND 0.031159014391135626;

SELECT 1334,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  l_commitdate BETWEEN 'Fri Nov 14 03:51:20 PST 1997' AND 'Thu Jul 23 04:51:20 PDT 1998'
       AND l_quantity BETWEEN 12.69971057301211 AND 19.87248357371149
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 1335,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_quantity BETWEEN 24.002288216912824 AND 32.93357890770828
       AND l_receiptdate BETWEEN 'Wed Oct 07 20:11:38 PDT 1992' AND 'Thu Oct 28 20:11:38 PDT 1993';

SELECT 1342,
       COUNT(*)
FROM   tpch.orders
WHERE  o_orderdate BETWEEN 'Tue May 12 00:11:45 PDT 1992' AND 'Mon Jun 01 00:11:45 PDT 1992'
       AND o_totalprice BETWEEN 151738.82293264213 AND 240597.6420256541;

SELECT 1345,
       COUNT(*)
FROM   tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  l_extendedprice BETWEEN 26541.61491815808 AND 41758.92864953205
       AND o_totalprice BETWEEN 302563.1283704498 AND 359841.15612864186
       AND o_orderkey = l_orderkey
       AND c_custkey = o_custkey;

SELECT 1355,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  l_shipdate BETWEEN 'Fri Jul 22 11:57:06 PDT 1994' AND 'Wed Oct 04 11:57:06 PDT 1995'
       AND l_discount BETWEEN 0.09675557376342578 AND 0.11330473021890965
       AND l_extendedprice BETWEEN 35688.81605532445 AND 53930.62331078554
       AND o_orderdate BETWEEN 'Sun Jul 24 10:02:53 PDT 1994' AND 'Wed Jul 27 10:02:53 PDT 1994'
       AND o_totalprice BETWEEN 120809.4025633744 AND 192122.5234418741
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 1356,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_extendedprice BETWEEN 3042.653732743923 AND 20735.148027259504
       AND l_commitdate BETWEEN 'Tue May 05 03:44:37 PDT 1992' AND 'Thu May 14 03:44:37 PDT 1992';

SELECT 1360,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  ps_availqty BETWEEN 4251 AND 4291
       AND o_orderdate BETWEEN 'Sat Oct 02 19:51:18 PDT 1993' AND 'Mon Oct 31 18:51:18 PST 1994'
       AND o_totalprice BETWEEN 146822.8489020799 AND 225118.2605626132
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 1361,
       COUNT(*)
FROM   tpch.supplier,
       tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  ps_supplycost BETWEEN 932.1476588017267 AND 933.606332579753
       AND l_extendedprice BETWEEN 30312.429244477506 AND 47153.08228529323
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND o_orderkey = l_orderkey
       AND s_suppkey = ps_suppkey;

SELECT 1362,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_commitdate BETWEEN 'Sat Jun 13 02:15:50 PDT 1992' AND 'Wed Mar 03 01:15:50 PST 1993'
       AND l_discount BETWEEN 0.0496697034438559 AND 0.06334693479303716;

SELECT 1367,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_tax BETWEEN 0.06113497447101675 AND 0.07408094325453465;

SELECT 1375,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_extendedprice BETWEEN 9195.758273762467 AND 10181.722261478435
       AND l_receiptdate BETWEEN 'Sun Jul 09 12:53:41 PDT 1995' AND 'Thu Oct 17 12:53:41 PDT 1996';

SELECT 1376,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  l_extendedprice BETWEEN 95302.59272538406 AND 95885.61363401382
       AND l_commitdate BETWEEN 'Mon Nov 18 05:30:23 PST 1996' AND 'Wed Aug 13 06:30:23 PDT 1997'
       AND o_orderkey = l_orderkey;

SELECT 1378,
       COUNT(*)
FROM   tpch.orders
WHERE  o_orderdate BETWEEN 'Fri Feb 25 22:04:22 PST 1994' AND 'Fri Jan 20 22:04:22 PST 1995'
       AND o_totalprice BETWEEN 37365.68396464411 AND 106698.2608989657;

SELECT 1385,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  l_tax BETWEEN 0.008340179746847705 AND 0.019634139215712824
       AND l_discount BETWEEN 0.012356322761971295 AND 0.028260995713682457
       AND o_totalprice BETWEEN 463961.9812626602 AND 468658.64979812835
       AND o_orderkey = l_orderkey;

SELECT 1387,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  l_quantity BETWEEN 40.94568173647773 AND 41.38803202364948
       AND o_totalprice BETWEEN 98927.207532111 AND 176261.45147683244
       AND o_orderdate BETWEEN 'Fri Jul 15 03:00:34 PDT 1994' AND 'Fri Mar 17 02:00:34 PST 1995'
       AND o_orderkey = l_orderkey;

SELECT 1390,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_commitdate BETWEEN 'Sun Jan 11 20:15:03 PST 1998' AND 'Mon Feb 15 20:15:03 PST 1999'
       AND l_tax BETWEEN 0.05540154857564852 AND 0.06806450598302848;

SELECT 1391,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_tax BETWEEN 0.0778078035142845 AND 0.08950372289741823
       AND l_shipdate BETWEEN 'Sun Feb 02 12:08:02 PST 1997' AND 'Wed Jun 10 13:08:02 PDT 1998';

SELECT 1398,
       COUNT(*)
FROM   tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  l_extendedprice BETWEEN 38603.191231316756 AND 39072.57958922852
       AND c_custkey = o_custkey
       AND o_orderkey = l_orderkey;

SELECT 1401,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_tax BETWEEN 0.03938465765238244 AND 0.05267698532660164
       AND l_shipdate BETWEEN 'Sat Apr 13 03:17:30 PDT 1996' AND 'Sun Apr 14 03:17:30 PDT 1996';

SELECT 1406,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_extendedprice BETWEEN 52210.64372962707 AND 67024.63793921893
       AND l_discount BETWEEN 0.04339105154571613 AND 0.057561507744846
       AND l_tax BETWEEN 0.059006273347368125 AND 0.07248647659507333;

SELECT 1409,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_extendedprice BETWEEN 44132.04252517913 AND 44620.405428644444
       AND l_commitdate BETWEEN 'Fri Dec 29 06:27:06 PST 1995' AND 'Fri Feb 21 06:27:06 PST 1997';

SELECT 1412,
       COUNT(*)
FROM   tpch.customer,
       tpch.orders
WHERE  c_acctbal BETWEEN 7141.205698422944 AND 8516.065401365024
       AND c_custkey = o_custkey;

SELECT 1425,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.part,
       tpch.lineitem,
       tpch.orders
WHERE  p_retailprice BETWEEN 1876.2270944246504 AND 1883.84086242501
       AND l_receiptdate BETWEEN 'Sun Sep 21 14:23:49 PDT 1997' AND 'Wed Jan 06 13:23:49 PST 1999'
       AND l_shipdate BETWEEN 'Tue Oct 14 12:39:17 PDT 1997' AND 'Fri Feb 05 11:39:17 PST 1999'
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND p_partkey = ps_partkey;

SELECT 1426,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  l_receiptdate BETWEEN 'Tue Mar 16 17:00:07 PST 1993' AND 'Fri Apr 09 18:00:07 PDT 1993'
       AND l_quantity BETWEEN 42.724387119566686 AND 43.197552181888504
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND c_custkey = o_custkey;

SELECT 1429,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem
WHERE  ps_supplycost BETWEEN 928.5631758271927 AND 1098.236990194691
       AND ps_availqty BETWEEN 1451 AND 2880
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 1432,
       COUNT(*)
FROM   tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  c_acctbal BETWEEN 2654.256309430858 AND 4689.008415617444
       AND o_totalprice BETWEEN 77725.58425925187 AND 150516.66810200346
       AND o_orderdate BETWEEN 'Wed May 19 11:40:03 PDT 1993' AND 'Sat Jun 05 11:40:03 PDT 1993'
       AND o_orderkey = l_orderkey
       AND c_custkey = o_custkey;

SELECT 1434,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  l_extendedprice BETWEEN 44132.04252517913 AND 44620.405428644444
       AND l_commitdate BETWEEN 'Fri Dec 29 06:27:06 PST 1995' AND 'Fri Feb 21 06:27:06 PST 1997'
       AND o_totalprice BETWEEN 193955.21110636345 AND 289082.38758137997
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 1436,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_extendedprice BETWEEN 39376.22760294208 AND 40406.979962964346;

SELECT 1439,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_commitdate BETWEEN 'Thu Aug 12 15:29:32 PDT 1993' AND 'Tue Oct 04 15:29:32 PDT 1994';

SELECT 1449,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_quantity BETWEEN 26.905997411602115 AND 27.36006213717891
       AND l_shipdate BETWEEN 'Mon Mar 27 23:30:16 PST 1995' AND 'Sun May 05 00:30:16 PDT 1996'
       AND l_tax BETWEEN 0.029107426578671013 AND 0.04195144534811995;

SELECT 1453,
       COUNT(*)
FROM   tpch.orders
WHERE  o_orderdate BETWEEN 'Fri Jun 12 04:04:51 PDT 1992' AND 'Sat Jun 19 04:04:51 PDT 1993'
       AND o_totalprice BETWEEN 145734.98234268453 AND 233202.77418591047;

SELECT 1455,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.part,
       tpch.lineitem,
       tpch.orders
WHERE  ps_availqty BETWEEN 3101 AND 5056
       AND ps_supplycost BETWEEN 878.4347356615707 AND 879.956720871681
       AND p_retailprice BETWEEN 1772.3938841206937 AND 1952.2354714720968
       AND p_size BETWEEN 10 AND 15
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND p_partkey = ps_partkey;

SELECT 1457,
       COUNT(*)
FROM   tpch.supplier,
       tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  s_acctbal BETWEEN 9288.800990167501 AND 10778.461650791032
       AND o_orderdate BETWEEN 'Mon May 09 19:51:08 PDT 1994' AND 'Mon May 16 19:51:08 PDT 1994'
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND s_suppkey = ps_suppkey;

SELECT 1463,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_quantity BETWEEN 29.460649969950502 AND 35.48928551733505
       AND l_tax BETWEEN 0.0689032089603857 AND 0.08308276652524775
       AND l_shipdate BETWEEN 'Fri Oct 04 10:11:36 PDT 1996' AND 'Fri Sep 12 10:11:36 PDT 1997';

SELECT 1465,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  o_totalprice BETWEEN 481502.88257717463 AND 563438.0906447308
       AND o_orderkey = l_orderkey
       AND c_custkey = o_custkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 1473,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_receiptdate BETWEEN 'Thu Jul 21 00:29:15 PDT 1994' AND 'Fri Jul 28 00:29:15 PDT 1995';

SELECT 1475,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_shipdate BETWEEN 'Sat Apr 16 17:11:52 PDT 1994' AND 'Sat May 07 17:11:52 PDT 1994'
       AND l_extendedprice BETWEEN 22710.507820744002 AND 22982.023544148804;

SELECT 1480,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_extendedprice BETWEEN 15374.915268575309 AND 27259.681907970975
       AND l_discount BETWEEN 0.0063308680698899505 AND 0.02545897659433837;

SELECT 1482,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_discount BETWEEN 0.031374891672292536 AND 0.0415377374213934
       AND l_shipdate BETWEEN 'Mon Jul 21 08:21:34 PDT 1997' AND 'Mon Aug 24 08:21:34 PDT 1998'
       AND l_extendedprice BETWEEN 82093.22287008258 AND 82350.62009683302;

SELECT 1488,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  o_totalprice BETWEEN 440718.5124456932 AND 445901.6355015847
       AND o_orderdate BETWEEN 'Thu Oct 13 23:53:57 PDT 1994' AND 'Sun Sep 03 23:53:57 PDT 1995'
       AND o_orderkey = l_orderkey;

SELECT 1489,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_quantity BETWEEN 18.066317354892615 AND 23.83295473718668
       AND l_tax BETWEEN 0.025091593078469598 AND 0.035695332579979894;

SELECT 1492,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  o_totalprice BETWEEN 259261.69041051267 AND 364820.621879755
       AND o_orderdate BETWEEN 'Mon Apr 11 21:23:19 PDT 1994' AND 'Thu May 11 21:23:19 PDT 1995'
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND o_orderkey = l_orderkey;

SELECT 1493,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_shipdate BETWEEN 'Tue Jan 13 01:11:27 PST 1998' AND 'Wed Feb 04 01:11:27 PST 1998';

SELECT 1495,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  o_orderdate BETWEEN 'Tue Nov 22 14:03:25 PST 1994' AND 'Sun Jul 30 15:03:25 PDT 1995'
       AND o_totalprice BETWEEN 164784.46295508684 AND 256069.87878254591
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 1498,
       COUNT(*)
FROM   tpch.supplier,
       tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  l_quantity BETWEEN 23.278924876013434 AND 30.37403962801164
       AND l_receiptdate BETWEEN 'Wed Feb 19 17:31:16 PST 1997' AND 'Wed Jan 07 17:31:16 PST 1998'
       AND o_totalprice BETWEEN 365671.1434642377 AND 370567.67888193246
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND o_orderkey = l_orderkey
       AND s_suppkey = ps_suppkey;

SELECT 1500,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  ps_supplycost BETWEEN 84.21679687745188 AND 193.26287126714482
       AND ps_availqty BETWEEN 1066 AND 2406
       AND o_orderkey = l_orderkey
       AND c_custkey = o_custkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 1512,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  o_totalprice BETWEEN 256306.63921089325 AND 329572.2738984181
       AND o_orderdate BETWEEN 'Thu Jan 09 20:43:50 PST 1997' AND 'Sat May 02 21:43:50 PDT 1998'
       AND o_orderkey = l_orderkey;

SELECT 1513,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  o_totalprice BETWEEN 16603.41553699638 AND 102226.13520319841
       AND o_orderdate BETWEEN 'Sat Apr 04 01:27:41 PST 1998' AND 'Sun Apr 26 02:27:41 PDT 1998'
       AND o_orderkey = l_orderkey;

SELECT 1515,
       COUNT(*)
FROM   tpch.part
WHERE  p_size BETWEEN 25 AND 30
       AND p_retailprice BETWEEN 927.5793885029175 AND 938.9313834469009;

SELECT 1520,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_shipdate BETWEEN 'Sat Apr 16 17:11:52 PDT 1994' AND 'Sat May 07 17:11:52 PDT 1994'
       AND l_extendedprice BETWEEN 22710.507820744002 AND 22982.023544148804;

SELECT 1523,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_tax BETWEEN 0.05756752550747243 AND 0.06885700362294706
       AND l_commitdate BETWEEN 'Sat Jun 06 13:27:21 PDT 1992' AND 'Tue Aug 03 13:27:21 PDT 1993';

SELECT 1528,
       COUNT(*)
FROM   tpch.orders
WHERE  o_orderdate BETWEEN 'Fri Jun 23 21:20:06 PDT 1995' AND 'Mon Jul 03 21:20:06 PDT 1995'
       AND o_totalprice BETWEEN 349645.2689584472 AND 353527.0976346283;

SELECT 1530,
       COUNT(*)
FROM   tpch.nation,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  c_acctbal BETWEEN -559.8278259312665 AND 1606.8700961951813
       AND o_orderkey = l_orderkey
       AND c_custkey = o_custkey
       AND n_nationkey = c_nationkey;

SELECT 1536,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_discount BETWEEN 0.01216811186405743 AND 0.023933446456747158;

SELECT 1542,
       COUNT(*)
FROM   tpch.partsupp
WHERE  ps_supplycost BETWEEN 666.2876140217824 AND 667.0944065815756;

SELECT 1543,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  ps_supplycost BETWEEN 494.72571800880195 AND 495.0803620000238
       AND o_orderdate BETWEEN 'Sun Apr 19 00:24:28 PDT 1992' AND 'Tue May 18 00:24:28 PDT 1993'
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND c_custkey = o_custkey;

SELECT 1544,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_discount BETWEEN 0.04436820505788158 AND 0.05552940600455606
       AND l_extendedprice BETWEEN 81360.35170524316 AND 91782.21719342342
       AND l_receiptdate BETWEEN 'Tue Dec 13 22:02:51 PST 1994' AND 'Wed Jan 04 22:02:51 PST 1995';

SELECT 1550,
       COUNT(*)
FROM   tpch.orders
WHERE  o_orderdate BETWEEN 'Mon Jul 10 10:45:53 PDT 1995' AND 'Fri Jul 28 10:45:53 PDT 1995'
       AND o_totalprice BETWEEN 457886.2804467981 AND 543800.3098217448;

SELECT 1553,
       COUNT(*)
FROM   tpch.supplier,
       tpch.partsupp,
       tpch.part,
       tpch.lineitem
WHERE  p_retailprice BETWEEN 2078.9575431998846 AND 2081.721341449786
       AND l_commitdate BETWEEN 'Sun May 02 05:44:36 PDT 1993' AND 'Fri May 20 05:44:36 PDT 1994'
       AND l_tax BETWEEN 0.023545145023301545 AND 0.03790606813693982
       AND p_partkey = ps_partkey
       AND s_suppkey = ps_suppkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 1555,
       COUNT(*)
FROM   tpch.customer
WHERE  c_acctbal BETWEEN -188.41424049632235 AND -124.94348581097341;

SELECT 1560,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_discount BETWEEN 0.07013914971128908 AND 0.08109063450080359;

SELECT 1564,
       COUNT(*)
FROM   tpch.orders
WHERE  o_orderdate BETWEEN 'Sat Dec 06 10:48:22 PST 1997' AND 'Wed Nov 25 10:48:22 PST 1998'
       AND o_totalprice BETWEEN 101789.96976383116 AND 171423.06202904027;

SELECT 1571,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_commitdate BETWEEN 'Sat Jun 13 02:15:50 PDT 1992' AND 'Wed Mar 03 01:15:50 PST 1993'
       AND l_discount BETWEEN 0.0496697034438559 AND 0.06334693479303716;

SELECT 1579,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  l_commitdate BETWEEN 'Fri Oct 09 23:31:28 PDT 1992' AND 'Mon Oct 26 22:31:28 PST 1992'
       AND l_discount BETWEEN 0.015671817495096243 AND 0.030657312880907815
       AND o_orderkey = l_orderkey;

SELECT 1580,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  ps_availqty BETWEEN 859 AND 1951
       AND ps_supplycost BETWEEN 113.82100309397784 AND 252.0539059321605
       AND l_tax BETWEEN 0.042018058547119284 AND 0.05567734174045606
       AND o_totalprice BETWEEN 121507.01168426803 AND 190817.65597272152
       AND o_orderdate BETWEEN 'Sat May 30 03:11:16 PDT 1998' AND 'Fri Jun 05 03:11:16 PDT 1998'
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 1581,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_extendedprice BETWEEN 75951.27727765717 AND 76491.75691170825;

SELECT 1588,
       COUNT(*)
FROM   tpch.supplier,
       tpch.partsupp,
       tpch.part,
       tpch.lineitem
WHERE  s_acctbal BETWEEN -930.6459391225465 AND -900.3640438332797
       AND ps_availqty BETWEEN 5695 AND 5732
       AND ps_supplycost BETWEEN 193.69080923147104 AND 201.91171025428355
       AND l_commitdate BETWEEN 'Mon Apr 17 03:36:40 PDT 1995' AND 'Sat Jan 06 02:36:40 PST 1996'
       AND p_partkey = ps_partkey
       AND s_suppkey = ps_suppkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 1590,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.part
WHERE  ps_supplycost BETWEEN 386.5948421198724 AND 390.46447213311717
       AND ps_availqty BETWEEN 2024 AND 2050
       AND p_partkey = ps_partkey;

SELECT 1597,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  ps_supplycost BETWEEN 693.8945733265506 AND 805.9607045487444
       AND ps_availqty BETWEEN 5325 AND 6699
       AND o_totalprice BETWEEN 70939.91626854602 AND 72133.76348452458
       AND o_orderdate BETWEEN 'Thu Feb 13 22:45:15 PST 1997' AND 'Sat Mar 01 22:45:15 PST 1997'
       AND c_custkey = o_custkey
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 1601,
       COUNT(*)
FROM   tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  o_orderdate BETWEEN 'Mon Jan 13 21:30:28 PST 1997' AND 'Mon Feb 23 21:30:28 PST 1998'
       AND o_totalprice BETWEEN 128613.49965588076 AND 133997.54073861206
       AND o_orderkey = l_orderkey
       AND c_custkey = o_custkey;

SELECT 1603,
       COUNT(*)
FROM   tpch.orders
WHERE  o_totalprice BETWEEN 303650.8468007094 AND 360830.8906457936;

SELECT 1605,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_tax BETWEEN 0.06773241887881469 AND 0.07573556116928813
       AND l_discount BETWEEN 0.07786949287250791 AND 0.09113192314000346;

SELECT 1610,
       COUNT(*)
FROM   tpch.partsupp
WHERE  ps_supplycost BETWEEN 627.4878351266818 AND 728.3950492764249
       AND ps_availqty BETWEEN 1368 AND 2939;

SELECT 1612,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_shipdate BETWEEN 'Tue Jul 05 05:44:57 PDT 1994' AND 'Tue Jun 06 05:44:57 PDT 1995'
       AND l_extendedprice BETWEEN 63742.50528968195 AND 82679.5516992607;

SELECT 1613,
       COUNT(*)
FROM   tpch.orders
WHERE  o_totalprice BETWEEN 207281.64584481722 AND 297822.7746152794;

SELECT 1616,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_quantity BETWEEN 12.153163521418243 AND 19.78630150213136
       AND l_discount BETWEEN 0.02309743785906576 AND 0.040337547182832015
       AND l_shipdate BETWEEN 'Mon Mar 01 11:49:04 PST 1993' AND 'Sun Mar 27 11:49:04 PST 1994';

SELECT 1624,
       COUNT(*)
FROM   tpch.orders
WHERE  o_orderdate BETWEEN 'Mon Dec 11 12:36:28 PST 1995' AND 'Sun Feb 16 12:36:28 PST 1997'
       AND o_totalprice BETWEEN 254937.79722936152 AND 260087.9047894202;

SELECT 1626,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_tax BETWEEN 1.1417632866243643E-4 AND 0.010151030695495365;

SELECT 1630,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  o_totalprice BETWEEN 16603.41553699638 AND 102226.13520319841
       AND o_orderdate BETWEEN 'Sat Apr 04 01:27:41 PST 1998' AND 'Sun Apr 26 02:27:41 PDT 1998'
       AND o_orderkey = l_orderkey;

SELECT 1631,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_tax BETWEEN 0.049168944323960115 AND 0.0636291210033138
       AND l_quantity BETWEEN 37.675691870329636 AND 46.620490824134805;

SELECT 1635,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  o_orderdate BETWEEN 'Fri Jun 26 21:23:12 PDT 1998' AND 'Thu Jul 02 21:23:12 PDT 1998'
       AND o_totalprice BETWEEN 126274.83691525484 AND 131766.55449200873
       AND o_orderkey = l_orderkey;

SELECT 1637,
       COUNT(*)
FROM   tpch.orders
WHERE  o_totalprice BETWEEN 327750.0491902049 AND 330042.11699712434;

SELECT 1638,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_shipdate BETWEEN 'Thu Jun 13 10:16:59 PDT 1996' AND 'Sat May 03 10:16:59 PDT 1997'
       AND l_discount BETWEEN 0.05946297582945621 AND 0.07942087241620747;

SELECT 1640,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_commitdate BETWEEN 'Sat Aug 29 19:01:27 PDT 1998' AND 'Wed Sep 09 19:01:27 PDT 1998';

SELECT 1655,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_discount BETWEEN 0.07022171634764578 AND 0.0869144310964004
       AND l_receiptdate BETWEEN 'Wed May 15 01:27:13 PDT 1996' AND 'Mon Feb 24 00:27:13 PST 1997';

SELECT 1657,
       COUNT(*)
FROM   tpch.supplier,
       tpch.partsupp,
       tpch.lineitem
WHERE  ps_availqty BETWEEN 4362 AND 4390
       AND ps_supplycost BETWEEN 616.7925239670286 AND 795.4181603973065
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND s_suppkey = ps_suppkey;

SELECT 1660,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  o_totalprice BETWEEN 132682.92227143972 AND 215342.5763260556
       AND o_orderdate BETWEEN 'Thu May 01 16:12:26 PDT 1997' AND 'Fri May 09 16:12:26 PDT 1997'
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 1672,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_extendedprice BETWEEN 101588.55311358026 AND 121773.30069605698;

SELECT 1680,
       COUNT(*)
FROM   tpch.nation,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  c_acctbal BETWEEN 9411.061842898474 AND 9460.345503464692
       AND c_custkey = o_custkey
       AND o_orderkey = l_orderkey
       AND n_nationkey = c_nationkey;

SELECT 1683,
       COUNT(*)
FROM   tpch.customer,
       tpch.orders
WHERE  c_acctbal BETWEEN 4772.821731996881 AND 6826.982933897599
       AND o_totalprice BETWEEN 98927.207532111 AND 176261.45147683244
       AND o_orderdate BETWEEN 'Fri Jul 15 03:00:34 PDT 1994' AND 'Fri Mar 17 02:00:34 PST 1995'
       AND c_custkey = o_custkey;

SELECT 1686,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  l_extendedprice BETWEEN 63796.50647155328 AND 75863.66829316047
       AND l_shipdate BETWEEN 'Mon Oct 19 23:46:48 PDT 1992' AND 'Fri Nov 12 22:46:48 PST 1993'
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 1689,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_extendedprice BETWEEN 48850.952750244716 AND 60683.28248080903
       AND l_receiptdate BETWEEN 'Sat Jun 11 20:19:19 PDT 1994' AND 'Fri May 05 20:19:19 PDT 1995';

SELECT 1690,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_receiptdate BETWEEN 'Mon Jul 01 22:57:32 PDT 1996' AND 'Mon Nov 03 21:57:32 PST 1997'
       AND l_quantity BETWEEN 31.003740123476465 AND 40.635132400532854;

SELECT 1695,
       COUNT(*)
FROM   tpch.orders
WHERE  o_totalprice BETWEEN 379184.61103240185 AND 454411.25105625007;

SELECT 1700,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem
WHERE  l_extendedprice BETWEEN 52974.57899132226 AND 70119.13535019066
       AND l_quantity BETWEEN 46.064526835866424 AND 55.66263575814256
       AND l_receiptdate BETWEEN 'Fri Jan 10 13:40:56 PST 1992' AND 'Sun Jan 19 13:40:56 PST 1992'
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 1705,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem
WHERE  ps_supplycost BETWEEN 833.2629557397057 AND 937.1059050290293
       AND ps_availqty BETWEEN 3014 AND 4486
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 1708,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_quantity BETWEEN 38.60390420504421 AND 47.538207259948415
       AND l_commitdate BETWEEN 'Sun Jul 10 15:47:47 PDT 1994' AND 'Wed May 31 15:47:47 PDT 1995'
       AND l_shipdate BETWEEN 'Fri Dec 23 04:15:06 PST 1994' AND 'Sat Jan 14 04:15:06 PST 1995';

SELECT 1722,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_shipdate BETWEEN 'Thu Jun 03 17:59:15 PDT 1993' AND 'Sun Jun 13 17:59:15 PDT 1993';

SELECT 1730,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  ps_availqty BETWEEN 2180 AND 3234
       AND ps_supplycost BETWEEN 910.8645753922245 AND 919.1322039881042
       AND l_tax BETWEEN 0.018457034308877435 AND 0.033361983667445784
       AND l_commitdate BETWEEN 'Fri May 03 19:32:53 PDT 1996' AND 'Wed Aug 27 19:32:53 PDT 1997'
       AND l_shipdate BETWEEN 'Sat May 11 14:41:34 PDT 1996' AND 'Mon May 27 14:41:34 PDT 1996'
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 1731,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  ps_availqty BETWEEN 859 AND 1951
       AND ps_supplycost BETWEEN 113.82100309397784 AND 252.0539059321605
       AND l_tax BETWEEN 0.042018058547119284 AND 0.05567734174045606
       AND o_totalprice BETWEEN 121507.01168426803 AND 190817.65597272152
       AND o_orderdate BETWEEN 'Sat May 30 03:11:16 PDT 1998' AND 'Fri Jun 05 03:11:16 PDT 1998'
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 1733,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  o_orderdate BETWEEN 'Sun Apr 23 07:23:19 PDT 1995' AND 'Mon May 08 07:23:19 PDT 1995'
       AND o_totalprice BETWEEN 424696.6725527435 AND 427493.28640145843
       AND o_orderkey = l_orderkey;

SELECT 1736,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_tax BETWEEN 0.05365973424643771 AND 0.06674818715447284
       AND l_receiptdate BETWEEN 'Thu Apr 15 09:36:22 PDT 1993' AND 'Tue Mar 29 08:36:22 PST 1994';

SELECT 1743,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_tax BETWEEN 0.019320077472157717 AND 0.03073517975773865;

SELECT 1745,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_quantity BETWEEN 16.870090688648162 AND 22.6921534733475
       AND l_commitdate BETWEEN 'Mon Dec 26 07:43:52 PST 1994' AND 'Wed Jan 10 07:43:52 PST 1996';

SELECT 1750,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_discount BETWEEN 0.02769912562390735 AND 0.03859923581119491
       AND l_extendedprice BETWEEN 40740.01847300819 AND 57080.859437769905;

SELECT 1752,
       COUNT(*)
FROM   tpch.partsupp
WHERE  ps_supplycost BETWEEN 787.9814475136031 AND 947.086919984512
       AND ps_availqty BETWEEN 5937 AND 7519;

SELECT 1753,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_receiptdate BETWEEN 'Tue Mar 10 12:07:50 PST 1998' AND 'Wed Dec 16 12:07:50 PST 1998'
       AND l_tax BETWEEN 0.07810416526621412 AND 0.08910786431604856;

SELECT 1757,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_tax BETWEEN 0.00975969681828734 AND 0.023846437264969295;

SELECT 1758,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_quantity BETWEEN 14.800052833053496 AND 15.127610635807432
       AND l_receiptdate BETWEEN 'Sat Jan 04 02:43:21 PST 1997' AND 'Fri Nov 21 02:43:21 PST 1997'
       AND l_tax BETWEEN 0.019978676608192282 AND 0.029285899575620347;

SELECT 1759,
       COUNT(*)
FROM   tpch.supplier,
       tpch.nation,
       tpch.customer,
       tpch.orders
WHERE  c_acctbal BETWEEN 4500.873227707883 AND 6351.645136741093
       AND o_orderdate BETWEEN 'Fri Nov 05 07:39:06 PST 1993' AND 'Sat Nov 20 07:39:06 PST 1993'
       AND c_custkey = o_custkey
       AND n_nationkey = c_nationkey
       AND n_nationkey = s_nationkey;

SELECT 1761,
       COUNT(*)
FROM   tpch.part
WHERE  p_size BETWEEN 17 AND 23
       AND p_retailprice BETWEEN 1337.5058414677032 AND 1516.5728552959438;

SELECT 1762,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.part,
       tpch.lineitem,
       tpch.orders
WHERE  p_size BETWEEN 29 AND 29
       AND p_retailprice BETWEEN 1269.472447746158 AND 1440.8117400932254
       AND o_orderdate BETWEEN 'Tue Jun 16 15:58:45 PDT 1998' AND 'Sun Apr 04 15:58:45 PDT 1999'
       AND o_totalprice BETWEEN 39533.306503719294 AND 44662.347871495906
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND o_orderkey = l_orderkey
       AND p_partkey = ps_partkey;

SELECT 1768,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  o_orderdate BETWEEN 'Sat Dec 21 10:45:12 PST 1996' AND 'Sun Nov 02 10:45:12 PST 1997'
       AND o_totalprice BETWEEN 260995.83835022585 AND 349119.3359266861
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND c_custkey = o_custkey;

SELECT 1769,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_shipdate BETWEEN 'Thu Jun 13 10:16:59 PDT 1996' AND 'Sat May 03 10:16:59 PDT 1997'
       AND l_discount BETWEEN 0.05946297582945621 AND 0.07942087241620747;

SELECT 1771,
       COUNT(*)
FROM   tpch.part
WHERE  p_retailprice BETWEEN 1679.3376370497645 AND 1684.873295019448
       AND p_size BETWEEN 10 AND 17;

SELECT 1772,
       COUNT(*)
FROM   tpch.orders
WHERE  o_orderdate BETWEEN 'Sun Apr 26 09:36:15 PDT 1998' AND 'Thu Dec 24 08:36:15 PST 1998'
       AND o_totalprice BETWEEN 275604.16499621083 AND 278763.7540071462;

SELECT 1773,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_receiptdate BETWEEN 'Thu Sep 26 05:04:36 PDT 1996' AND 'Wed Oct 02 05:04:36 PDT 1996'
       AND l_quantity BETWEEN 4.2701528465070595 AND 12.263044574221354;

SELECT 1777,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  ps_availqty BETWEEN 2832 AND 4588
       AND ps_supplycost BETWEEN 211.90703941751514 AND 410.2431624071234
       AND l_receiptdate BETWEEN 'Sun Mar 01 09:09:49 PST 1998' AND 'Wed Mar 18 09:09:49 PST 1998'
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND c_custkey = o_custkey;

SELECT 1781,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_receiptdate BETWEEN 'Thu Sep 26 05:04:36 PDT 1996' AND 'Wed Oct 02 05:04:36 PDT 1996'
       AND l_quantity BETWEEN 4.2701528465070595 AND 12.263044574221354;

SELECT 1785,
       COUNT(*)
FROM   tpch.part
WHERE  p_size BETWEEN 26 AND 34
       AND p_retailprice BETWEEN 1016.6361294190707 AND 1241.5636583305115;

SELECT 1789,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_shipdate BETWEEN 'Tue Jul 01 02:08:02 PDT 1997' AND 'Wed Jul 16 02:08:02 PDT 1997'
       AND l_commitdate BETWEEN 'Sun May 11 01:12:03 PDT 1997' AND 'Fri Aug 07 01:12:03 PDT 1998'
       AND l_extendedprice BETWEEN 101545.56508482285 AND 119167.05443846797;

SELECT 1794,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  o_totalprice BETWEEN 259781.0982520854 AND 347302.42927411303
       AND o_orderdate BETWEEN 'Tue Nov 21 07:00:52 PST 1995' AND 'Sun Aug 04 08:00:52 PDT 1996'
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 1795,
       COUNT(*)
FROM   tpch.nation,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  c_acctbal BETWEEN 9411.061842898474 AND 9460.345503464692
       AND c_custkey = o_custkey
       AND o_orderkey = l_orderkey
       AND n_nationkey = c_nationkey;

SELECT 1801,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_receiptdate BETWEEN 'Fri Aug 19 06:31:14 PDT 1994' AND 'Fri Nov 10 05:31:14 PST 1995'
       AND l_tax BETWEEN 0.009224182225769458 AND 0.02282724558028025;

SELECT 1802,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_shipdate BETWEEN 'Thu Jun 03 17:59:15 PDT 1993' AND 'Sun Jun 13 17:59:15 PDT 1993';

SELECT 1812,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_quantity BETWEEN 23.53447813903498 AND 30.653385051334475
       AND l_commitdate BETWEEN 'Sun Jan 29 00:11:06 PST 1995' AND 'Sat Feb 18 00:11:06 PST 1995';

SELECT 1816,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_commitdate BETWEEN 'Wed Dec 10 23:27:01 PST 1997' AND 'Sun Feb 21 23:27:01 PST 1999';

SELECT 1817,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_quantity BETWEEN 28.372703615237146 AND 36.13164816944917
       AND l_shipdate BETWEEN 'Mon Dec 19 21:36:55 PST 1994' AND 'Thu Dec 29 21:36:55 PST 1994'
       AND l_discount BETWEEN 0.06421989032232882 AND 0.07444741697264684;

SELECT 1820,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_commitdate BETWEEN 'Fri Nov 22 15:33:52 PST 1996' AND 'Mon Nov 25 15:33:52 PST 1996';

SELECT 1835,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_commitdate BETWEEN 'Thu May 21 18:17:46 PDT 1992' AND 'Sat Mar 13 17:17:46 PST 1993'
       AND l_extendedprice BETWEEN 27575.850613386487 AND 27736.751008600484;

SELECT 1840,
       COUNT(*)
FROM   tpch.part
WHERE  p_size BETWEEN 36 AND 41
       AND p_retailprice BETWEEN 1213.0049470875724 AND 1375.9603404929476;

SELECT 1844,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_extendedprice BETWEEN 75951.27727765717 AND 76491.75691170825;

SELECT 1858,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_discount BETWEEN 0.0727439445357074 AND 0.0905724430384972
       AND l_receiptdate BETWEEN 'Mon Sep 11 01:53:19 PDT 1995' AND 'Fri Sep 15 01:53:19 PDT 1995'
       AND l_tax BETWEEN 0.011610299825874072 AND 0.02142563462032334;

SELECT 1860,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  ps_availqty BETWEEN 859 AND 1951
       AND ps_supplycost BETWEEN 113.82100309397784 AND 252.0539059321605
       AND l_tax BETWEEN 0.042018058547119284 AND 0.05567734174045606
       AND o_totalprice BETWEEN 121507.01168426803 AND 190817.65597272152
       AND o_orderdate BETWEEN 'Sat May 30 03:11:16 PDT 1998' AND 'Fri Jun 05 03:11:16 PDT 1998'
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 1864,
       COUNT(*)
FROM   tpch.partsupp
WHERE  ps_availqty BETWEEN 3126 AND 3152;

SELECT 1870,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  o_totalprice BETWEEN 93135.21464193477 AND 186953.79549345287
       AND o_orderdate BETWEEN 'Wed May 17 07:09:41 PDT 1995' AND 'Tue May 30 07:09:41 PDT 1995'
       AND o_orderkey = l_orderkey;

SELECT 1871,
       COUNT(*)
FROM   tpch.orders
WHERE  o_orderdate BETWEEN 'Tue Aug 19 00:21:53 PDT 1997' AND 'Fri May 08 00:21:53 PDT 1998'
       AND o_totalprice BETWEEN 350668.6312286037 AND 441630.0465439123;

SELECT 1877,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem
WHERE  ps_supplycost BETWEEN 278.26779425298673 AND 284.46310285973107
       AND ps_availqty BETWEEN 5576 AND 7441
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 1881,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_discount BETWEEN 0.00977478456292069 AND 0.010295920335178295
       AND l_tax BETWEEN 0.07278252516414709 AND 0.0814194504804247
       AND l_commitdate BETWEEN 'Tue Mar 01 09:54:58 PST 1994' AND 'Sun Mar 06 09:54:58 PST 1994';

SELECT 1885,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_discount BETWEEN 0.009566597641386133 AND 0.021633384803186058;

SELECT 1886,
       COUNT(*)
FROM   tpch.orders
WHERE  o_orderdate BETWEEN 'Tue Aug 26 12:26:44 PDT 1997' AND 'Thu Nov 19 11:26:44 PST 1998'
       AND o_totalprice BETWEEN 182499.45713470798 AND 270458.9819044935;

SELECT 1889,
       COUNT(*)
FROM   tpch.part
WHERE  p_retailprice BETWEEN 1045.4005801818685 AND 1057.142294650524;

SELECT 1894,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  o_orderdate BETWEEN 'Tue Jun 28 17:29:02 PDT 1994' AND 'Sun Jul 23 17:29:02 PDT 1995'
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND c_custkey = o_custkey;

SELECT 1895,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  o_orderdate BETWEEN 'Wed Nov 15 10:57:53 PST 1995' AND 'Mon Nov 11 10:57:53 PST 1996'
       AND o_totalprice BETWEEN 22855.643095777486 AND 95372.31888765944
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 1897,
       COUNT(*)
FROM   tpch.nation,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  l_commitdate BETWEEN 'Wed Jan 06 00:51:48 PST 1993' AND 'Fri Apr 15 01:51:48 PDT 1994'
       AND l_quantity BETWEEN 45.71507066975066 AND 51.27362794843792
       AND o_orderdate BETWEEN 'Fri Jun 12 04:04:51 PDT 1992' AND 'Sat Jun 19 04:04:51 PDT 1993'
       AND o_totalprice BETWEEN 145734.98234268453 AND 233202.77418591047
       AND c_custkey = o_custkey
       AND n_nationkey = c_nationkey
       AND o_orderkey = l_orderkey;

SELECT 1902,
       COUNT(*)
FROM   tpch.supplier,
       tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  l_discount BETWEEN 0.07902492285060723 AND 0.08931660828123215
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND s_suppkey = ps_suppkey
       AND o_orderkey = l_orderkey;

SELECT 1903,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  l_quantity BETWEEN 40.94568173647773 AND 41.38803202364948
       AND o_totalprice BETWEEN 98927.207532111 AND 176261.45147683244
       AND o_orderdate BETWEEN 'Fri Jul 15 03:00:34 PDT 1994' AND 'Fri Mar 17 02:00:34 PST 1995'
       AND o_orderkey = l_orderkey;

SELECT 1910,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_commitdate BETWEEN 'Sat Jun 06 04:20:43 PDT 1992' AND 'Thu Jun 11 04:20:43 PDT 1992'
       AND l_extendedprice BETWEEN 67986.18183079733 AND 68762.79551058913
       AND l_tax BETWEEN 0.02236616240876386 AND 0.03195029958369945;

SELECT 1911,
       COUNT(*)
FROM   tpch.orders
WHERE  o_totalprice BETWEEN 138869.64529159525 AND 225113.26095612906
       AND o_orderdate BETWEEN 'Tue Aug 12 11:41:09 PDT 1997' AND 'Wed Sep 16 11:41:09 PDT 1998';

SELECT 1912,
       COUNT(*)
FROM   tpch.orders
WHERE  o_orderdate BETWEEN 'Sat May 18 18:27:20 PDT 1996' AND 'Mon May 20 18:27:20 PDT 1996';

SELECT 1913,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  l_extendedprice BETWEEN 25885.85788321864 AND 26842.413065397235
       AND o_orderkey = l_orderkey;

SELECT 1916,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_extendedprice BETWEEN 74717.7540666211 AND 74879.04890005288;

SELECT 1918,
       COUNT(*)
FROM   tpch.supplier,
       tpch.partsupp,
       tpch.part,
       tpch.lineitem
WHERE  s_acctbal BETWEEN -930.6459391225465 AND -900.3640438332797
       AND ps_availqty BETWEEN 5695 AND 5732
       AND ps_supplycost BETWEEN 193.69080923147104 AND 201.91171025428355
       AND l_commitdate BETWEEN 'Mon Apr 17 03:36:40 PDT 1995' AND 'Sat Jan 06 02:36:40 PST 1996'
       AND p_partkey = ps_partkey
       AND s_suppkey = ps_suppkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 1924,
       COUNT(*)
FROM   tpch.supplier,
       tpch.partsupp,
       tpch.lineitem,
       tpch.orders
WHERE  s_acctbal BETWEEN 9354.205773594793 AND 9375.242193592565
       AND ps_supplycost BETWEEN 191.1608127162495 AND 365.8819632046876
       AND ps_availqty BETWEEN 5589 AND 7235
       AND l_extendedprice BETWEEN 53048.592958189634 AND 70474.94917497836
       AND l_tax BETWEEN 0.011359956784770073 AND 0.024027531518139144
       AND o_orderkey = l_orderkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey
       AND s_suppkey = ps_suppkey;

SELECT 1926,
       COUNT(*)
FROM   tpch.nation,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  c_acctbal BETWEEN 9411.061842898474 AND 9460.345503464692
       AND c_custkey = o_custkey
       AND o_orderkey = l_orderkey
       AND n_nationkey = c_nationkey;

SELECT 1928,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_extendedprice BETWEEN 45145.176819492575 AND 65836.71176580846
       AND l_commitdate BETWEEN 'Wed Feb 23 16:09:07 PST 1994' AND 'Sat Mar 12 16:09:07 PST 1994'
       AND l_tax BETWEEN 0.01169657582094306 AND 0.025459364889956884;

SELECT 1932,
       COUNT(*)
FROM   tpch.customer
WHERE  c_acctbal BETWEEN 6494.78495527637 AND 6576.996392510281;

SELECT 1936,
       COUNT(*)
FROM   tpch.orders
WHERE  o_orderdate BETWEEN 'Sat May 18 18:27:20 PDT 1996' AND 'Mon May 20 18:27:20 PDT 1996';

SELECT 1941,
       COUNT(*)
FROM   tpch.customer,
       tpch.orders
WHERE  c_acctbal BETWEEN 2827.8060294038987 AND 4537.135052170672
       AND c_custkey = o_custkey;

SELECT 1946,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_discount BETWEEN 0.05223153458880932 AND 0.06269340227537365;

SELECT 1958,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_commitdate BETWEEN 'Fri Nov 22 15:33:52 PST 1996' AND 'Mon Nov 25 15:33:52 PST 1996';

SELECT 1959,
       COUNT(*)
FROM   tpch.orders
WHERE  o_orderdate BETWEEN 'Tue Aug 26 12:26:44 PDT 1997' AND 'Thu Nov 19 11:26:44 PST 1998'
       AND o_totalprice BETWEEN 182499.45713470798 AND 270458.9819044935;

SELECT 1971,
       COUNT(*)
FROM   tpch.lineitem,
       tpch.orders
WHERE  l_quantity BETWEEN 40.94568173647773 AND 41.38803202364948
       AND o_totalprice BETWEEN 98927.207532111 AND 176261.45147683244
       AND o_orderdate BETWEEN 'Fri Jul 15 03:00:34 PDT 1994' AND 'Fri Mar 17 02:00:34 PST 1995'
       AND o_orderkey = l_orderkey;

SELECT 1976,
       COUNT(*)
FROM   tpch.part
WHERE  p_size BETWEEN 3 AND 3;

SELECT 1981,
       COUNT(*)
FROM   tpch.partsupp,
       tpch.customer,
       tpch.lineitem,
       tpch.orders
WHERE  ps_supplycost BETWEEN 265.1088339687791 AND 270.5073132067024
       AND ps_availqty BETWEEN 6974 AND 8838
       AND o_orderdate BETWEEN 'Thu Aug 26 09:37:27 PDT 1993' AND 'Wed Sep 08 09:37:27 PDT 1993'
       AND o_orderkey = l_orderkey
       AND c_custkey = o_custkey
       AND ps_partkey = l_partkey
       AND ps_suppkey = l_suppkey;

SELECT 1993,
       COUNT(*)
FROM   tpch.supplier,
       tpch.partsupp,
       tpch.part
WHERE  p_size BETWEEN 50 AND 50
       AND p_retailprice BETWEEN 1225.4004309559232 AND 1227.3407042000536
       AND s_suppkey = ps_suppkey
       AND p_partkey = ps_partkey;

SELECT 1999,
       COUNT(*)
FROM   tpch.lineitem
WHERE  l_tax BETWEEN 0.004443446666703999 AND 0.013511649194103945
       AND l_extendedprice BETWEEN 55492.30464162413 AND 70543.40723972487; 
