


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



UPDATE tpch.lineitem
   SET l_tax = l_tax - 0.000001
 WHERE tpch.lineitem.l_extendedprice BETWEEN
       65522.37840623342 AND 66256.94369066914;

UPDATE tpch.lineitem
   SET l_extendedprice = l_extendedprice - 0.000001
 WHERE tpch.lineitem.l_shipdate BETWEEN 'Fri Aug 18 23:48:58 PDT 1995' AND
                                        'Tue Sep 05 23:48:58 PDT 1995';

UPDATE tpch.lineitem
   SET l_tax = l_tax - 0.000001
 WHERE tpch.lineitem.l_commitdate BETWEEN 'Tue Jun 30 22:07:36 PDT 1992' AND
                                          'Fri Jul 17 22:07:36 PDT 1992';

UPDATE tpch.orders
   SET o_shippriority = o_shippriority - 1
 WHERE tpch.orders.o_orderdate BETWEEN 'Thu Apr 07 05:46:45 PDT 1994' AND
                                       'Thu Apr 14 05:46:45 PDT 1994';

UPDATE tpch.lineitem
   SET l_quantity = l_quantity - 0.000001
 WHERE tpch.lineitem.l_receiptdate BETWEEN
       'Fri Oct 25 19:36:34 PDT 1996' AND 'Mon Oct 28 18:36:34 PST 1996';

UPDATE tpch.partsupp
   SET ps_supplycost = ps_supplycost - 0.000001
 WHERE tpch.partsupp.ps_availqty BETWEEN 1425 AND 1464;

UPDATE tpch.lineitem
   SET l_extendedprice = l_extendedprice - 0.000001
 WHERE tpch.lineitem.l_commitdate BETWEEN 'Tue May 24 21:09:40 PDT 1994' AND
                                          'Fri Jun 17 21:09:40 PDT 1994';

UPDATE tpch.partsupp
   SET ps_supplycost = ps_supplycost - 0.000001
 WHERE tpch.partsupp.ps_availqty BETWEEN 6777 AND 6844;

UPDATE tpch.lineitem
   SET l_extendedprice = l_extendedprice - 0.000001
 WHERE tpch.lineitem.l_commitdate BETWEEN 'Mon Jul 27 04:39:49 PDT 1998' AND
                                          'Sat Aug 08 04:39:49 PDT 1998';

UPDATE tpch.lineitem
   SET l_quantity = l_quantity - 0.000001
 WHERE tpch.lineitem.l_receiptdate BETWEEN
       'Mon Oct 06 10:20:41 PDT 1997' AND 'Mon Oct 27 09:20:41 PST 1997';

UPDATE tpch.lineitem
   SET l_tax = l_tax - 0.000001
 WHERE tpch.lineitem.l_extendedprice BETWEEN
       37019.781528540014 AND 37378.021592062534;

UPDATE tpch.lineitem
   SET l_quantity = l_quantity - 0.000001
 WHERE tpch.lineitem.l_commitdate BETWEEN 'Sat Sep 26 14:56:02 PDT 1998' AND
                                          'Fri Jan 28 13:56:02 PST 2000';

UPDATE tpch.lineitem
   SET l_extendedprice = l_extendedprice - 0.000001
 WHERE tpch.lineitem.l_shipdate BETWEEN 'Sun May 15 23:25:44 PDT 1994' AND
                                        'Sat May 21 23:25:44 PDT 1994';

UPDATE tpch.lineitem
   SET l_extendedprice = l_extendedprice - 0.000001
 WHERE tpch.lineitem.l_shipdate BETWEEN 'Mon Apr 05 09:51:35 PDT 1993' AND
                                        'Tue Apr 27 09:51:35 PDT 1993';

UPDATE tpch.lineitem
   SET l_quantity = l_quantity - 0.000001
 WHERE tpch.lineitem.l_receiptdate BETWEEN
       'Mon Jul 17 14:09:02 PDT 1995' AND 'Thu Jul 20 14:09:02 PDT 1995';

UPDATE tpch.lineitem
   SET l_quantity = l_quantity - 0.000001
 WHERE tpch.lineitem.l_receiptdate BETWEEN
       'Sun Dec 20 01:15:21 PST 1992' AND 'Mon Dec 28 01:15:21 PST 1992';

UPDATE tpch.lineitem
   SET l_extendedprice = l_extendedprice - 0.000001
 WHERE tpch.lineitem.l_commitdate BETWEEN 'Tue May 24 21:09:40 PDT 1994' AND
                                          'Fri Jun 17 21:09:40 PDT 1994';

UPDATE tpch.lineitem
   SET l_tax = l_tax - 0.000001
 WHERE tpch.lineitem.l_receiptdate BETWEEN
       'Sun Aug 07 20:53:40 PDT 1994' AND 'Wed Aug 31 20:53:40 PDT 1994';

UPDATE tpch.orders
   SET o_shippriority = o_shippriority - 1
 WHERE tpch.orders.o_orderdate BETWEEN 'Tue Mar 22 22:48:58 PST 1994' AND
                                       'Wed Mar 23 22:48:58 PST 1994';

UPDATE tpch.partsupp
   SET ps_availqty = ps_availqty - 1
 WHERE tpch.partsupp.ps_supplycost BETWEEN
       452.2498448729841 AND 459.8000030939402;







