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

UPDATE tpch.lineitem
   SET l_extendedprice = l_extendedprice - 0.000001
 WHERE tpch.lineitem.l_receiptdate BETWEEN
       'Wed Apr 28 02:31:05 PDT 1993' AND 'Wed May 05 02:31:05 PDT 1993';

UPDATE tpch.lineitem
   SET l_quantity = l_quantity - 0.000001
 WHERE tpch.lineitem.l_shipdate BETWEEN 'Wed Jun 10 06:34:55 PDT 1998' AND
                                        'Sun Jun 28 06:34:55 PDT 1998';

UPDATE tpch.orders
   SET o_shippriority = o_shippriority - 1
 WHERE tpch.orders.o_totalprice BETWEEN 477708.8460834578 AND 584509.4689130272;

UPDATE tpch.lineitem
   SET l_discount = l_discount - 0.000001
 WHERE tpch.lineitem.l_receiptdate BETWEEN
       'Wed Sep 21 04:20:01 PDT 1994' AND 'Fri Oct 07 04:20:01 PDT 1994';

UPDATE tpch.orders
   SET o_shippriority = o_shippriority - 1
 WHERE tpch.orders.o_totalprice BETWEEN 477708.8460834578 AND 584509.4689130272;

UPDATE tpch.lineitem
   SET l_quantity = l_quantity - 0.000001
 WHERE tpch.lineitem.l_receiptdate BETWEEN
       'Fri Feb 25 12:12:36 PST 1994' AND 'Sun Mar 06 12:12:36 PST 1994';

UPDATE tpch.lineitem
   SET l_tax = l_tax - 0.000001
 WHERE tpch.lineitem.l_commitdate BETWEEN 'Mon Jan 16 17:17:45 PST 1995' AND
                                          'Fri Feb 03 17:17:45 PST 1995';

UPDATE tpch.lineitem
   SET l_tax = l_tax - 0.000001
 WHERE tpch.lineitem.l_extendedprice BETWEEN
       52830.786489814825 AND 53552.16450816543;

UPDATE tpch.orders
   SET o_totalprice = o_totalprice + 0.000001
 WHERE tpch.orders.o_orderdate BETWEEN 'Tue Dec 03 21:19:36 PST 1996' AND
                                       'Thu Dec 19 21:19:36 PST 1996';

UPDATE tpch.lineitem
   SET l_discount = l_discount + 0.000001
 WHERE tpch.lineitem.l_extendedprice BETWEEN
       13294.572484763343 AND 13617.514467619252;

UPDATE tpch.lineitem
   SET l_tax = l_tax + 0.000001
 WHERE tpch.lineitem.l_extendedprice BETWEEN
       72967.20560888002 AND 73920.43709064345;

UPDATE tpch.lineitem
   SET l_quantity = l_quantity + 0.000001
 WHERE tpch.lineitem.l_extendedprice BETWEEN
       64803.95256364816 AND 65740.87743623629;

UPDATE tpch.lineitem
   SET l_quantity = l_quantity + 0.000001
 WHERE tpch.lineitem.l_shipdate BETWEEN 'Mon Jun 19 03:08:09 PDT 1995' AND
                                        'Sat Jun 24 03:08:09 PDT 1995';

UPDATE tpch.lineitem
   SET l_tax = l_tax + 0.000001
 WHERE tpch.lineitem.l_extendedprice BETWEEN
       52830.786489814825 AND 53552.16450816543;

UPDATE tpch.lineitem
   SET l_discount = l_discount + 0.000001
 WHERE tpch.lineitem.l_receiptdate BETWEEN
       'Wed Jul 22 10:23:07 PDT 1992' AND 'Wed Jul 22 10:23:07 PDT 1992';

UPDATE tpch.lineitem
   SET l_discount = l_discount + 0.000001
 WHERE tpch.lineitem.l_extendedprice BETWEEN
       25328.646749630512 AND 25572.18925091511;

UPDATE tpch.lineitem
   SET l_extendedprice = l_extendedprice + 0.000001
 WHERE tpch.lineitem.l_commitdate BETWEEN 'Tue May 11 13:07:31 PDT 1993' AND
                                          'Thu May 13 13:07:31 PDT 1993';

UPDATE tpch.lineitem
   SET l_extendedprice = l_extendedprice + 0.000001
 WHERE tpch.lineitem.l_shipdate BETWEEN 'Mon Dec 14 10:58:14 PST 1992' AND
                                        'Tue Dec 15 10:58:14 PST 1992';

UPDATE tpch.lineitem
   SET l_extendedprice = l_extendedprice + 0.000001
 WHERE tpch.lineitem.l_receiptdate BETWEEN
       'Fri Aug 27 07:22:29 PDT 1993' AND 'Sun Sep 12 07:22:29 PDT 1993';

UPDATE tpch.lineitem
   SET l_discount = l_discount + 0.000001
 WHERE tpch.lineitem.l_receiptdate BETWEEN
       'Sun Feb 21 09:21:30 PST 1993' AND 'Sat Mar 06 09:21:30 PST 1993';

UPDATE tpch.lineitem
   SET l_quantity = l_quantity + 0.000001
 WHERE tpch.lineitem.l_commitdate BETWEEN 'Sun Jul 02 22:45:19 PDT 1995' AND
                                          'Mon Jul 17 22:45:19 PDT 1995';

UPDATE tpch.partsupp
   SET ps_supplycost = ps_supplycost + 0.000001
 WHERE tpch.partsupp.ps_availqty BETWEEN 8524 AND 8565;

UPDATE tpch.lineitem
   SET l_tax = l_tax + 0.000001
 WHERE tpch.lineitem.l_shipdate BETWEEN 'Wed Oct 15 20:43:12 PDT 1997' AND
                                        'Thu Oct 16 20:43:12 PDT 1997';

UPDATE tpch.lineitem
   SET l_tax = l_tax + 0.000001
 WHERE tpch.lineitem.l_commitdate BETWEEN 'Fri Jul 19 18:46:43 PDT 1996' AND
                                          'Sun Aug 11 18:46:43 PDT 1996';

UPDATE tpch.lineitem
   SET l_quantity = l_quantity + 0.000001
 WHERE tpch.lineitem.l_extendedprice BETWEEN
       78335.53203042406 AND 79154.6240933582;

UPDATE tpch.lineitem
   SET l_extendedprice = l_extendedprice + 0.000001
 WHERE tpch.lineitem.l_shipdate BETWEEN 'Wed Apr 22 17:44:35 PDT 1992' AND
                                        'Wed May 13 17:44:35 PDT 1992';

UPDATE tpch.partsupp
   SET ps_availqty = ps_availqty + 1
 WHERE tpch.partsupp.ps_supplycost BETWEEN
       331.42090722103404 AND 335.2362587135389;

UPDATE tpch.lineitem
   SET l_tax = l_tax + 0.000001
 WHERE tpch.lineitem.l_commitdate BETWEEN 'Tue Jun 30 22:07:36 PDT 1992' AND
                                          'Fri Jul 17 22:07:36 PDT 1992';

UPDATE tpch.lineitem
   SET l_quantity = l_quantity + 0.000001
 WHERE tpch.lineitem.l_shipdate BETWEEN 'Mon Jan 06 05:10:02 PST 1992' AND
                                        'Sun Jan 26 05:10:02 PST 1992';

UPDATE tpch.lineitem
   SET l_quantity = l_quantity + 0.000001
 WHERE tpch.lineitem.l_commitdate BETWEEN 'Mon Nov 06 15:29:47 PST 1995' AND
                                          'Tue Nov 14 15:29:47 PST 1995';

UPDATE tpch.partsupp
   SET ps_supplycost = ps_supplycost + 0.000001
 WHERE tpch.partsupp.ps_availqty BETWEEN 6049 AND 6102;

UPDATE tpch.lineitem
   SET l_extendedprice = l_extendedprice + 0.000001
 WHERE tpch.lineitem.l_commitdate BETWEEN 'Mon Jul 27 04:39:49 PDT 1998' AND
                                          'Sat Aug 08 04:39:49 PDT 1998';

UPDATE tpch.lineitem
   SET l_discount = l_discount + 0.000001
 WHERE tpch.lineitem.l_commitdate BETWEEN 'Wed Aug 14 17:08:16 PDT 1996' AND
                                          'Fri Aug 23 17:08:16 PDT 1996';

UPDATE tpch.orders
   SET o_shippriority = o_shippriority + 1
 WHERE tpch.orders.o_orderdate BETWEEN 'Fri Oct 20 05:05:53 PDT 1995' AND
                                       'Sun Nov 05 04:05:53 PST 1995';

UPDATE tpch.lineitem
   SET l_discount = l_discount + 0.000001
 WHERE tpch.lineitem.l_shipdate BETWEEN 'Sat Apr 29 10:31:45 PDT 1995' AND
                                        'Fri May 19 10:31:45 PDT 1995';

UPDATE tpch.lineitem
   SET l_discount = l_discount + 0.000001
 WHERE tpch.lineitem.l_shipdate BETWEEN 'Thu Mar 05 18:40:44 PST 1992' AND
                                        'Sun Mar 29 18:40:44 PST 1992';

UPDATE tpch.lineitem
   SET l_discount = l_discount + 0.000001
 WHERE tpch.lineitem.l_shipdate BETWEEN 'Tue Apr 04 08:26:47 PDT 1995' AND
                                        'Sun Apr 09 08:26:47 PDT 1995';

UPDATE tpch.lineitem
   SET l_quantity = l_quantity + 0.000001
 WHERE tpch.lineitem.l_extendedprice BETWEEN
       89309.47356988743 AND 90064.69237813987;

UPDATE tpch.lineitem
   SET l_extendedprice = l_extendedprice + 0.000001
 WHERE tpch.lineitem.l_receiptdate BETWEEN
       'Sat Jan 02 02:06:43 PST 1993' AND 'Thu Jan 07 02:06:43 PST 1993';

UPDATE tpch.lineitem
   SET l_discount = l_discount + 0.000001
 WHERE tpch.lineitem.l_commitdate BETWEEN 'Wed Aug 14 17:08:16 PDT 1996' AND
                                          'Fri Aug 23 17:08:16 PDT 1996';

UPDATE tpch.orders
   SET o_shippriority = o_shippriority + 1
 WHERE tpch.orders.o_orderdate BETWEEN 'Tue May 31 03:38:30 PDT 1994' AND
                                       'Tue Jun 21 03:38:30 PDT 1994';

UPDATE tpch.part
   SET p_size = p_size + 1
 WHERE tpch.part.p_retailprice BETWEEN 1158.1053417913936 AND 1167.9252322300965
;

UPDATE tpch.lineitem
   SET l_tax = l_tax + 0.000001
 WHERE tpch.lineitem.l_extendedprice BETWEEN
       104019.31371382915 AND 104082.1455424701;

UPDATE tpch.lineitem
   SET l_quantity = l_quantity + 0.000001
 WHERE tpch.lineitem.l_receiptdate BETWEEN
       'Sun Dec 20 01:15:21 PST 1992' AND 'Mon Dec 28 01:15:21 PST 1992';

UPDATE tpch.lineitem
   SET l_discount = l_discount + 0.000001
 WHERE tpch.lineitem.l_commitdate BETWEEN 'Wed Aug 14 17:08:16 PDT 1996' AND
                                          'Fri Aug 23 17:08:16 PDT 1996'; 

--UPDATE tpch.partsupp
   --SET ps_supplycost = ps_supplycost - 0.000001
 --WHERE tpch.partsupp.ps_supplycost BETWEEN
       --343.38192329849375 AND 352.89716370586683;

--UPDATE tpch.partsupp
   --SET ps_supplycost = ps_supplycost - 0.000001
 --WHERE tpch.partsupp.ps_supplycost BETWEEN
       --516.8553082806344 AND 520.7931494478674;

--UPDATE tpch.orders
   --SET o_totalprice = o_totalprice - 0.000001
 --WHERE tpch.orders.o_totalprice BETWEEN 235736.15582258778 AND 236778.1602355858;

--UPDATE tpch.customer
   --SET c_acctbal = c_acctbal - 0.000001
 --WHERE tpch.customer.c_acctbal BETWEEN 9714.80387595837 AND 9816.86602176394;

--UPDATE tpch.partsupp
   --SET ps_supplycost = ps_supplycost - 0.000001
 --WHERE tpch.partsupp.ps_supplycost BETWEEN
       --341.0948463496943 AND 350.0665342631519;

--UPDATE tpch.orders
   --SET o_totalprice = o_totalprice - 0.000001
 --WHERE tpch.orders.o_totalprice BETWEEN 235736.15582258778 AND 236778.1602355858
--;

--UPDATE tpch.partsupp
   --SET ps_supplycost = ps_supplycost - 0.000001
 --WHERE tpch.partsupp.ps_supplycost BETWEEN
       --106.49003704475444 AND 111.94124171431024;

--UPDATE tpch.orders
   --SET o_totalprice = o_totalprice - 0.000001
 --WHERE tpch.orders.o_totalprice BETWEEN 235736.15582258778 AND 236778.1602355858;

--UPDATE tpch.part
   --SET p_retailprice = p_retailprice - 0.000001
 --WHERE tpch.part.p_retailprice BETWEEN 1941.435418319549 AND 1946.7336431047843;

--UPDATE tpch.partsupp
   --SET ps_availqty = ps_availqty + 1
 --WHERE tpch.partsupp.ps_availqty BETWEEN 4823 AND 4901;

--UPDATE tpch.customer
   --SET c_acctbal = c_acctbal + 0.000001
 --WHERE tpch.customer.c_acctbal BETWEEN 1034.1745936436641 AND 1107.8793288827928;

--UPDATE tpch.orders
   --SET o_totalprice = o_totalprice + 0.000001
 --WHERE tpch.orders.o_totalprice BETWEEN 311646.2023200287 AND 313986.55191938294;

--UPDATE tpch.lineitem
   --SET l_extendedprice = l_extendedprice + 0.000001
 --WHERE tpch.lineitem.l_extendedprice BETWEEN
       --91949.84953402694 AND 91986.30537361333;

--UPDATE tpch.orders
   --SET o_totalprice = o_totalprice + 0.000001
 --WHERE tpch.orders.o_totalprice BETWEEN 318591.5663842903 AND 323037.87898730097;

--UPDATE tpch.orders
   --SET o_totalprice = o_totalprice + 0.000001
 --WHERE tpch.orders.o_totalprice BETWEEN 318591.5663842903 AND 323037.87898730097;

--UPDATE tpch.orders
   --SET o_totalprice = o_totalprice + 0.000001
 --WHERE tpch.orders.o_totalprice BETWEEN 117439.1743096517 AND 119884.68364198053;
