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
 WHERE tpch.partsupp.ps_supplycost BETWEEN
       343.38192329849375 AND 352.89716370586683;

UPDATE tpch.partsupp
   SET ps_supplycost = ps_supplycost - 0.000001
 WHERE tpch.partsupp.ps_supplycost BETWEEN
       516.8553082806344 AND 520.7931494478674;

UPDATE tpch.orders
   SET o_totalprice = o_totalprice - 0.000001
 WHERE tpch.orders.o_totalprice BETWEEN 235736.15582258778 AND 236778.1602355858;

UPDATE tpch.partsupp
   SET ps_supplycost = ps_supplycost - 0.000001
 WHERE tpch.partsupp.ps_availqty BETWEEN 1425 AND 1464;

UPDATE tpch.lineitem
   SET l_extendedprice = l_extendedprice - 0.000001
 WHERE tpch.lineitem.l_commitdate BETWEEN 'Tue May 24 21:09:40 PDT 1994' AND
                                          'Fri Jun 17 21:09:40 PDT 1994';
