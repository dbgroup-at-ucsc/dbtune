 INSERT INTO tpch.lineitem VALUES(9,127857,5394,1,45,84818.25,0.09,0.05,'N','O','1998-10-20','1998-09-10','1998-11-15','COLLECT COD','SHIP','es haggle blithely above the silent ac');
 INSERT INTO tpch.lineitem VALUES(9,92115,9643,2,47,52034.17,0.08,0.02,'N','O','1998-09-08','1998-08-31','1998-09-15','COLLECT COD','AIR','counts. furio');
 INSERT INTO tpch.lineitem VALUES(10,183288,843,1,13,17826.64,0.01,0.07,'N','O','1996-10-29','1996-11-29','1996-10-31','DELIVER IN PERSON','AIR','ackages haggle slyly. bold, exp');
 INSERT INTO tpch.lineitem VALUES(10,117623,135,2,39,63984.18,0.03,0.02,'N','O','1997-01-06','1996-11-26','1997-02-03','TAKE BACK RETURN','TRUCK',' final platel');
 INSERT INTO tpch.lineitem VALUES(10,83486,1011,3,34,49962.32,0.04,0.00,'N','O','1996-10-13','1996-11-14','1996-11-04','COLLECT COD','REG AIR','ar instructio');
 INSERT INTO tpch.lineitem VALUES(11,82555,5064,1,21,32288.55,0.04,0.08,'A','F','1993-06-26','1993-08-23','1993-07-07','NONE','RAIL','cajole slyly ironic deposits. fluffil');
 INSERT INTO tpch.lineitem VALUES(11,87018,9527,2,30,30150.30,0.01,0.03,'A','F','1993-10-08','1993-08-28','1993-10-28','COLLECT COD','TRUCK','have to cajole furiously: special, fina');
 INSERT INTO tpch.lineitem VALUES(11,101742,6763,3,46,80212.04,0.09,0.08,'R','F','1993-09-20','1993-08-30','1993-09-26','NONE','AIR','g packages');
 INSERT INTO tpch.lineitem VALUES(11,169644,2161,4,32,54836.48,0.04,0.03,'R','F','1993-08-07','1993-08-28','1993-08-11','COLLECT COD','FOB','ven excuses cajole ideas');
 INSERT INTO tpch.lineitem VALUES(11,198586,1106,5,23,38745.34,0.10,0.04,'A','F','1993-10-09','1993-07-15','1993-11-08','COLLECT COD','SHIP',' express pinto b');
 DELETE FROM TPCH.ORDERS WHERE O_ORDERKEY = 1;
 DELETE FROM TPCH.ORDERS WHERE O_ORDERKEY = 2;
 DELETE FROM TPCH.ORDERS WHERE O_ORDERKEY = 3;
 DELETE FROM TPCH.ORDERS WHERE O_ORDERKEY = 4;
 DELETE FROM TPCH.ORDERS WHERE O_ORDERKEY = 5;
 DELETE FROM TPCH.ORDERS WHERE O_ORDERKEY = 6;
 DELETE FROM TPCH.ORDERS WHERE O_ORDERKEY = 7;
 DELETE FROM TPCH.ORDERS WHERE O_ORDERKEY = 32;
 DELETE FROM TPCH.ORDERS WHERE O_ORDERKEY = 33;
 DELETE FROM TPCH.ORDERS WHERE O_ORDERKEY = 34;
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

