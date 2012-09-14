SELECT 0, COUNT(*)  FROM tpce.security, tpce.daily_market, tpce.status_type WHERE  tpce.security.s_start_date BETWEEN 'Wed Oct 11 12:47:34 PST 1916' AND 'Sun Jul 29 12:47:34 PST 1934' AND tpce.security.s_exch_date BETWEEN 'Fri Aug 12 16:03:41 PDT 1988' AND 'Fri Jan 16 15:03:41 PST 2004' AND tpce.security.s_52wk_low_date BETWEEN 'Tue Apr 06 06:09:34 PDT 2004' AND 'Wed Jun 02 06:09:34 PDT 2004' AND tpce.daily_market.dm_close BETWEEN 28.329909869607892 AND 28.42669467037347 AND tpce.daily_market.dm_high BETWEEN 28.592793529612266 AND 30.442782809147648 AND tpce.daily_market.dm_vol BETWEEN 9795.153166263999 AND 9863.443168852615 AND tpce.security.s_symb=tpce.daily_market.dm_s_symb AND tpce.status_type.st_id=tpce.security.s_st_id ; 
SELECT 1, COUNT(*)  FROM tpch.orders, tpch.lineitem, tpch.customer WHERE  tpch.lineitem.l_quantity BETWEEN 46.79928318619356 AND 54.084424197386845 AND tpch.lineitem.l_receiptdate BETWEEN 'Sun Oct 15 11:04:29 PDT 1995' AND 'Thu Oct 19 11:04:29 PDT 1995' AND tpch.lineitem.l_extendedprice BETWEEN 92965.82889495825 AND 93931.85215396482 AND tpch.orders.o_orderkey=tpch.lineitem.l_orderkey AND tpch.customer.c_custkey=tpch.orders.o_custkey ; 
--SELECT 2, COUNT(*)  FROM tpcc.orderline, tpcc.stock, tpcc.orders WHERE  tpcc.stock.s_quantity BETWEEN 31.860355526635026 AND 42.95844864665973 AND tpcc.orders.o_carrier_id BETWEEN 2 AND 2 AND tpcc.stock.s_w_id=tpcc.orderline.ol_supply_w_id AND tpcc.stock.s_i_id=tpcc.orderline.ol_i_id AND tpcc.orders.o_w_id=tpcc.orderline.ol_w_id AND tpcc.orders.o_d_id=tpcc.orderline.ol_d_id AND tpcc.orders.o_id=tpcc.orderline.ol_o_id ; 
SELECT 3, COUNT(*)  FROM tpch.supplier, tpch.partsupp, tpch.part WHERE  tpch.supplier.s_acctbal BETWEEN 1697.1037510223762 AND 3529.066531923416 AND tpch.partsupp.ps_supplycost BETWEEN 425.75447107608784 AND 609.213188094174 AND tpch.partsupp.ps_availqty BETWEEN 1356 AND 2833 AND tpch.part.p_retailprice BETWEEN 1261.0674604281671 AND 1404.4468989518643 AND tpch.part.p_size BETWEEN 28 AND 35 AND tpch.supplier.s_suppkey=tpch.partsupp.ps_suppkey AND tpch.part.p_partkey=tpch.partsupp.ps_partkey ; 
SELECT 4, COUNT(*)  FROM tpcc.orderline WHERE  tpcc.orderline.ol_delivery_d BETWEEN 'Tue Mar 18 00:00:00 PDT 2008' AND 'Tue Mar 18 00:00:00 PDT 2008' ; 
SELECT 5, COUNT(*)  FROM tpch.lineitem WHERE  tpch.lineitem.l_extendedprice BETWEEN 79861.36753562656 AND 80707.89082898502 AND tpch.lineitem.l_commitdate BETWEEN 'Sun Dec 12 04:29:36 PST 1993' AND 'Mon Oct 31 04:29:36 PST 1994' ; 
SELECT 6, COUNT(*)  FROM tpch.part WHERE  tpch.part.p_size BETWEEN 3 AND 3 ; 
SELECT 7, COUNT(*)  FROM tpcc.customer WHERE  tpcc.customer.c_since BETWEEN 'Tue Mar 18 00:00:00 PDT 2008' AND 'Tue Mar 18 00:00:00 PDT 2008' AND tpcc.customer.c_discount BETWEEN 0.2584365825053047 AND 0.34321683858966023 ; 
SELECT 8, COUNT(*)  FROM tpch.orders WHERE  tpch.orders.o_totalprice BETWEEN 88840.85427847636 AND 174191.8333814839 AND tpch.orders.o_orderdate BETWEEN 'Thu Feb 19 04:09:56 PST 1998' AND 'Thu Apr 22 05:09:56 PDT 1999' ; 
SELECT 9, COUNT(*)  FROM tpch.lineitem WHERE  tpch.lineitem.l_commitdate BETWEEN 'Wed Dec 10 23:27:01 PST 1997' AND 'Sun Feb 21 23:27:01 PST 1999' ; 
--SELECT 10, COUNT(*)  FROM tpcc.orderline, tpcc.orders WHERE  tpcc.orders.o_ol_cnt BETWEEN 11.33778771795958 AND 12.867589593179797 AND tpcc.orderline.ol_amount BETWEEN 7199.429354863254 AND 7274.359970178637 AND tpcc.orders.o_w_id=tpcc.orderline.ol_w_id AND tpcc.orders.o_d_id=tpcc.orderline.ol_d_id AND tpcc.orders.o_id=tpcc.orderline.ol_o_id ; 
SELECT 11, COUNT(*)  FROM nref.neighboring_seq WHERE  nref.neighboring_seq.end_2 BETWEEN 705 AND 4228 AND nref.neighboring_seq.start_2 BETWEEN 1514 AND 3944 AND nref.neighboring_seq.score BETWEEN 98.32421983346111 AND 98.86002793474663 ; 
SELECT 12, COUNT(*)  FROM tpce.security, tpce.daily_market, tpce.company WHERE  tpce.security.s_start_date BETWEEN 'Tue Oct 02 14:13:32 PDT 1962' AND 'Tue Dec 29 13:13:32 PST 1981' AND tpce.security.s_exch_date BETWEEN 'Mon May 06 04:29:19 PDT 1974' AND 'Wed Feb 26 03:29:19 PST 1992' AND tpce.security.s_52wk_low_date BETWEEN 'Tue May 25 06:32:21 PDT 2004' AND 'Tue Jul 27 06:32:21 PDT 2004' AND tpce.security.s_symb=tpce.daily_market.dm_s_symb AND tpce.company.co_id=tpce.security.s_co_id ; 
SELECT 13, COUNT(*)  FROM tpch.lineitem WHERE  tpch.lineitem.l_commitdate BETWEEN 'Sun Jan 11 20:15:03 PST 1998' AND 'Mon Feb 15 20:15:03 PST 1999' AND tpch.lineitem.l_tax BETWEEN 0.05540154857564852 AND 0.06806450598302848 ; 
SELECT 14, COUNT(*)  FROM tpch.lineitem WHERE  tpch.lineitem.l_receiptdate BETWEEN 'Mon Nov 07 02:35:54 PST 1994' AND 'Fri Dec 08 02:35:54 PST 1995' ; 
SELECT 15, COUNT(*)  FROM nref.protein, nref.neighboring_seq, nref.source WHERE  nref.protein.seq_length BETWEEN 1351 AND 6626 AND nref.protein.last_updated BETWEEN 'Fri Aug 09 22:59:05 PDT 2002' AND 'Tue Aug 13 22:59:05 PDT 2002' AND nref.protein.nref_id=nref.neighboring_seq.nref_id_1 AND nref.protein.nref_id=nref.source.nref_id ; 
SELECT 16, COUNT(*)  FROM nref.neighboring_seq WHERE  nref.neighboring_seq.score BETWEEN 97.13955015890203 AND 97.7860321771585 ; 
SELECT 17, COUNT(*)  FROM tpch.partsupp, tpch.lineitem, tpch.customer, tpch.orders WHERE  tpch.customer.c_acctbal BETWEEN 6677.25489709987 AND 8763.599323234928 AND tpch.orders.o_totalprice BETWEEN 161409.62125961325 AND 161978.7926355417 AND tpch.orders.o_orderdate BETWEEN 'Sun Jan 18 01:09:19 PST 1998' AND 'Sat Jan 30 01:09:19 PST 1999' AND tpch.partsupp.ps_partkey=tpch.lineitem.l_partkey AND tpch.partsupp.ps_suppkey=tpch.lineitem.l_suppkey AND tpch.orders.o_orderkey=tpch.lineitem.l_orderkey AND tpch.customer.c_custkey=tpch.orders.o_custkey ; 
SELECT 18, COUNT(*)  FROM tpce.daily_market WHERE  tpce.daily_market.dm_high BETWEEN 29.305141387953103 AND 31.039837443367315 AND tpce.daily_market.dm_vol BETWEEN 5939.0187586608745 AND 7443.149906332435 ; 
SELECT 19, COUNT(*)  FROM tpce.security, tpce.daily_market WHERE  tpce.daily_market.dm_vol BETWEEN 5323.6657061461 AND 5398.138538643341 AND tpce.daily_market.dm_high BETWEEN 29.26879397661535 AND 29.34907469975082 AND tpce.security.s_symb=tpce.daily_market.dm_s_symb ; 
SELECT 20, COUNT(*)  FROM tpcc.orders WHERE  tpcc.orders.o_entry_d BETWEEN 'Tue Mar 18 00:00:00 PDT 2008' AND 'Tue Mar 18 00:00:00 PDT 2008' AND tpcc.orders.o_carrier_id BETWEEN 8 AND 9 AND tpcc.orders.o_ol_cnt BETWEEN 12.520467617291457 AND 14.231736312535398 ; 
SELECT 21, COUNT(*)  FROM tpce.security, tpce.daily_market, tpce.company, tpce.status_type WHERE  tpce.company.co_open_date BETWEEN 'Wed Oct 16 03:42:34 PDT 1991' AND 'Sat Jul 27 03:42:34 PDT 2013' AND tpce.daily_market.dm_low BETWEEN 20.25000365114954 AND 21.22143698151205 AND tpce.security.s_symb=tpce.daily_market.dm_s_symb AND tpce.status_type.st_id=tpce.security.s_st_id AND tpce.company.co_id=tpce.security.s_co_id ; 
SELECT 22, COUNT(*)  FROM nref.protein, nref.taxonomy WHERE  nref.protein.seq_length BETWEEN 174 AND 3507 AND nref.protein.last_updated BETWEEN 'Fri Apr 05 15:40:29 PST 2002' AND 'Wed May 29 16:40:29 PDT 2002' AND nref.protein.nref_id=nref.taxonomy.nref_id ; 
SELECT 23, COUNT(*)  FROM tpch.lineitem WHERE  tpch.lineitem.l_commitdate BETWEEN 'Wed Dec 10 23:27:01 PST 1997' AND 'Sun Feb 21 23:27:01 PST 1999' ; 
SELECT 24, COUNT(*)  FROM tpch.orders WHERE  tpch.orders.o_totalprice BETWEEN 51265.464079043435 AND 112628.82025791754 AND tpch.orders.o_orderdate BETWEEN 'Thu Apr 06 05:53:23 PDT 1995' AND 'Sat Dec 16 04:53:23 PST 1995' ; 
SELECT 25, COUNT(*)  FROM tpce.security, tpce.daily_market WHERE  tpce.security.s_num_out BETWEEN 3.093069606069822E8 AND 1.538473098952184E9 AND tpce.security.s_52wk_low_date BETWEEN 'Sat Sep 18 18:55:36 PDT 2004' AND 'Tue Sep 21 18:55:36 PDT 2004' AND tpce.daily_market.dm_close BETWEEN 20.576222190670844 AND 20.655707622594797 AND tpce.security.s_symb=tpce.daily_market.dm_s_symb ; 
SELECT 26, COUNT(*)  FROM tpcc.customer WHERE  tpcc.customer.c_since BETWEEN 'Tue Mar 18 00:00:00 PDT 2008' AND 'Tue Mar 18 00:00:00 PDT 2008' AND tpcc.customer.c_discount BETWEEN 0.059401360010014725 AND 0.13870723100099258 ; 
SELECT 27, COUNT(*)  FROM tpch.lineitem, tpch.orders WHERE  tpch.lineitem.l_receiptdate BETWEEN 'Wed Jul 14 10:16:19 PDT 1993' AND 'Fri Jun 17 10:16:19 PDT 1994' AND tpch.lineitem.l_commitdate BETWEEN 'Sat Dec 18 06:24:40 PST 1993' AND 'Thu Feb 23 06:24:40 PST 1995' AND tpch.orders.o_orderkey=tpch.lineitem.l_orderkey ; 
SELECT 28, COUNT(*)  FROM nref.neighboring_seq WHERE  nref.neighboring_seq.ordinal BETWEEN 3289 AND 4726 AND nref.neighboring_seq.overlap_length BETWEEN 131 AND 192 AND nref.neighboring_seq.score BETWEEN 98.67553315876872 AND 99.57521861070478 ; 
-- java.sql.SQLException: Impossible to look for SYSIBM.SQL120503143426870
--SELECT 29, COUNT(*)  FROM tpce.security, tpce.daily_market, tpce.exchange, tpce.watch_item WHERE  tpce.exchange.ex_num_symb BETWEEN 847.5817992961973 AND 852.6585500507161 AND tpce.security.s_start_date BETWEEN 'Fri Dec 26 18:57:07 PST 1980' AND 'Thu Dec 09 18:57:07 PST 1999' AND tpce.security.s_symb=tpce.daily_market.dm_s_symb AND tpce.security.s_symb=tpce.watch_item.wi_s_symb AND tpce.exchange.ex_id=tpce.security.s_ex_id ; 
SELECT 30, COUNT(*)  FROM tpch.orders, tpch.lineitem, tpch.part, tpch.partsupp WHERE  tpch.part.p_retailprice BETWEEN 1304.6957402590888 AND 1307.7796820431902 AND tpch.part.p_size BETWEEN 18 AND 23 AND tpch.orders.o_orderkey=tpch.lineitem.l_orderkey AND tpch.partsupp.ps_partkey=tpch.lineitem.l_partkey AND tpch.partsupp.ps_suppkey=tpch.lineitem.l_suppkey AND tpch.part.p_partkey=tpch.partsupp.ps_partkey ; 
SELECT 31, COUNT(*)  FROM tpch.customer WHERE  tpch.customer.c_acctbal BETWEEN 312.82135226880155 AND 1442.9677544071765 ; 
SELECT 32, COUNT(*)  FROM tpcc.orderline WHERE  tpcc.orderline.ol_amount BETWEEN 909.4417145511343 AND 918.782543953104 ; 
SELECT 33, COUNT(*)  FROM tpce.daily_market WHERE  tpce.daily_market.dm_vol BETWEEN 9051.444869720295 AND 9089.840411672978 AND tpce.daily_market.dm_high BETWEEN 22.457640615082145 AND 24.218137042149543 ; 
SELECT 34, COUNT(*)  FROM tpce.daily_market WHERE  tpce.daily_market.dm_close BETWEEN 23.543806462802596 AND 23.583058998584352 AND tpce.daily_market.dm_high BETWEEN 23.909521867116737 AND 25.107841059482908 ; 
SELECT 35, COUNT(*)  FROM tpch.lineitem WHERE  tpch.lineitem.l_shipdate BETWEEN 'Wed Aug 19 02:14:59 PDT 1992' AND 'Tue Jul 27 02:14:59 PDT 1993' ; 
SELECT 36, COUNT(*)  FROM nref.protein, nref.neighboring_seq WHERE  nref.neighboring_seq.score BETWEEN 99.35275826218552 AND 100.05596785960496 AND nref.protein.nref_id=nref.neighboring_seq.nref_id_1 ; 
SELECT 37, COUNT(*)  FROM nref.protein, nref.neighboring_seq WHERE  nref.protein.seq_length BETWEEN 892 AND 5046 AND nref.protein.last_updated BETWEEN 'Wed Jan 30 23:24:25 PST 2002' AND 'Thu Mar 21 23:24:25 PST 2002' AND nref.protein.nref_id=nref.neighboring_seq.nref_id_1 ; 
SELECT 38, COUNT(*)  FROM nref.neighboring_seq WHERE  nref.neighboring_seq.overlap_length BETWEEN 911 AND 2968 AND nref.neighboring_seq.start_2 BETWEEN 2907 AND 5653 AND nref.neighboring_seq.end_2 BETWEEN 2918 AND 5011 ; 
SELECT 39, COUNT(*)  FROM nref.protein, nref.neighboring_seq WHERE  nref.protein.last_updated BETWEEN 'Tue Feb 12 11:43:11 PST 2002' AND 'Mon Apr 15 12:43:11 PDT 2002' AND nref.protein.nref_id=nref.neighboring_seq.nref_id_1 ; 
SELECT 40, COUNT(*)  FROM tpce.security, tpce.daily_market, tpce.exchange WHERE  tpce.exchange.ex_num_symb BETWEEN 844.7282123471803 AND 849.513572019854 AND tpce.security.s_pe BETWEEN 86.01111310315076 AND 103.45698973114257 AND tpce.security.s_yield BETWEEN 51.72102410482892 AND 71.8408203267383 AND tpce.security.s_52wk_low BETWEEN 25.010177011145874 AND 26.25872080635501 AND tpce.security.s_symb=tpce.daily_market.dm_s_symb AND tpce.exchange.ex_id=tpce.security.s_ex_id ; 
SELECT 41, COUNT(*)  FROM tpcc.orderline WHERE  tpcc.orderline.ol_amount BETWEEN 3616.440417526016 AND 3623.67611253437 ; 
SELECT 42, COUNT(*)  FROM tpce.daily_market WHERE  tpce.daily_market.dm_close BETWEEN 21.40186058129545 AND 21.46213172467651 AND tpce.daily_market.dm_vol BETWEEN 1866.1065629842155 AND 1876.67418269385 ; 
SELECT 43, COUNT(*)  FROM tpce.daily_market WHERE  tpce.daily_market.dm_close BETWEEN 24.557351439996843 AND 26.488431078404986 AND tpce.daily_market.dm_high BETWEEN 24.341317228312587 AND 25.83857773907507 ; 
SELECT 44, COUNT(*)  FROM tpce.security, tpce.daily_market WHERE  tpce.security.s_52wk_high_date BETWEEN 'Wed Feb 04 12:55:09 PST 2004' AND 'Tue Apr 06 13:55:09 PDT 2004' AND tpce.security.s_symb=tpce.daily_market.dm_s_symb ; 
SELECT 45, COUNT(*)  FROM tpce.security, tpce.daily_market WHERE  tpce.security.s_pe BETWEEN 113.03846515133833 AND 113.67766055112763 AND tpce.security.s_symb=tpce.daily_market.dm_s_symb ; 
SELECT 46, COUNT(*)  FROM tpcc.item WHERE  tpcc.item.i_price BETWEEN 2.6384614906832375 AND 21.356522347104423 AND tpcc.item.i_im_id BETWEEN 7744 AND 9563 ; 
SELECT 47, COUNT(*)  FROM nref.protein, nref.neighboring_seq, nref.source, nref.organism WHERE  nref.protein.last_updated BETWEEN 'Mon Oct 21 17:33:39 PDT 2002' AND 'Mon Jan 13 16:33:39 PST 2003' AND nref.protein.nref_id=nref.neighboring_seq.nref_id_1 AND nref.protein.nref_id=nref.organism.nref_id AND nref.protein.nref_id=nref.source.nref_id ; 
SELECT 48, COUNT(*)  FROM tpce.daily_market WHERE  tpce.daily_market.dm_high BETWEEN 23.62776215703594 AND 25.647324225147678 AND tpce.daily_market.dm_vol BETWEEN 3062.09290507057 AND 4823.529333580862 ; 
--SELECT 49, COUNT(*)  FROM tpcc.orderline, tpcc.stock, tpcc.orders, tpcc.item WHERE  tpcc.item.i_price BETWEEN 36.476781120022196 AND 53.829950993817654 AND tpcc.item.i_im_id BETWEEN 5254 AND 6308 AND tpcc.orders.o_ol_cnt BETWEEN 6.102884627517746 AND 7.647775377322866 AND tpcc.orders.o_entry_d BETWEEN 'Tue Mar 18 00:00:00 PDT 2008' AND 'Tue Mar 18 00:00:00 PDT 2008' AND tpcc.orders.o_carrier_id BETWEEN 4 AND 4 AND tpcc.stock.s_w_id=tpcc.orderline.ol_supply_w_id AND tpcc.stock.s_i_id=tpcc.orderline.ol_i_id AND tpcc.item.i_id=tpcc.stock.s_i_id AND tpcc.orders.o_w_id=tpcc.orderline.ol_w_id AND tpcc.orders.o_d_id=tpcc.orderline.ol_d_id AND tpcc.orders.o_id=tpcc.orderline.ol_o_id ; 
SELECT 50, COUNT(*)  FROM tpcc.orderline WHERE  tpcc.orderline.ol_amount BETWEEN 558.9206426319727 AND 601.6685045211347 ; 
SELECT 51, COUNT(*)  FROM nref.neighboring_seq WHERE  nref.neighboring_seq.length_2 BETWEEN 16100 AND 21937 AND nref.neighboring_seq.end_2 BETWEEN 12525 AND 12677 ; 
SELECT 52, COUNT(*)  FROM nref.neighboring_seq WHERE  nref.neighboring_seq.ordinal BETWEEN 4873 AND 6347 ; 
SELECT 53, COUNT(*)  FROM tpce.security, tpce.daily_market, tpce.company WHERE  tpce.security.s_exch_date BETWEEN 'Mon Mar 06 11:47:25 PST 1939' AND 'Wed Aug 16 11:47:25 PST 1939' AND tpce.daily_market.dm_high BETWEEN 22.64309334454888 AND 24.62625023922281 AND tpce.daily_market.dm_vol BETWEEN 3287.4420639725763 AND 4402.090571973646 AND tpce.security.s_symb=tpce.daily_market.dm_s_symb AND tpce.company.co_id=tpce.security.s_co_id ; 
SELECT 54, COUNT(*)  FROM tpcc.orderline WHERE  tpcc.orderline.ol_amount BETWEEN 6901.548301378219 AND 6953.132053489596 ; 
SELECT 55, COUNT(*)  FROM tpch.orders, tpch.lineitem, tpch.partsupp WHERE  tpch.orders.o_orderdate BETWEEN 'Tue Nov 22 14:03:25 PST 1994' AND 'Sun Jul 30 15:03:25 PDT 1995' AND tpch.orders.o_totalprice BETWEEN 164784.46295508684 AND 256069.87878254591 AND tpch.orders.o_orderkey=tpch.lineitem.l_orderkey AND tpch.partsupp.ps_partkey=tpch.lineitem.l_partkey AND tpch.partsupp.ps_suppkey=tpch.lineitem.l_suppkey ; 
SELECT 56, COUNT(*)  FROM tpch.orders, tpch.lineitem WHERE  tpch.lineitem.l_commitdate BETWEEN 'Sat Oct 24 02:16:56 PDT 1992' AND 'Mon Jan 24 01:16:56 PST 1994' AND tpch.lineitem.l_quantity BETWEEN 6.067721881448492 AND 12.98122019260068 AND tpch.orders.o_totalprice BETWEEN 150188.80381220076 AND 217951.59444053945 AND tpch.orders.o_orderkey=tpch.lineitem.l_orderkey ; 
SELECT 57, COUNT(*)  FROM tpch.partsupp, tpch.lineitem, tpch.part, tpch.orders WHERE  tpch.part.p_size BETWEEN 29 AND 29 AND tpch.part.p_retailprice BETWEEN 1269.472447746158 AND 1440.8117400932254 AND tpch.orders.o_orderdate BETWEEN 'Tue Jun 16 15:58:45 PDT 1998' AND 'Sun Apr 04 15:58:45 PDT 1999' AND tpch.orders.o_totalprice BETWEEN 39533.306503719294 AND 44662.347871495906 AND tpch.partsupp.ps_partkey=tpch.lineitem.l_partkey AND tpch.partsupp.ps_suppkey=tpch.lineitem.l_suppkey AND tpch.orders.o_orderkey=tpch.lineitem.l_orderkey AND tpch.part.p_partkey=tpch.partsupp.ps_partkey ; 
--SELECT 58, COUNT(*)  FROM tpcc.orderline, tpcc.orders, tpcc.customer, tpcc.stock WHERE  tpcc.orders.o_ol_cnt BETWEEN 13.048476164696082 AND 14.725201038180673 AND tpcc.customer.c_since BETWEEN 'Tue Mar 18 00:00:00 PDT 2008' AND 'Tue Mar 18 00:00:00 PDT 2008' AND tpcc.customer.c_discount BETWEEN 0.08931840559468429 AND 0.09322709717893539 AND tpcc.orders.o_w_id=tpcc.orderline.ol_w_id AND tpcc.orders.o_d_id=tpcc.orderline.ol_d_id AND tpcc.orders.o_id=tpcc.orderline.ol_o_id AND tpcc.stock.s_w_id=tpcc.orderline.ol_supply_w_id AND tpcc.stock.s_i_id=tpcc.orderline.ol_i_id AND tpcc.customer.c_w_id=tpcc.orders.o_w_id AND tpcc.customer.c_d_id=tpcc.orders.o_d_id AND tpcc.customer.c_id=tpcc.orders.o_c_id ; 
SELECT 59, COUNT(*)  FROM nref.protein, nref.neighboring_seq, nref.identical_seq WHERE  nref.neighboring_seq.overlap_length BETWEEN 170 AND 3483 AND nref.neighboring_seq.ordinal BETWEEN 1197 AND 2429 AND nref.neighboring_seq.score BETWEEN 96.01513484762351 AND 96.90632878475577 AND nref.protein.nref_id=nref.neighboring_seq.nref_id_1 AND nref.protein.nref_id=nref.identical_seq.nref_id_1 ; 
SELECT 60, COUNT(*)  FROM tpce.daily_market WHERE  tpce.daily_market.dm_vol BETWEEN 6990.678112290023 AND 7003.7749223794945 AND tpce.daily_market.dm_close BETWEEN 25.163024585559327 AND 26.29446202326217 AND tpce.daily_market.dm_high BETWEEN 25.936065535458642 AND 27.308576567011116 ; 
SELECT 61, COUNT(*)  FROM nref.neighboring_seq WHERE  nref.neighboring_seq.score BETWEEN 95.10069622660961 AND 96.01517838699803 AND nref.neighboring_seq.ordinal BETWEEN 512 AND 2447 ; 
SELECT 62, COUNT(*)  FROM tpch.orders, tpch.lineitem, tpch.part, tpch.partsupp WHERE  tpch.part.p_retailprice BETWEEN 1876.2270944246504 AND 1883.84086242501 AND tpch.lineitem.l_receiptdate BETWEEN 'Sun Sep 21 14:23:49 PDT 1997' AND 'Wed Jan 06 13:23:49 PST 1999' AND tpch.lineitem.l_shipdate BETWEEN 'Tue Oct 14 12:39:17 PDT 1997' AND 'Fri Feb 05 11:39:17 PST 1999' AND tpch.orders.o_orderkey=tpch.lineitem.l_orderkey AND tpch.partsupp.ps_partkey=tpch.lineitem.l_partkey AND tpch.partsupp.ps_suppkey=tpch.lineitem.l_suppkey AND tpch.part.p_partkey=tpch.partsupp.ps_partkey ; 
SELECT 63, COUNT(*)  FROM nref.neighboring_seq WHERE  nref.neighboring_seq.start_2 BETWEEN 245 AND 2126 AND nref.neighboring_seq.overlap_length BETWEEN 695 AND 3499 AND nref.neighboring_seq.length_2 BETWEEN 34349 AND 40217 ; 
SELECT 64, COUNT(*)  FROM nref.neighboring_seq WHERE  nref.neighboring_seq.score BETWEEN 95.10069622660961 AND 96.01517838699803 AND nref.neighboring_seq.ordinal BETWEEN 512 AND 2447 ; 
SELECT 65, COUNT(*)  FROM nref.neighboring_seq WHERE  nref.neighboring_seq.ordinal BETWEEN 1229 AND 1241 AND nref.neighboring_seq.score BETWEEN 97.43155444110327 AND 98.2236892518494 ; 
SELECT 66, COUNT(*)  FROM nref.neighboring_seq WHERE  nref.neighboring_seq.ordinal BETWEEN 5246 AND 6608 AND nref.neighboring_seq.score BETWEEN 95.59223539275973 AND 96.17793464451769 ; 
SELECT 67, COUNT(*)  FROM nref.neighboring_seq WHERE  nref.neighboring_seq.score BETWEEN 97.08978105261033 AND 97.98040341462445 ; 
SELECT 68, COUNT(*)  FROM tpce.security, tpce.daily_market, tpce.company WHERE  tpce.daily_market.dm_low BETWEEN 20.596421399183566 AND 20.640795251944752 AND tpce.security.s_symb=tpce.daily_market.dm_s_symb AND tpce.company.co_id=tpce.security.s_co_id ; 
--SELECT 69, COUNT(*)  FROM nref.protein, nref.neighboring_seq, nref.taxonomy, nref.organism WHERE  nref.neighboring_seq.end_2 BETWEEN 308 AND 334 AND nref.neighboring_seq.score BETWEEN 95.55772706947698 AND 96.26528031821337 AND nref.neighboring_seq.end_1 BETWEEN 328 AND 3035 AND nref.protein.nref_id=nref.neighboring_seq.nref_id_1 AND nref.protein.nref_id=nref.organism.nref_id AND nref.protein.nref_id=nref.taxonomy.nref_id ; 
SELECT 70, COUNT(*)  FROM tpch.orders WHERE  tpch.orders.o_orderdate BETWEEN 'Fri Feb 25 22:04:22 PST 1994' AND 'Fri Jan 20 22:04:22 PST 1995' AND tpch.orders.o_totalprice BETWEEN 37365.68396464411 AND 106698.2608989657 ; 
SELECT 71, COUNT(*)  FROM tpce.daily_market WHERE  tpce.daily_market.dm_high BETWEEN 23.52841970111281 AND 25.337918982913813 AND tpce.daily_market.dm_vol BETWEEN 3680.386954450534 AND 4608.779835253595 ; 
SELECT 72, COUNT(*)  FROM nref.protein, nref.neighboring_seq, nref.source WHERE  nref.protein.last_updated BETWEEN 'Fri Aug 16 14:23:49 PDT 2002' AND 'Sat Oct 05 14:23:49 PDT 2002' AND nref.protein.nref_id=nref.neighboring_seq.nref_id_1 AND nref.protein.nref_id=nref.source.nref_id ; 
SELECT 73, COUNT(*)  FROM tpch.customer, tpch.orders, tpch.nation WHERE  tpch.orders.o_orderdate BETWEEN 'Sat Oct 07 21:58:04 PDT 1995' AND 'Sat Dec 28 20:58:04 PST 1996' AND tpch.orders.o_totalprice BETWEEN 366689.6662007361 AND 468947.0794495564 AND tpch.customer.c_custkey=tpch.orders.o_custkey AND tpch.nation.n_nationkey=tpch.customer.c_nationkey ; 
SELECT 74, COUNT(*)  FROM tpch.lineitem WHERE  tpch.lineitem.l_shipdate BETWEEN 'Wed Jan 04 18:06:57 PST 1995' AND 'Mon Mar 04 18:06:57 PST 1996' AND tpch.lineitem.l_tax BETWEEN 0.026251812127526176 AND 0.041359376193486855 ; 
SELECT 75, COUNT(*)  FROM tpch.partsupp, tpch.lineitem, tpch.orders, tpch.part WHERE  tpch.part.p_size BETWEEN 26 AND 33 AND tpch.orders.o_totalprice BETWEEN 416287.9048999934 AND 420400.2249158094 AND tpch.partsupp.ps_partkey=tpch.lineitem.l_partkey AND tpch.partsupp.ps_suppkey=tpch.lineitem.l_suppkey AND tpch.part.p_partkey=tpch.partsupp.ps_partkey AND tpch.orders.o_orderkey=tpch.lineitem.l_orderkey ; 
SELECT 76, COUNT(*)  FROM tpch.orders, tpch.lineitem, tpch.partsupp WHERE  tpch.lineitem.l_extendedprice BETWEEN 22898.55557537188 AND 36682.317071133846 AND tpch.orders.o_orderkey=tpch.lineitem.l_orderkey AND tpch.partsupp.ps_partkey=tpch.lineitem.l_partkey AND tpch.partsupp.ps_suppkey=tpch.lineitem.l_suppkey ; 
SELECT 77, COUNT(*)  FROM tpcc.orderline WHERE  tpcc.orderline.ol_delivery_d BETWEEN 'Tue Mar 18 00:00:00 PDT 2008' AND 'Tue Mar 18 00:00:00 PDT 2008' ; 
SELECT 78, COUNT(*)  FROM tpce.security, tpce.daily_market, tpce.exchange WHERE  tpce.security.s_52wk_low_date BETWEEN 'Fri Mar 12 15:29:44 PST 2004' AND 'Tue May 11 16:29:44 PDT 2004' AND tpce.security.s_pe BETWEEN 66.67448562154863 AND 79.23757301482112 AND tpce.daily_market.dm_vol BETWEEN 6656.286838270026 AND 6698.836857860924 AND tpce.daily_market.dm_close BETWEEN 29.142062318787328 AND 31.058026394849872 AND tpce.security.s_symb=tpce.daily_market.dm_s_symb AND tpce.exchange.ex_id=tpce.security.s_ex_id ; 
SELECT 79, COUNT(*)  FROM nref.neighboring_seq WHERE  nref.neighboring_seq.overlap_length BETWEEN 71 AND 2513 ; 
SELECT 80, COUNT(*)  FROM nref.protein, nref.neighboring_seq, nref.source WHERE  nref.protein.last_updated BETWEEN 'Fri Aug 16 14:23:49 PDT 2002' AND 'Sat Oct 05 14:23:49 PDT 2002' AND nref.protein.nref_id=nref.neighboring_seq.nref_id_1 AND nref.protein.nref_id=nref.source.nref_id ; 
SELECT 81, COUNT(*)  FROM tpce.daily_market WHERE  tpce.daily_market.dm_low BETWEEN 24.416558689859684 AND 26.200969179712526 ; 
SELECT 82, COUNT(*)  FROM tpce.security, tpce.daily_market, tpce.company WHERE  tpce.company.co_open_date BETWEEN 'Sat Jan 08 21:34:58 PST 1825' AND 'Thu Mar 09 21:34:58 PST 1826' AND tpce.daily_market.dm_vol BETWEEN 4156.308111529779 AND 5103.899750479823 AND tpce.daily_market.dm_low BETWEEN 22.494344330560434 AND 23.932521097195693 AND tpce.security.s_symb=tpce.daily_market.dm_s_symb AND tpce.company.co_id=tpce.security.s_co_id ; 
SELECT 83, COUNT(*)  FROM tpch.orders, tpch.lineitem, tpch.partsupp WHERE  tpch.partsupp.ps_availqty BETWEEN 9609 AND 9676 AND tpch.partsupp.ps_supplycost BETWEEN 656.2181096779609 AND 853.1264542767201 AND tpch.lineitem.l_commitdate BETWEEN 'Mon Jul 13 22:29:23 PDT 1998' AND 'Tue Apr 27 22:29:23 PDT 1999' AND tpch.lineitem.l_extendedprice BETWEEN 82133.3178949841 AND 93560.50903247872 AND tpch.lineitem.l_discount BETWEEN 0.03206797027658611 AND 0.044944241397877754 AND tpch.orders.o_orderkey=tpch.lineitem.l_orderkey AND tpch.partsupp.ps_partkey=tpch.lineitem.l_partkey AND tpch.partsupp.ps_suppkey=tpch.lineitem.l_suppkey ; 
SELECT 84, COUNT(*)  FROM tpcc.stock, tpcc.orderline WHERE  tpcc.orderline.ol_amount BETWEEN 558.9206426319727 AND 601.6685045211347 AND tpcc.stock.s_w_id=tpcc.orderline.ol_supply_w_id AND tpcc.stock.s_i_id=tpcc.orderline.ol_i_id ; 
SELECT 85, COUNT(*)  FROM tpch.lineitem WHERE  tpch.lineitem.l_quantity BETWEEN 37.05799160857271 AND 42.9896659990098 ; 
SELECT 86, COUNT(*)  FROM tpcc.orderline WHERE  tpcc.orderline.ol_delivery_d BETWEEN 'Tue Mar 18 00:00:00 PDT 2008' AND 'Tue Mar 18 00:00:00 PDT 2008' ; 
SELECT 87, COUNT(*)  FROM tpce.security, tpce.daily_market WHERE  tpce.security.s_dividend BETWEEN 18.032894427191316 AND 22.22236816705047 AND tpce.security.s_yield BETWEEN 75.49810139940016 AND 75.79266844869355 AND tpce.security.s_symb=tpce.daily_market.dm_s_symb ; 
SELECT 88, COUNT(*)  FROM tpce.security, tpce.daily_market, tpce.company WHERE  tpce.company.co_open_date BETWEEN 'Thu Feb 26 13:15:38 PST 1976' AND 'Thu Feb 10 13:15:38 PST 2005' AND tpce.daily_market.dm_high BETWEEN 23.442991754004705 AND 25.261224431604276 AND tpce.daily_market.dm_low BETWEEN 19.91267047192923 AND 21.00716510521396 AND tpce.security.s_symb=tpce.daily_market.dm_s_symb AND tpce.company.co_id=tpce.security.s_co_id ; 
SELECT 89, COUNT(*)  FROM tpce.security, tpce.daily_market, tpce.exchange, tpce.last_trade WHERE  tpce.security.s_start_date BETWEEN 'Tue Oct 02 14:13:32 PDT 1962' AND 'Tue Dec 29 13:13:32 PST 1981' AND tpce.security.s_exch_date BETWEEN 'Mon May 06 04:29:19 PDT 1974' AND 'Wed Feb 26 03:29:19 PST 1992' AND tpce.security.s_52wk_low_date BETWEEN 'Tue May 25 06:32:21 PDT 2004' AND 'Tue Jul 27 06:32:21 PDT 2004' AND tpce.security.s_symb=tpce.daily_market.dm_s_symb AND tpce.security.s_symb=tpce.last_trade.lt_s_symb AND tpce.exchange.ex_id=tpce.security.s_ex_id ; 
SELECT 90, COUNT(*)  FROM tpcc.orderline WHERE  tpcc.orderline.ol_amount BETWEEN 5170.729489133814 AND 5172.384166272982 ; 
SELECT 91, COUNT(*)  FROM nref.neighboring_seq WHERE  nref.neighboring_seq.start_2 BETWEEN 948 AND 985 ; 
SELECT 92, COUNT(*)  FROM tpce.security, tpce.daily_market, tpce.last_trade WHERE  tpce.daily_market.dm_low BETWEEN 26.488077926810703 AND 27.555732817331922 AND tpce.daily_market.dm_vol BETWEEN 8563.978420944384 AND 9802.326234767455 AND tpce.security.s_symb=tpce.daily_market.dm_s_symb AND tpce.security.s_symb=tpce.last_trade.lt_s_symb ; 
SELECT 93, COUNT(*)  FROM tpce.daily_market WHERE  tpce.daily_market.dm_close BETWEEN 25.27328048051263 AND 26.998483507176346 ; 
SELECT 94, COUNT(*)  FROM tpcc.stock, tpcc.orderline, tpcc.warehouse WHERE  tpcc.stock.s_quantity BETWEEN 83.8370520895543 AND 96.04813892847069 AND tpcc.warehouse.w_tax BETWEEN 0.035217006514365684 AND 0.060837752839317916 AND tpcc.stock.s_w_id=tpcc.orderline.ol_supply_w_id AND tpcc.stock.s_i_id=tpcc.orderline.ol_i_id AND tpcc.warehouse.w_id=tpcc.stock.s_w_id ; 
SELECT 95, COUNT(*)  FROM tpce.daily_market WHERE  tpce.daily_market.dm_close BETWEEN 20.623162938659735 AND 22.165638002317174 AND tpce.daily_market.dm_vol BETWEEN 3497.6431512774107 AND 4617.9446315177465 ; 
SELECT 96, COUNT(*)  FROM nref.neighboring_seq WHERE  nref.neighboring_seq.score BETWEEN 95.10069622660961 AND 96.01517838699803 AND nref.neighboring_seq.ordinal BETWEEN 512 AND 2447 ; 
SELECT 97, COUNT(*)  FROM tpce.daily_market WHERE  tpce.daily_market.dm_vol BETWEEN 5546.9906593736805 AND 7228.398142235783 AND tpce.daily_market.dm_low BETWEEN 25.824028671121333 AND 27.513148527739315 ; 
SELECT 98, COUNT(*)  FROM tpch.lineitem, tpch.orders WHERE  tpch.lineitem.l_receiptdate BETWEEN 'Fri Aug 19 06:31:14 PDT 1994' AND 'Fri Nov 10 05:31:14 PST 1995' AND tpch.lineitem.l_tax BETWEEN 0.009224182225769458 AND 0.02282724558028025 AND tpch.orders.o_orderkey=tpch.lineitem.l_orderkey ; 
SELECT 99, COUNT(*)  FROM tpcc.stock, tpcc.orderline, tpcc.item WHERE  tpcc.orderline.ol_amount BETWEEN 4246.375994483605 AND 5804.080500618356 AND tpcc.stock.s_w_id=tpcc.orderline.ol_supply_w_id AND tpcc.stock.s_i_id=tpcc.orderline.ol_i_id AND tpcc.item.i_id=tpcc.stock.s_i_id ; 
SELECT 100, COUNT(*)  FROM tpce.daily_market WHERE  tpce.daily_market.dm_high BETWEEN 25.732893171363976 AND 25.750771571996548 AND tpce.daily_market.dm_vol BETWEEN 7013.000437776472 AND 7025.356556202202 ; 
SELECT 101, COUNT(*)  FROM tpch.orders, tpch.lineitem, tpch.supplier, tpch.partsupp WHERE  tpch.partsupp.ps_availqty BETWEEN 2295 AND 2309 AND tpch.partsupp.ps_supplycost BETWEEN 850.5471573411701 AND 1029.0154826222245 AND tpch.orders.o_orderdate BETWEEN 'Fri Jun 12 04:04:51 PDT 1992' AND 'Sat Jun 19 04:04:51 PDT 1993' AND tpch.orders.o_totalprice BETWEEN 145734.98234268453 AND 233202.77418591047 AND tpch.orders.o_orderkey=tpch.lineitem.l_orderkey AND tpch.partsupp.ps_partkey=tpch.lineitem.l_partkey AND tpch.partsupp.ps_suppkey=tpch.lineitem.l_suppkey AND tpch.supplier.s_suppkey=tpch.partsupp.ps_suppkey ; 
SELECT 102, COUNT(*)  FROM tpce.security, tpce.daily_market, tpce.status_type WHERE  tpce.security.s_start_date BETWEEN 'Wed Oct 11 12:47:34 PST 1916' AND 'Sun Jul 29 12:47:34 PST 1934' AND tpce.security.s_exch_date BETWEEN 'Fri Aug 12 16:03:41 PDT 1988' AND 'Fri Jan 16 15:03:41 PST 2004' AND tpce.security.s_52wk_low_date BETWEEN 'Tue Apr 06 06:09:34 PDT 2004' AND 'Wed Jun 02 06:09:34 PDT 2004' AND tpce.daily_market.dm_close BETWEEN 28.329909869607892 AND 28.42669467037347 AND tpce.daily_market.dm_high BETWEEN 28.592793529612266 AND 30.442782809147648 AND tpce.daily_market.dm_vol BETWEEN 9795.153166263999 AND 9863.443168852615 AND tpce.security.s_symb=tpce.daily_market.dm_s_symb AND tpce.status_type.st_id=tpce.security.s_st_id ; 
SELECT 103, COUNT(*)  FROM tpch.orders, tpch.lineitem WHERE  tpch.lineitem.l_commitdate BETWEEN 'Fri Oct 09 23:31:28 PDT 1992' AND 'Mon Oct 26 22:31:28 PST 1992' AND tpch.lineitem.l_discount BETWEEN 0.015671817495096243 AND 0.030657312880907815 AND tpch.orders.o_orderkey=tpch.lineitem.l_orderkey ; 
SELECT 104, COUNT(*)  FROM nref.protein, nref.taxonomy WHERE  nref.protein.seq_length BETWEEN 487 AND 4104 AND nref.protein.last_updated BETWEEN 'Wed Nov 21 22:44:25 PST 2001' AND 'Wed Feb 20 22:44:25 PST 2002' AND nref.protein.nref_id=nref.taxonomy.nref_id ; 
SELECT 105, COUNT(*)  FROM nref.neighboring_seq WHERE  nref.neighboring_seq.ordinal BETWEEN 3289 AND 4726 AND nref.neighboring_seq.overlap_length BETWEEN 131 AND 192 AND nref.neighboring_seq.score BETWEEN 98.67553315876872 AND 99.57521861070478 ; 
SELECT 106, COUNT(*)  FROM tpcc.orderline WHERE  tpcc.orderline.ol_delivery_d BETWEEN 'Tue Mar 18 00:00:00 PDT 2008' AND 'Tue Mar 18 00:00:00 PDT 2008' ; 

