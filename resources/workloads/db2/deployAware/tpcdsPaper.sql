select i_item_id, avg(ss_quantity) agg1, avg(ss_list_price) agg2, avg(ss_coupon_amt) agg3, avg(ss_sales_price) agg4 from tpcds.store_sales, tpcds.customer_demographics, tpcds.date_dim, tpcds.item, tpcds.promotion where ss_sold_date_sk = d_date_sk and ss_item_sk = i_item_sk and ss_cdemo_sk = cd_demo_sk and ss_promo_sk = p_promo_sk and cd_gender = 'F' and cd_marital_status = 'M' and cd_education_status = 'College' and (p_channel_email = 'N' or p_channel_event = 'N') and d_year = 2001 group by i_item_id order by i_item_id fetch first 100 rows only;
with all_sales AS ( SELECT d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id, SUM(sales_cnt) AS sales_cnt, SUM(sales_amt) AS sales_amt FROM (SELECT d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id, cs_quantity - COALESCE(cr_return_quantity,0) AS sales_cnt, cs_ext_sales_price - COALESCE(cr_return_amount,0.0) AS sales_amt FROM tpcds.catalog_sales JOIN tpcds.item ON i_item_sk=cs_item_sk JOIN tpcds.date_dim ON d_date_sk=cs_sold_date_sk LEFT JOIN tpcds.catalog_returns ON (cs_order_number=cr_order_number AND cs_item_sk=cr_item_sk) WHERE i_category='Music' UNION SELECT d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id, ss_quantity - COALESCE(sr_return_quantity,0) AS sales_cnt, ss_ext_sales_price - COALESCE(sr_return_amt,0.0) AS sales_amt FROM tpcds.store_sales JOIN tpcds.item ON i_item_sk=ss_item_sk JOIN tpcds.date_dim ON d_date_sk=ss_sold_date_sk LEFT JOIN tpcds.store_returns ON (ss_ticket_number=sr_ticket_number AND ss_item_sk=sr_item_sk) WHERE i_category='Music' UNION SELECT d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id, ws_quantity - COALESCE(wr_return_quantity,0) AS sales_cnt, ws_ext_sales_price - COALESCE(wr_return_amt,0.0) AS sales_amt FROM tpcds.web_sales JOIN tpcds.item ON i_item_sk=ws_item_sk JOIN tpcds.date_dim ON d_date_sk=ws_sold_date_sk LEFT JOIN tpcds.web_returns ON (ws_order_number=wr_order_number AND ws_item_sk=wr_item_sk) WHERE i_category='Music') sales_detail GROUP BY d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id) SELECT prev_yr.d_year AS prev_year, curr_yr.d_year AS year, curr_yr.i_brand_id, curr_yr.i_class_id, curr_yr.i_category_id, curr_yr.i_manufact_id, prev_yr.sales_cnt AS prev_yr_cnt, curr_yr.sales_cnt AS curr_yr_cnt, curr_yr.sales_cnt-prev_yr.sales_cnt AS sales_cnt_diff, curr_yr.sales_amt-prev_yr.sales_amt AS sales_amt_diff FROM all_sales curr_yr, all_sales prev_yr WHERE curr_yr.i_brand_id=prev_yr.i_brand_id AND curr_yr.i_class_id=prev_yr.i_class_id AND curr_yr.i_category_id=prev_yr.i_category_id AND curr_yr.i_manufact_id=prev_yr.i_manufact_id AND curr_yr.d_year=2000 AND prev_yr.d_year=2000-1 AND CAST(curr_yr.sales_cnt AS DECIMAL(17,2))/CAST(prev_yr.sales_cnt AS DECIMAL(17,2))<0.9 ORDER BY sales_cnt_diff fetch first 100 rows only;
select asceding.rnk, i1.i_product_name best_performing, i2.i_product_name worst_performing from(select * from (select item_sk,rank() over (order by rank_col asc) rnk from (select ss_item_sk item_sk,avg(ss_net_profit) rank_col from tpcds.store_sales ss1 where ss_store_sk = 42 group by ss_item_sk having avg(ss_net_profit) > 0.9*(select avg(ss_net_profit) rank_col from tpcds.store_sales where ss_store_sk = 42 and ss_promo_sk is null group by ss_store_sk))V1)V11 where rnk < 11) asceding, (select * from (select item_sk,rank() over (order by rank_col desc) rnk from (select ss_item_sk item_sk,avg(ss_net_profit) rank_col from tpcds.store_sales ss1 where ss_store_sk = 42 group by ss_item_sk having avg(ss_net_profit) > 0.9*(select avg(ss_net_profit) rank_col from tpcds.store_sales where ss_store_sk = 42 and ss_promo_sk is null group by ss_store_sk))V2)V21 where rnk < 11) descending, tpcds.item i1, tpcds.item i2 where asceding.rnk = descending.rnk and i1.i_item_sk=asceding.item_sk and i2.i_item_sk=descending.item_sk order by asceding.rnk fetch first 100 rows only;
with ssr as (select s_store_id as store_id, sum(ss_ext_sales_price) as sales, sum(coalesce(sr_return_amt, 0)) as returns, sum(ss_net_profit - coalesce(sr_net_loss, 0)) as profit from tpcds.store_sales left outer join tpcds.store_returns on (ss_item_sk = sr_item_sk and ss_ticket_number = sr_ticket_number), tpcds.date_dim, tpcds.store, tpcds.item, tpcds.promotion where ss_sold_date_sk = d_date_sk and d_date between cast('2000-08-19' as date) and (cast('2000-08-19' as date) + 30 days) and ss_store_sk = s_store_sk and ss_item_sk = i_item_sk and i_current_price > 50 and ss_promo_sk = p_promo_sk and p_channel_tv = 'N' group by s_store_id), csr as (select cp_catalog_page_id as catalog_page_id, sum(cs_ext_sales_price) as sales, sum(coalesce(cr_return_amount, 0)) as returns, sum(cs_net_profit - coalesce(cr_net_loss, 0)) as profit from tpcds.catalog_sales left outer join tpcds.catalog_returns on (cs_item_sk = cr_item_sk and cs_order_number = cr_order_number), tpcds.date_dim, tpcds.catalog_page, tpcds.item, tpcds.promotion where cs_sold_date_sk = d_date_sk and d_date between cast('2000-08-19' as date) and (cast('2000-08-19' as date) + 30 days) and cs_catalog_page_sk = cp_catalog_page_sk and cs_item_sk = i_item_sk and i_current_price > 50 and cs_promo_sk = p_promo_sk and p_channel_tv = 'N' group by cp_catalog_page_id), wsr as (select web_site_id, sum(ws_ext_sales_price) as sales, sum(coalesce(wr_return_amt, 0)) as returns, sum(ws_net_profit - coalesce(wr_net_loss, 0)) as profit from tpcds.web_sales left outer join tpcds.web_returns on (ws_item_sk = wr_item_sk and ws_order_number = wr_order_number), tpcds.date_dim, tpcds.web_site, tpcds.item, tpcds.promotion where ws_sold_date_sk = d_date_sk and d_date between cast('2000-08-19' as date) and (cast('2000-08-19' as date) + 30 days) and ws_web_site_sk = web_site_sk and ws_item_sk = i_item_sk and i_current_price > 50 and ws_promo_sk = p_promo_sk and p_channel_tv = 'N' group by web_site_id) select channel, id, sum(sales) as sales, sum(returns) as returns, sum(profit) as profit from (select 'store channel' as channel, 'store' || store_id as id, sales, returns, profit from ssr union all select 'catalog channel' as channel, 'catalog_page' || catalog_page_id as id, sales, returns, profit from csr union all select 'web channel' as channel, 'web_site' || web_site_id as id, sales, returns, profit from wsr ) x group by rollup (channel, id) order by channel, id fetch first 100 rows only;
select sum(cs_ext_discount_amt) as "excess discount amount" from tpcds.catalog_sales, tpcds.item, tpcds.date_dim where i_manufact_id = 645 and i_item_sk = cs_item_sk and d_date between '2001-03-05' and (cast('2001-03-05' as date) + 90 days) and d_date_sk = cs_sold_date_sk and cs_ext_discount_amt > ( select 1.3 * avg(cs_ext_discount_amt) from tpcds.catalog_sales, tpcds.date_dim where cs_item_sk = i_item_sk and d_date between '2001-03-05' and (cast('2001-03-05' as date) + 90 days) and d_date_sk = cs_sold_date_sk ) fetch first 100 rows only;
select i_brand_id brand_id, i_brand brand, i_manufact_id, i_manufact, sum(ss_ext_sales_price) ext_price from tpcds.date_dim, tpcds.store_sales, tpcds.item,tpcds.customer,tpcds.customer_address,tpcds.store where d_date_sk = ss_sold_date_sk and ss_item_sk = i_item_sk and i_manager_id=20 and d_moy=11 and d_year=2000 and ss_customer_sk = c_customer_sk and c_current_addr_sk = ca_address_sk and substr(ca_zip,1,5) <> substr(s_zip,1,5) and ss_store_sk = s_store_sk group by i_brand, i_brand_id, i_manufact_id, i_manufact order by ext_price desc, i_brand, i_brand_id, i_manufact_id, i_manufact fetch first 100 rows only ;
with ws as (select d_year AS ws_sold_year, ws_item_sk, ws_bill_customer_sk ws_customer_sk, sum(ws_quantity) ws_qty, sum(ws_wholesale_cost) ws_wc, sum(ws_sales_price) ws_sp from tpcds.web_sales left join tpcds.web_returns on wr_order_number=ws_order_number and ws_item_sk=wr_item_sk join tpcds.date_dim on ws_sold_date_sk = d_date_sk where wr_order_number is null group by d_year, ws_item_sk, ws_bill_customer_sk ), cs as (select d_year AS cs_sold_year, cs_item_sk, cs_bill_customer_sk cs_customer_sk, sum(cs_quantity) cs_qty, sum(cs_wholesale_cost) cs_wc, sum(cs_sales_price) cs_sp from tpcds.catalog_sales left join tpcds.catalog_returns on cr_order_number=cs_order_number and cs_item_sk=cr_item_sk join tpcds.date_dim on cs_sold_date_sk = d_date_sk where cr_order_number is null group by d_year, cs_item_sk, cs_bill_customer_sk ), ss as (select d_year AS ss_sold_year, ss_item_sk, ss_customer_sk, sum(ss_quantity) ss_qty, sum(ss_wholesale_cost) ss_wc, sum(ss_sales_price) ss_sp from tpcds.store_sales left join tpcds.store_returns on sr_ticket_number=ss_ticket_number and ss_item_sk=sr_item_sk join tpcds.date_dim on ss_sold_date_sk = d_date_sk where sr_ticket_number is null group by d_year, ss_item_sk, ss_customer_sk ) select ss_sold_year, ss_item_sk, ss_customer_sk, round(ss_qty/(coalesce(ws_qty+cs_qty,1)),2) ratio, ss_qty store_qty, ss_wc store_wholesale_cost, ss_sp store_sales_price, coalesce(ws_qty,0)+coalesce(cs_qty,0) other_chan_qty, coalesce(ws_wc,0)+coalesce(cs_wc,0) other_chan_wholesale_cost, coalesce(ws_sp,0)+coalesce(cs_sp,0) other_chan_sales_price from ss left join ws on (ws_sold_year=ss_sold_year and ws_item_sk=ss_item_sk and ws_customer_sk=ss_customer_sk) left join cs on (cs_sold_year=ss_sold_year and cs_item_sk=cs_item_sk and cs_customer_sk=ss_customer_sk) where coalesce(ws_qty,0)>0 and coalesce(cs_qty, 0)>0 and ss_sold_year=2001 order by ss_sold_year, ss_item_sk, ss_customer_sk, ss_qty desc, ss_wc desc, ss_sp desc, coalesce(ws_qty,0)+coalesce(cs_qty,0), coalesce(ws_wc,0)+coalesce(cs_wc,0), coalesce(ws_sp,0)+coalesce(cs_sp,0), round(ss_qty/(coalesce(ws_qty+cs_qty,1)),2) fetch first 100 rows only;
select sum(ws_net_paid) as total_sum, i_category, i_class, grouping(i_category)+grouping(i_class) as lochierarchy, rank() over ( partition by grouping(i_category)+grouping(i_class), case when grouping(i_class) = 0 then i_category end order by sum(ws_net_paid) desc) as rank_within_parent from tpcds.web_sales, tpcds.date_dim d1, tpcds.item where d1.d_year = 1999 and d1.d_date_sk = ws_sold_date_sk and i_item_sk = ws_item_sk group by rollup(i_category,i_class) order by lochierarchy desc, case when lochierarchy = 0 then i_category end, rank_within_parent fetch first 100 rows only;
select cc_call_center_id Call_Center, cc_name Call_Center_Name, cc_manager Manager, sum(cr_net_loss) Returns_Loss from tpcds.call_center, tpcds.catalog_returns, tpcds.date_dim, tpcds.customer, tpcds.customer_address, tpcds.customer_demographics, tpcds.household_demographics where cr_call_center_sk = cc_call_center_sk and cr_returned_date_sk = d_date_sk and cr_returning_customer_sk= c_customer_sk and cd_demo_sk = c_current_cdemo_sk and hd_demo_sk = c_current_hdemo_sk and ca_address_sk = c_current_addr_sk and d_year = 2002 and d_moy = 12 and ( (cd_marital_status = 'M' and cd_education_status = 'Unknown') or(cd_marital_status = 'W' and cd_education_status = 'Advanced Degree')) and hd_buy_potential like '5001-10000%' and ca_gmt_offset = -7 group by cc_call_center_id,cc_name,cc_manager,cd_marital_status,cd_education_status order by sum(cr_net_loss) desc;
select * from(select w_warehouse_name, i_item_id, sum(case when (cast(d_date as date) < cast ('2002-05-09' as date)) then inv_quantity_on_hand else 0 end) as inv_before, sum(case when (cast(d_date as date) >= cast ('2002-05-09' as date)) then inv_quantity_on_hand else 0 end) as inv_after from tpcds.inventory, tpcds.warehouse, tpcds.item, tpcds.date_dim where i_current_price between 0.99 and 1.49 and i_item_sk = inv_item_sk and inv_warehouse_sk = w_warehouse_sk and inv_date_sk = d_date_sk and d_date between (cast ('2002-05-09' as date) - 30 days) and (cast ('2002-05-09' as date) + 30 days) group by w_warehouse_name, i_item_id) x where (case when inv_before > 0 then inv_after / inv_before else null end) between 2.0/3.0 and 3.0/2.0 order by w_warehouse_name, i_item_id fetch first 100 rows only;
select s_store_name, s_store_id, sum(case when (d_day_name='Sunday') then ss_sales_price else null end) sun_sales, sum(case when (d_day_name='Monday') then ss_sales_price else null end) mon_sales, sum(case when (d_day_name='Tuesday') then ss_sales_price else null end) tue_sales, sum(case when (d_day_name='Wednesday') then ss_sales_price else null end) wed_sales, sum(case when (d_day_name='Thursday') then ss_sales_price else null end) thu_sales, sum(case when (d_day_name='Friday') then ss_sales_price else null end) fri_sales, sum(case when (d_day_name='Saturday') then ss_sales_price else null end) sat_sales from tpcds.date_dim, tpcds.store_sales, tpcds.store where d_date_sk = ss_sold_date_sk and s_store_sk = ss_store_sk and s_gmt_offset = -6 and d_year = 1999 group by s_store_name, s_store_id order by s_store_name, s_store_id,sun_sales,mon_sales,tue_sales,wed_sales,thu_sales,fri_sales,sat_sales fetch first 100 rows only;
select ca_zip, sum(ws_sales_price) from tpcds.web_sales, tpcds.customer, tpcds.customer_address, tpcds.date_dim, tpcds.item where ws_bill_customer_sk = c_customer_sk and c_current_addr_sk = ca_address_sk and ws_item_sk = i_item_sk and ( substr(ca_zip,1,5) in ('85669', '86197','88274','83405','86475', '85392', '85460', '80348', '81792') or i_item_id in (select i_item_id from tpcds.item where i_item_sk in (2, 3, 5, 7, 11, 13, 17, 19, 23, 29) ) ) and ws_sold_date_sk = d_date_sk and d_qoy = 2 and d_year = 1999 group by ca_zip order by ca_zip fetch first 100 rows only;
select sum(ss_net_profit)/sum(ss_ext_sales_price) as gross_margin, i_category, i_class, grouping(i_category)+grouping(i_class) as lochierarchy, rank() over ( partition by grouping(i_category)+grouping(i_class), case when grouping(i_class) = 0 then i_category end order by sum(ss_net_profit)/sum(ss_ext_sales_price) asc) as rank_within_parent from tpcds.store_sales, tpcds.date_dim d1, tpcds.item, tpcds.store where d1.d_year = 1999 and d1.d_date_sk = ss_sold_date_sk and i_item_sk = ss_item_sk and s_store_sk = ss_store_sk and s_state in ('SD','AL','TN','SD', 'SD','SD','AL','SD') group by rollup(i_category,i_class) order by lochierarchy desc, case when lochierarchy = 0 then i_category end, rank_within_parent fetch first 100 rows only;
with ss as ( select i_manufact_id,sum(ss_ext_sales_price) total_sales from tpcds.store_sales, tpcds.date_dim, tpcds.customer_address, tpcds.item where i_manufact_id in (select i_manufact_id from tpcds.item where i_category in ('Electronics')) and ss_item_sk = i_item_sk and ss_sold_date_sk = d_date_sk and d_year = 2001 and d_moy = 5 and ss_addr_sk = ca_address_sk and ca_gmt_offset = -5 group by i_manufact_id), cs as ( select i_manufact_id,sum(cs_ext_sales_price) total_sales from tpcds.catalog_sales, tpcds.date_dim, tpcds.customer_address, tpcds.item where i_manufact_id in (select i_manufact_id from tpcds.item where i_category in ('Electronics')) and cs_item_sk = i_item_sk and cs_sold_date_sk = d_date_sk and d_year = 2001 and d_moy = 5 and cs_bill_addr_sk = ca_address_sk and ca_gmt_offset = -5 group by i_manufact_id), ws as ( select i_manufact_id,sum(ws_ext_sales_price) total_sales from tpcds.web_sales, tpcds.date_dim, tpcds.customer_address, tpcds.item where i_manufact_id in (select i_manufact_id from tpcds.item where i_category in ('Electronics')) and ws_item_sk = i_item_sk and ws_sold_date_sk = d_date_sk and d_year = 2001 and d_moy = 5 and ws_bill_addr_sk = ca_address_sk and ca_gmt_offset = -5 group by i_manufact_id) select i_manufact_id ,sum(total_sales) total_sales from (select * from ss union all select * from cs union all select * from ws) tmp1 group by i_manufact_id order by total_sales fetch first 100 rows only;
select c_last_name, c_first_name, ca_city, bought_city, ss_ticket_number, amt,profit from (select ss_ticket_number, ss_customer_sk, ca_city bought_city, sum(ss_coupon_amt) amt, sum(ss_net_profit) profit from tpcds.store_sales,tpcds.date_dim,tpcds.store,tpcds.household_demographics,tpcds.customer_address where tpcds.store_sales.ss_sold_date_sk = tpcds.date_dim.d_date_sk and tpcds.store_sales.ss_store_sk = tpcds.store.s_store_sk and tpcds.store_sales.ss_hdemo_sk = tpcds.household_demographics.hd_demo_sk and tpcds.store_sales.ss_addr_sk = tpcds.customer_address.ca_address_sk and (tpcds.household_demographics.hd_dep_count = 7 or tpcds.household_demographics.hd_vehicle_count= 0) and tpcds.date_dim.d_dow in (6,0) and tpcds.date_dim.d_year in (1998,1998+1,1998+2) and tpcds.store.s_city in ('Oak Grove','Riverside','Fairview','Five Points','Midway') group by ss_ticket_number,ss_customer_sk,ss_addr_sk,ca_city) dn,tpcds.customer,tpcds.customer_address current_addr where ss_customer_sk = c_customer_sk and tpcds.customer.c_current_addr_sk = current_addr.ca_address_sk and current_addr.ca_city <> bought_city order by c_last_name, c_first_name, ca_city, bought_city, ss_ticket_number fetch first 100 rows only;
select substr(w_warehouse_name,1,20), sm_type, web_name, sum(case when (ws_ship_date_sk - ws_sold_date_sk <= 30 ) then 1 else 0 end) as "30 days", sum(case when (ws_ship_date_sk - ws_sold_date_sk > 30) and (ws_ship_date_sk - ws_sold_date_sk <= 60) then 1 else 0 end ) as "31-60 days", sum(case when (ws_ship_date_sk - ws_sold_date_sk > 60) and (ws_ship_date_sk - ws_sold_date_sk <= 90) then 1 else 0 end) as "61-90 days", sum(case when (ws_ship_date_sk - ws_sold_date_sk > 90) and (ws_ship_date_sk - ws_sold_date_sk <= 120) then 1 else 0 end) as "91-120 days", sum(case when (ws_ship_date_sk - ws_sold_date_sk > 120) then 1 else 0 end) as ">120 days" from tpcds.web_sales, tpcds.warehouse, tpcds.ship_mode, tpcds.web_site, tpcds.date_dim where extract(year from d_date) = 1998 and ws_ship_date_sk = d_date_sk and ws_warehouse_sk = w_warehouse_sk and ws_ship_mode_sk = sm_ship_mode_sk and ws_web_site_sk = web_site_sk group by substr(w_warehouse_name,1,20), sm_type, web_name order by substr(w_warehouse_name,1,20), sm_type, web_name fetch first 100 rows only;
select * from (select i_manager_id, sum(ss_sales_price) sum_sales, avg(sum(ss_sales_price)) over (partition by i_manager_id) avg_monthly_sales from tpcds.item, tpcds.store_sales, tpcds.date_dim, tpcds.store where ss_item_sk = i_item_sk and ss_sold_date_sk = d_date_sk and ss_store_sk = s_store_sk and d_year in (1999) and (( i_category in ('Books','Children','Electronics') and i_class in ('personal','portable','refernece','self-help') and i_brand in ('scholaramalgamalg #14','scholaramalgamalg #7', 'exportiunivamalg #9','scholaramalgamalg #9')) or( i_category in ('Women','Music','Men') and i_class in ('accessories','classical','fragrances','pants') and i_brand in ('amalgimporto #1','edu packscholar #1','exportiimporto #1', 'importoamalg #1'))) group by i_manager_id, d_moy) tmp1 where case when avg_monthly_sales > 0 then abs (sum_sales - avg_monthly_sales) / avg_monthly_sales else null end > 0.1 order by i_manager_id, avg_monthly_sales, sum_sales fetch first 100 rows only;
with ss as ( select i_item_id,sum(ss_ext_sales_price) total_sales from tpcds.store_sales, tpcds.date_dim, tpcds.customer_address, tpcds.item where i_item_id in (select i_item_id from tpcds.item where i_category in ('Men')) and ss_item_sk = i_item_sk and ss_sold_date_sk = d_date_sk and d_year = 1999 and d_moy = 8 and ss_addr_sk = ca_address_sk and ca_gmt_offset = -6 group by i_item_id), cs as ( select i_item_id,sum(cs_ext_sales_price) total_sales from tpcds.catalog_sales, tpcds.date_dim, tpcds.customer_address, tpcds.item where i_item_id in (select i_item_id from tpcds.item where i_category in ('Men')) and cs_item_sk = i_item_sk and cs_sold_date_sk = d_date_sk and d_year = 1999 and d_moy = 8 and cs_bill_addr_sk = ca_address_sk and ca_gmt_offset = -6 group by i_item_id), ws as ( select i_item_id,sum(ws_ext_sales_price) total_sales from tpcds.web_sales, tpcds.date_dim, tpcds.customer_address, tpcds.item where i_item_id in (select i_item_id from tpcds.item where i_category in ('Men')) and ws_item_sk = i_item_sk and ws_sold_date_sk = d_date_sk and d_year = 1999 and d_moy = 8 and ws_bill_addr_sk = ca_address_sk and ca_gmt_offset = -6 group by i_item_id) select i_item_id ,sum(total_sales) total_sales from (select * from ss union all select * from cs union all select * from ws) tmp1 group by i_item_id order by i_item_id, total_sales fetch first 100 rows only;
with wss as (select d_week_seq, ss_store_sk, sum(case when (d_day_name='Sunday') then ss_sales_price else null end) sun_sales, sum(case when (d_day_name='Monday') then ss_sales_price else null end) mon_sales, sum(case when (d_day_name='Tuesday') then ss_sales_price else null end) tue_sales, sum(case when (d_day_name='Wednesday') then ss_sales_price else null end) wed_sales, sum(case when (d_day_name='Thursday') then ss_sales_price else null end) thu_sales, sum(case when (d_day_name='Friday') then ss_sales_price else null end) fri_sales, sum(case when (d_day_name='Saturday') then ss_sales_price else null end) sat_sales from tpcds.store_sales,tpcds.date_dim where d_date_sk = ss_sold_date_sk group by d_week_seq,ss_store_sk ) select s_store_name1,s_store_id1,d_week_seq1, sun_sales1/sun_sales2,mon_sales1/mon_sales2, tue_sales1/tue_sales1,wed_sales1/wed_sales2,thu_sales1/thu_sales2, fri_sales1/fri_sales2,sat_sales1/sat_sales2 from (select s_store_name s_store_name1,wss.d_week_seq d_week_seq1, s_store_id s_store_id1,sun_sales sun_sales1, mon_sales mon_sales1,tue_sales tue_sales1, wed_sales wed_sales1,thu_sales thu_sales1, fri_sales fri_sales1,sat_sales sat_sales1 from wss,tpcds.store,tpcds.date_dim d where d.d_week_seq = wss.d_week_seq and ss_store_sk = s_store_sk and d_year = 2000) y, (select s_store_name s_store_name2,wss.d_week_seq d_week_seq2, s_store_id s_store_id2,sun_sales sun_sales2, mon_sales mon_sales2,tue_sales tue_sales2, wed_sales wed_sales2,thu_sales thu_sales2, fri_sales fri_sales2,sat_sales sat_sales2 from wss,tpcds.store,tpcds.date_dim d where d.d_week_seq = wss.d_week_seq and ss_store_sk = s_store_sk and d_year = 2000+1) x where s_store_id1=s_store_id2 and d_week_seq1=d_week_seq2-52 order by s_store_name1,s_store_id1,d_week_seq1 fetch first 100 rows only;
select i_item_id, i_item_desc, i_current_price from tpcds.item, tpcds.inventory, tpcds.date_dim, tpcds.catalog_sales where i_current_price between 24 and 24 + 30 and inv_item_sk = i_item_sk and d_date_sk=inv_date_sk and d_date between cast('2001-02-08' as date) and (cast('2001-02-08' as date) + 60 days) and i_manufact_id in (946,774,749,744) and inv_quantity_on_hand between 100 and 500 and cs_item_sk = i_item_sk group by i_item_id,i_item_desc,i_current_price order by i_item_id fetch first 100 rows only;
select sum(ss_net_profit) as total_sum, s_state, s_county, grouping(s_state)+grouping(s_county) as lochierarchy, rank() over ( partition by grouping(s_state)+grouping(s_county), case when grouping(s_county) = 0 then s_state end order by sum(ss_net_profit) desc) as rank_within_parent from tpcds.store_sales, tpcds.date_dim d1, tpcds.store where d1.d_year = 2002 and d1.d_date_sk = ss_sold_date_sk and s_store_sk = ss_store_sk and s_state in ( select s_state from (select s_state as s_state, rank() over ( partition by s_state order by sum(ss_net_profit) desc) as ranking from tpcds.store_sales, tpcds.store, tpcds.date_dim where d_year =2002 and d_date_sk = ss_sold_date_sk and s_store_sk = ss_store_sk group by s_state ) tmp1 where ranking <= 5 ) group by rollup(s_state,s_county) order by lochierarchy desc, case when lochierarchy = 0 then s_state end, rank_within_parent fetch first 100 rows only;
select * from (select i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, s_store_id, sumsales, rank() over (partition by i_category order by sumsales desc) rk from (select i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, s_store_id, sum(coalesce(ss_sales_price*ss_quantity,0)) sumsales from tpcds.store_sales, tpcds.date_dim, tpcds.store, tpcds.item where ss_sold_date_sk=d_date_sk and ss_item_sk=i_item_sk and ss_store_sk = s_store_sk and d_year=1999 group by rollup(i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy,s_store_id))dw1) dw2 where rk <= 100 order by i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, s_store_id, sumsales, rk fetch first 100 rows only;
select * from (select avg(ss_list_price) B1_LP, count(ss_list_price) B1_CNT, count(distinct ss_list_price) B1_CNTD from tpcds.store_sales where ss_quantity between 0 and 5 and (ss_list_price between 137 and 137+10 or ss_coupon_amt between 12463 and 12463+1000 or ss_wholesale_cost between 11 and 11+20)) B1, (select avg(ss_list_price) B2_LP, count(ss_list_price) B2_CNT, count(distinct ss_list_price) B2_CNTD from tpcds.store_sales where ss_quantity between 6 and 10 and (ss_list_price between 33 and 33+10 or ss_coupon_amt between 11715 and 11715+1000 or ss_wholesale_cost between 28 and 28+20)) B2, (select avg(ss_list_price) B3_LP, count(ss_list_price) B3_CNT, count(distinct ss_list_price) B3_CNTD from tpcds.store_sales where ss_quantity between 11 and 15 and (ss_list_price between 129 and 129+10 or ss_coupon_amt between 976 and 976+1000 or ss_wholesale_cost between 15 and 15+20)) B3, (select avg(ss_list_price) B4_LP, count(ss_list_price) B4_CNT, count(distinct ss_list_price) B4_CNTD from tpcds.store_sales where ss_quantity between 16 and 20 and (ss_list_price between 16 and 16+10 or ss_coupon_amt between 13211 and 13211+1000 or ss_wholesale_cost between 10 and 10+20)) B4, (select avg(ss_list_price) B5_LP, count(ss_list_price) B5_CNT, count(distinct ss_list_price) B5_CNTD from tpcds.store_sales where ss_quantity between 21 and 25 and (ss_list_price between 104 and 104+10 or ss_coupon_amt between 5947 and 5947+1000 or ss_wholesale_cost between 0 and 0+20)) B5, (select avg(ss_list_price) B6_LP, count(ss_list_price) B6_CNT, count(distinct ss_list_price) B6_CNTD from tpcds.store_sales where ss_quantity between 26 and 30 and (ss_list_price between 27 and 27+10 or ss_coupon_amt between 10996 and 10996+1000 or ss_wholesale_cost between 32 and 32+20)) B6 fetch first 100 rows only;
with customer_total_return as (select cr_returning_customer_sk as ctr_customer_sk, ca_state as ctr_state, sum(cr_return_amt_inc_tax) as ctr_total_return from tpcds.catalog_returns, tpcds.date_dim, tpcds.customer_address where cr_returned_date_sk = d_date_sk and d_year =1999 and cr_returning_addr_sk = ca_address_sk group by cr_returning_customer_sk, ca_state ) select c_customer_id,c_salutation,c_first_name,c_last_name,ca_street_number,ca_street_name, ca_street_type,ca_suite_number,ca_city,ca_county,ca_state,ca_zip,ca_country,ca_gmt_offset, ca_location_type,ctr_total_return from customer_total_return ctr1, tpcds.customer_address, tpcds.customer where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2 from customer_total_return ctr2 where ctr1.ctr_state = ctr2.ctr_state) and ca_address_sk = c_current_addr_sk and ca_state = 'CA' and ctr1.ctr_customer_sk = c_customer_sk order by c_customer_id,c_salutation,c_first_name,c_last_name,ca_street_number,ca_street_name, ca_street_type,ca_suite_number,ca_city,ca_county,ca_state,ca_zip,ca_country,ca_gmt_offset, ca_location_type,ctr_total_return fetch first 100 rows only;
with ssci as ( select ss_customer_sk customer_sk, ss_item_sk item_sk from tpcds.store_sales,tpcds.date_dim where ss_sold_date_sk = d_date_sk and d_year=2001 group by ss_customer_sk, ss_item_sk), csci as( select cs_bill_customer_sk customer_sk, cs_item_sk item_sk from tpcds.catalog_sales,tpcds.date_dim where cs_sold_date_sk = d_date_sk and d_year=2001 group by cs_bill_customer_sk, cs_item_sk) select sum(case when ssci.customer_sk is not null and csci.customer_sk is null then 1 else 0 end) store_only, sum(case when ssci.customer_sk is null and csci.customer_sk is not null then 1 else 0 end) catalog_only, sum(case when ssci.customer_sk is not null and csci.customer_sk is not null then 1 else 0 end) store_and_catalog from ssci full outer join csci on (ssci.customer_sk=csci.customer_sk and ssci.item_sk = csci.item_sk) fetch first 100 rows only;
select w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country, ship_carriers, year, sum(jan_sales) as jan_sales, sum(feb_sales) as feb_sales, sum(mar_sales) as mar_sales, sum(apr_sales) as apr_sales, sum(may_sales) as may_sales, sum(jun_sales) as jun_sales, sum(jul_sales) as jul_sales, sum(aug_sales) as aug_sales, sum(sep_sales) as sep_sales, sum(oct_sales) as oct_sales, sum(nov_sales) as nov_sales, sum(dec_sales) as dec_sales, sum(jan_sales/w_warehouse_sq_ft) as jan_sales_per_sq_foot, sum(feb_sales/w_warehouse_sq_ft) as feb_sales_per_sq_foot, sum(mar_sales/w_warehouse_sq_ft) as mar_sales_per_sq_foot, sum(apr_sales/w_warehouse_sq_ft) as apr_sales_per_sq_foot, sum(may_sales/w_warehouse_sq_ft) as may_sales_per_sq_foot, sum(jun_sales/w_warehouse_sq_ft) as jun_sales_per_sq_foot, sum(jul_sales/w_warehouse_sq_ft) as jul_sales_per_sq_foot, sum(aug_sales/w_warehouse_sq_ft) as aug_sales_per_sq_foot, sum(sep_sales/w_warehouse_sq_ft) as sep_sales_per_sq_foot, sum(oct_sales/w_warehouse_sq_ft) as oct_sales_per_sq_foot, sum(nov_sales/w_warehouse_sq_ft) as nov_sales_per_sq_foot, sum(dec_sales/w_warehouse_sq_ft) as dec_sales_per_sq_foot, sum(jan_net) as jan_net, sum(feb_net) as feb_net, sum(mar_net) as mar_net, sum(apr_net) as apr_net, sum(may_net) as may_net, sum(jun_net) as jun_net, sum(jul_net) as jul_net, sum(aug_net) as aug_net, sum(sep_net) as sep_net, sum(oct_net) as oct_net, sum(nov_net) as nov_net, sum(dec_net) as dec_net from ( (select w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country, 'LATVIAN' || ',' || 'DHL' as ship_carriers, d_year as year, sum(case when d_moy = 1 then ws_sales_price* ws_quantity else 0 end) as jan_sales, sum(case when d_moy = 2 then ws_sales_price* ws_quantity else 0 end) as feb_sales, sum(case when d_moy = 3 then ws_sales_price* ws_quantity else 0 end) as mar_sales, sum(case when d_moy = 4 then ws_sales_price* ws_quantity else 0 end) as apr_sales, sum(case when d_moy = 5 then ws_sales_price* ws_quantity else 0 end) as may_sales, sum(case when d_moy = 6 then ws_sales_price* ws_quantity else 0 end) as jun_sales, sum(case when d_moy = 7 then ws_sales_price* ws_quantity else 0 end) as jul_sales, sum(case when d_moy = 8 then ws_sales_price* ws_quantity else 0 end) as aug_sales, sum(case when d_moy = 9 then ws_sales_price* ws_quantity else 0 end) as sep_sales, sum(case when d_moy = 10 then ws_sales_price* ws_quantity else 0 end) as oct_sales, sum(case when d_moy = 11 then ws_sales_price* ws_quantity else 0 end) as nov_sales, sum(case when d_moy = 12 then ws_sales_price* ws_quantity else 0 end) as dec_sales, sum(case when d_moy = 1 then ws_net_paid_inc_ship * ws_quantity else 0 end) as jan_net, sum(case when d_moy = 2 then ws_net_paid_inc_ship * ws_quantity else 0 end) as feb_net, sum(case when d_moy = 3 then ws_net_paid_inc_ship * ws_quantity else 0 end) as mar_net, sum(case when d_moy = 4 then ws_net_paid_inc_ship * ws_quantity else 0 end) as apr_net, sum(case when d_moy = 5 then ws_net_paid_inc_ship * ws_quantity else 0 end) as may_net, sum(case when d_moy = 6 then ws_net_paid_inc_ship * ws_quantity else 0 end) as jun_net, sum(case when d_moy = 7 then ws_net_paid_inc_ship * ws_quantity else 0 end) as jul_net, sum(case when d_moy = 8 then ws_net_paid_inc_ship * ws_quantity else 0 end) as aug_net, sum(case when d_moy = 9 then ws_net_paid_inc_ship * ws_quantity else 0 end) as sep_net, sum(case when d_moy = 10 then ws_net_paid_inc_ship * ws_quantity else 0 end) as oct_net, sum(case when d_moy = 11 then ws_net_paid_inc_ship * ws_quantity else 0 end) as nov_net, sum(case when d_moy = 12 then ws_net_paid_inc_ship * ws_quantity else 0 end) as dec_net from tpcds.web_sales, tpcds.warehouse, tpcds.date_dim, tpcds.time_dim, tpcds.ship_mode where ws_warehouse_sk = w_warehouse_sk and ws_sold_date_sk = d_date_sk and ws_sold_time_sk = t_time_sk and ws_ship_mode_sk = sm_ship_mode_sk and d_year = 2001 and t_time between 39920 and 39920+28800 and sm_carrier in ('LATVIAN','DHL') group by w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country, d_year ) union all (select w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country, 'LATVIAN' || ',' || 'DHL' as ship_carriers, d_year as year, sum(case when d_moy = 1 then cs_sales_price* cs_quantity else 0 end) as jan_sales, sum(case when d_moy = 2 then cs_sales_price* cs_quantity else 0 end) as feb_sales, sum(case when d_moy = 3 then cs_sales_price* cs_quantity else 0 end) as mar_sales, sum(case when d_moy = 4 then cs_sales_price* cs_quantity else 0 end) as apr_sales, sum(case when d_moy = 5 then cs_sales_price* cs_quantity else 0 end) as may_sales, sum(case when d_moy = 6 then cs_sales_price* cs_quantity else 0 end) as jun_sales, sum(case when d_moy = 7 then cs_sales_price* cs_quantity else 0 end) as jul_sales, sum(case when d_moy = 8 then cs_sales_price* cs_quantity else 0 end) as aug_sales, sum(case when d_moy = 9 then cs_sales_price* cs_quantity else 0 end) as sep_sales, sum(case when d_moy = 10 then cs_sales_price* cs_quantity else 0 end) as oct_sales, sum(case when d_moy = 11 then cs_sales_price* cs_quantity else 0 end) as nov_sales, sum(case when d_moy = 12 then cs_sales_price* cs_quantity else 0 end) as dec_sales, sum(case when d_moy = 1 then cs_net_paid_inc_tax * cs_quantity else 0 end) as jan_net, sum(case when d_moy = 2 then cs_net_paid_inc_tax * cs_quantity else 0 end) as feb_net, sum(case when d_moy = 3 then cs_net_paid_inc_tax * cs_quantity else 0 end) as mar_net, sum(case when d_moy = 4 then cs_net_paid_inc_tax * cs_quantity else 0 end) as apr_net, sum(case when d_moy = 5 then cs_net_paid_inc_tax * cs_quantity else 0 end) as may_net, sum(case when d_moy = 6 then cs_net_paid_inc_tax * cs_quantity else 0 end) as jun_net, sum(case when d_moy = 7 then cs_net_paid_inc_tax * cs_quantity else 0 end) as jul_net, sum(case when d_moy = 8 then cs_net_paid_inc_tax * cs_quantity else 0 end) as aug_net, sum(case when d_moy = 9 then cs_net_paid_inc_tax * cs_quantity else 0 end) as sep_net, sum(case when d_moy = 10 then cs_net_paid_inc_tax * cs_quantity else 0 end) as oct_net, sum(case when d_moy = 11 then cs_net_paid_inc_tax * cs_quantity else 0 end) as nov_net, sum(case when d_moy = 12 then cs_net_paid_inc_tax * cs_quantity else 0 end) as dec_net from tpcds.catalog_sales, tpcds.warehouse, tpcds.date_dim, tpcds.time_dim, tpcds.ship_mode where cs_warehouse_sk = w_warehouse_sk and cs_sold_date_sk = d_date_sk and cs_sold_time_sk = t_time_sk and cs_ship_mode_sk = sm_ship_mode_sk and d_year = 2001 and t_time between 39920 AND 39920+28800 and sm_carrier in ('LATVIAN','DHL') group by w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country, d_year ) ) x group by w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country, ship_carriers, year order by w_warehouse_name fetch first 100 rows only;
select cast(amc as decimal(15,4))/cast(pmc as decimal(15,4)) am_pm_ratio from ( select count(*) amc from tpcds.web_sales, tpcds.household_demographics , tpcds.time_dim, tpcds.web_page where ws_sold_time_sk = tpcds.time_dim.t_time_sk and ws_ship_hdemo_sk = tpcds.household_demographics.hd_demo_sk and ws_web_page_sk = tpcds.web_page.wp_web_page_sk and tpcds.time_dim.t_hour between 7 and 7+1 and tpcds.household_demographics.hd_dep_count = 0 and tpcds.web_page.wp_char_count between 5000 and 5200) at, ( select count(*) pmc from tpcds.web_sales, tpcds.household_demographics , tpcds.time_dim, tpcds.web_page where ws_sold_time_sk = tpcds.time_dim.t_time_sk and ws_ship_hdemo_sk = tpcds.household_demographics.hd_demo_sk and ws_web_page_sk = tpcds.web_page.wp_web_page_sk and tpcds.time_dim.t_hour between 17 and 17+1 and tpcds.household_demographics.hd_dep_count = 0 and tpcds.web_page.wp_char_count between 5000 and 5200) pt order by am_pm_ratio fetch first 100 rows only;
with ws_wh as (select ws1.ws_order_number,ws1.ws_warehouse_sk wh1,ws2.ws_warehouse_sk wh2 from tpcds.web_sales ws1,tpcds.web_sales ws2 where ws1.ws_order_number = ws2.ws_order_number and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk) select count(distinct ws_order_number) as "order count", sum(ws_ext_ship_cost) as "total shipping cost", sum(ws_net_profit) as "total net profit" from tpcds.web_sales ws1, tpcds.date_dim, tpcds.customer_address, tpcds.web_site where d_date between '1999-3-01' and (cast('1999-3-01' as date) + 60 days) and ws1.ws_ship_date_sk = d_date_sk and ws1.ws_ship_addr_sk = ca_address_sk and ca_state = 'IN' and ws1.ws_web_site_sk = web_site_sk and web_company_name = 'pri' and ws1.ws_order_number in (select ws_order_number from ws_wh) and ws1.ws_order_number in (select wr_order_number from tpcds.web_returns,ws_wh where wr_order_number = ws_wh.ws_order_number) order by count(distinct ws_order_number) fetch first 100 rows only;
select sum(ws_ext_discount_amt) as "Excess Discount Amount" from tpcds.web_sales, tpcds.item, tpcds.date_dim where i_manufact_id = 675 and i_item_sk = ws_item_sk and d_date between '1998-01-30' and (cast('1998-01-30' as date) + 90 days) and d_date_sk = ws_sold_date_sk and ws_ext_discount_amt > ( SELECT 1.3 * avg(ws_ext_discount_amt) FROM tpcds.web_sales, tpcds.date_dim WHERE ws_item_sk = i_item_sk and d_date between '1998-01-30' and (cast('1998-01-30' as date) + 90 days) and d_date_sk = ws_sold_date_sk ) order by sum(ws_ext_discount_amt) fetch first 100 rows only;
select dt.d_year, tpcds.item.i_brand_id brand_id, tpcds.item.i_brand brand, sum(ss_ext_sales_price) ext_price from tpcds.date_dim dt, tpcds.store_sales, tpcds.item where dt.d_date_sk = tpcds.store_sales.ss_sold_date_sk and tpcds.store_sales.ss_item_sk = tpcds.item.i_item_sk and tpcds.item.i_manufact_id = 741 and dt.d_moy=11 group by dt.d_year, tpcds.item.i_brand, tpcds.item.i_brand_id order by dt.d_year, ext_price desc, brand_id fetch first 100 rows only;


-- 73049
-- insert new date record
insert INTO tpcds.date_dim
(
	D_DATE_SK,
	D_DATE_ID,
	D_DATE,
	D_MONTH_SEQ,
	D_WEEK_SEQ,
	D_QUARTER_SEQ,
	D_YEAR,
	D_DOW,
	D_MOY,
	D_DOM,
	D_QOY,
	D_FY_YEAR,
	D_FY_QUARTER_SEQ,
	D_FY_WEEK_SEQ,
	D_DAY_NAME,
	D_QUARTER_NAME,
	D_HOLIDAY,
	D_WEEKEND,
	D_FOLLOWING_HOLIDAY,
	D_FIRST_DOM,
	D_LAST_DOM,
	D_SAME_DAY_LY,
	D_SAME_DAY_LQ,
	D_CURRENT_DAY,
	D_CURRENT_WEEK,
	D_CURRENT_MONTH,
	D_CURRENT_QUARTER,
	D_CURRENT_YEAR
) values (
	2500001,
	'AAAAAAAAOKJNECAB',
	'2012-12-21',
	0,
	1,
	1,
	2012,
	1,
	12,
	21,
	1,
	2012,
	1,
	1,
	'Monday   ',
	'2012Q4',
	'N',
	'N',
	'Y',
	2415021,
	2415020,
	2414657,
	2414930,
	'N',
	'N',
	'N',
	'N',
	'N'
);

-- 25404658
-- new store sales
insert into tpcds.store_sales
(
	SS_SOLD_DATE_SK,
	SS_SOLD_TIME_SK,
	SS_ITEM_SK,
	SS_CUSTOMER_SK,
	SS_CDEMO_SK,
	SS_HDEMO_SK,
	SS_ADDR_SK,
	SS_STORE_SK,
	SS_PROMO_SK,
	SS_TICKET_NUMBER,
	SS_QUANTITY,
	SS_WHOLESALE_COST,
	SS_LIST_PRICE,
	SS_SALES_PRICE,
	SS_EXT_DISCOUNT_AMT,
	SS_EXT_SALES_PRICE,
	SS_EXT_WHOLESALE_COST,
	SS_EXT_LIST_PRICE,
	SS_EXT_TAX,
	SS_COUPON_AMT,
	SS_NET_PAID,
	SS_NET_PAID_INC_TAX,
	SS_NET_PROFIT
) values (
	2500001,
	44911,
	97607,
	292215,
	1517913,
	1564,
	147662,
	16,
	40,
	2400001,
	29,
	87.52,
	134.78,
	52.56,
	0.00,
	1524.24,
	2538.08,
	3908.62,
	15.24,
	0.00,
	1524.24,
	1539.48,
	-1013.84
);

-- 2536607
-- new store return
insert into tpcds.store_returns
(
	SR_RETURNED_DATE_SK,
	SR_RETURN_TIME_SK,
	SR_ITEM_SK,
	SR_CUSTOMER_SK,
	SR_CDEMO_SK,
	SR_HDEMO_SK,
	SR_ADDR_SK,
	SR_STORE_SK,
	SR_REASON_SK,
	SR_TICKET_NUMBER,
	SR_RETURN_QUANTITY,
	SR_RETURN_AMT,
	SR_RETURN_TAX,
	SR_RETURN_AMT_INC_TAX,
	SR_FEE,
	SR_RETURN_SHIP_COST,
	SR_REFUNDED_CASH,
	SR_REVERSED_CHARGE,
	SR_STORE_CREDIT,
	SR_NET_LOSS
) values (
	2500001,
	48457,
	89115,
	292215,
	275732,
	5766,
	97353,
	58,
	11,
	2400001,
	12,
	3.96,
	0.00,
	3.96,
	24.57,
	198.60,
	3.44,
	0.43,
	0.09,
	223.17
);

-- 14401261
-- new catalog sales
insert into tpcds.catalog_sales
(
	CS_SOLD_DATE_SK,
	CS_SOLD_TIME_SK,
	CS_SHIP_DATE_SK,
	CS_BILL_CUSTOMER_SK,
	CS_BILL_CDEMO_SK,
	CS_BILL_HDEMO_SK,
	CS_BILL_ADDR_SK,
	CS_SHIP_CUSTOMER_SK,
	CS_SHIP_CDEMO_SK,
	CS_SHIP_HDEMO_SK,
	CS_SHIP_ADDR_SK,
	CS_CALL_CENTER_SK,
	CS_CATALOG_PAGE_SK,
	CS_SHIP_MODE_SK,
	CS_WAREHOUSE_SK,
	CS_ITEM_SK,
	CS_PROMO_SK,
	CS_ORDER_NUMBER,
	CS_QUANTITY,
	CS_WHOLESALE_COST,
	CS_LIST_PRICE,
	CS_SALES_PRICE,
	CS_EXT_DISCOUNT_AMT,
	CS_EXT_SALES_PRICE,
	CS_EXT_WHOLESALE_COST,
	CS_EXT_LIST_PRICE,
	CS_EXT_TAX,
	CS_COUPON_AMT,
	CS_EXT_SHIP_COST,
	CS_NET_PAID,
	CS_NET_PAID_INC_TAX,
	CS_NET_PAID_INC_SHIP,
	CS_NET_PAID_INC_SHIP_TAX,
	CS_NET_PROFIT
) values (
	2452653,
	12032,
	2452712,
	378549,
	1208754,
	3312,
	189117,
	378549,
	1208754,
	3312,
	189117,
	7,
	10084,
	16,
	8,
	50503,
	469,
	1600001,
	20,
	72.97,
	77.34,
	26.29,
	1021.00,
	525.80,
	1459.40,
	1546.80,
	0.00,
	0.00,
	665.00,
	525.80,
	525.80,
	1190.80,
	1190.80,
	-933.60
);

-- 1439749
-- new catalog return
insert into tpcds.catalog_returns
(
	CR_RETURNED_DATE_SK,
	CR_RETURNED_TIME_SK,
	CR_ITEM_SK,
	CR_REFUNDED_CUSTOMER_SK,
	CR_REFUNDED_CDEMO_SK,
	CR_REFUNDED_HDEMO_SK,
	CR_REFUNDED_ADDR_SK,
	CR_RETURNING_CUSTOMER_SK,
	CR_RETURNING_CDEMO_SK,
	CR_RETURNING_HDEMO_SK,
	CR_RETURNING_ADDR_SK,
	CR_CALL_CENTER_SK,
	CR_CATALOG_PAGE_SK,
	CR_SHIP_MODE_SK,
	CR_WAREHOUSE_SK,
	CR_REASON_SK,
	CR_ORDER_NUMBER,
	CR_RETURN_QUANTITY,
	CR_RETURN_AMOUNT,
	CR_RETURN_TAX,
	CR_RETURN_AMT_INC_TAX,
	CR_FEE,
	CR_RETURN_SHIP_COST,
	CR_REFUNDED_CASH,
	CR_REVERSED_CHARGE,
	CR_STORE_CREDIT,
	CR_NET_LOSS
) values (
	2452857,
	35897,
	86055,
	14358,
	1439035,
	5312,
	53989,
	14358,
	1439035,
	1903,
	53989,
	3,
	10032,
	20,
	9,
	35,
	1599999,
	48,
	5242.56,
	0.00,
	5242.56,
	31.59,
	245.28,
	3827.06,
	1415.50,
	0.00,
	276.87
);

--7197566
insert into tpcds.web_sales
(
	WS_SOLD_DATE_SK,
	WS_SOLD_TIME_SK,
	WS_SHIP_DATE_SK,
	WS_ITEM_SK,
	WS_BILL_CUSTOMER_SK,
	WS_BILL_CDEMO_SK,
	WS_BILL_HDEMO_SK,
	WS_BILL_ADDR_SK,
	WS_SHIP_CUSTOMER_SK,
	WS_SHIP_CDEMO_SK,
	WS_SHIP_HDEMO_SK,
	WS_SHIP_ADDR_SK,
	WS_WEB_PAGE_SK,
	WS_WEB_SITE_SK,
	WS_SHIP_MODE_SK,
	WS_WAREHOUSE_SK,
	WS_PROMO_SK,
	WS_ORDER_NUMBER,
	WS_QUANTITY,
	WS_WHOLESALE_COST,
	WS_LIST_PRICE,
	WS_SALES_PRICE,
	WS_EXT_DISCOUNT_AMT,
	WS_EXT_SALES_PRICE,
	WS_EXT_WHOLESALE_COST,
	WS_EXT_LIST_PRICE,
	WS_EXT_TAX,
	WS_COUPON_AMT,
	WS_EXT_SHIP_COST,
	WS_NET_PAID,
	WS_NET_PAID_INC_TAX,
	WS_NET_PAID_INC_SHIP,
	WS_NET_PAID_INC_SHIP_TAX,
	WS_NET_PROFIT
) values (
	2451041,
	45542,
	2451066,
	53618,
	320172,
	1441008,
	2644,
	237927,
	154410,
	893289,
	482,
	133718,
	160,
	20,
	7,
	2,
	324,
	600001,
	83,
	9.78,
	28.45,
	13.37,
	1251.64,
	1109.71,
	811.74,
	2361.35,
	23.85,
	632.53,
	731.23,
	477.18,
	501.03,
	1208.41,
	1232.26,
	-334.56
);

--719217
insert into tpcds.web_returns
(
	WR_RETURNED_DATE_SK,
	WR_RETURNED_TIME_SK,
	WR_ITEM_SK,
	WR_REFUNDED_CUSTOMER_SK,
	WR_REFUNDED_CDEMO_SK,
	WR_REFUNDED_HDEMO_SK,
	WR_REFUNDED_ADDR_SK,
	WR_RETURNING_CUSTOMER_SK,
	WR_RETURNING_CDEMO_SK,
	WR_RETURNING_HDEMO_SK,
	WR_RETURNING_ADDR_SK,
	WR_WEB_PAGE_SK,
	WR_REASON_SK,
	WR_ORDER_NUMBER,
	WR_RETURN_QUANTITY,
	WR_RETURN_AMT,
	WR_RETURN_TAX,
	WR_RETURN_AMT_INC_TAX,
	WR_FEE,
	WR_RETURN_SHIP_COST,
	WR_REFUNDED_CASH,
	WR_REVERSED_CHARGE,
	WR_ACCOUNT_CREDIT,
	WR_NET_LOSS
) values (
	2451337,
	58039,
	28573,
	100963,
	671148,
	1780,
	139165,
	100963,
	671148,
	1780,
	139165,
	169,
	41,
	600001,
	11,
	546.04,
	21.84,
	567.88,
	58.84,
	95.70,
	92.82,
	203.94,
	249.28,
	176.38
);



with web_v1 as ( select ws_item_sk item_sk, d_date, sum(sum(ws_sales_price)) over (partition by ws_item_sk order by d_date rows between unbounded preceding and current row) cume_sales from tpcds.web_sales, tpcds.date_dim where ws_sold_date_sk=d_date_sk and d_year=1998 and ws_item_sk is not NULL group by ws_item_sk, d_date), store_v1 as ( select ss_item_sk item_sk, d_date, sum(sum(ss_sales_price)) over (partition by ss_item_sk order by d_date rows between unbounded preceding and current row) cume_sales from tpcds.store_sales, tpcds.date_dim where ss_sold_date_sk=d_date_sk and d_year=1998 and ss_item_sk is not NULL group by ss_item_sk, d_date) select * from (select item_sk, d_date, web_sales, store_sales, max(web_sales) over (partition by item_sk order by d_date rows between unbounded preceding and current row) web_cumulative, max(store_sales) over (partition by item_sk order by d_date rows between unbounded preceding and current row) store_cumulative from (select case when web.item_sk is not null then web.item_sk else store.item_sk end item_sk, case when web.d_date is not null then web.d_date else store.d_date end d_date, web.cume_sales web_sales, store.cume_sales store_sales from web_v1 web full outer join store_v1 store on (web.item_sk = store.item_sk and web.d_date = store.d_date) )x )y where web_cumulative > store_cumulative order by item_sk, d_date fetch first 100 rows only;
select 'web' as channel, web.item, web.return_ratio, web.return_rank, web.currency_rank from ( select item, return_ratio, currency_ratio, rank() over (order by return_ratio) as return_rank, rank() over (order by currency_ratio) as currency_rank from ( select ws.ws_item_sk as item, (cast(sum(coalesce(wr.wr_return_quantity,0)) as dec(15,4))/ cast(sum(coalesce(ws.ws_quantity,0)) as dec(15,4) )) as return_ratio, (cast(sum(coalesce(wr.wr_return_amt,0)) as dec(15,4))/ cast(sum(coalesce(ws.ws_net_paid,0)) as dec(15,4) )) as currency_ratio from tpcds.web_sales ws left outer join tpcds.web_returns wr on (ws.ws_order_number = wr.wr_order_number and ws.ws_item_sk = wr.wr_item_sk), tpcds.date_dim where wr.wr_return_amt > 10000 and ws.ws_net_profit > 1 and ws.ws_net_paid > 0 and ws.ws_quantity > 0 and ws_sold_date_sk = d_date_sk and d_year = 2002 and d_moy = 11 group by ws.ws_item_sk ) in_web ) web where ( web.return_rank <= 10 or web.currency_rank <= 10 ) union select 'catalog' as channel, catalog.item, catalog.return_ratio, catalog.return_rank, catalog.currency_rank from ( select item, return_ratio, currency_ratio, rank() over (order by return_ratio) as return_rank, rank() over ( order by currency_ratio) as currency_rank from (select cs.cs_item_sk as item, (cast(sum(coalesce(cr.cr_return_quantity,0)) as dec(15,4))/ cast(sum(coalesce(cs.cs_quantity,0)) as dec(15,4) )) as return_ratio, (cast(sum(coalesce(cr.cr_return_amount,0)) as dec(15,4))/ cast(sum(coalesce(cs.cs_net_paid,0)) as dec(15,4) )) as currency_ratio from tpcds.catalog_sales cs left outer join tpcds.catalog_returns cr on (cs.cs_order_number = cr.cr_order_number and cs.cs_item_sk = cr.cr_item_sk), tpcds.date_dim where cr.cr_return_amount > 10000 and cs.cs_net_profit > 1 and cs.cs_net_paid > 0 and cs.cs_quantity > 0 and cs_sold_date_sk = d_date_sk and d_year = 2002 and d_moy = 11 group by cs.cs_item_sk ) in_cat ) catalog where ( catalog.return_rank <= 10 or catalog.currency_rank <=10 ) union select 'store' as channel, store.item, store.return_ratio, store.return_rank, store.currency_rank from ( select item, return_ratio, currency_ratio, rank() over (order by return_ratio) as return_rank, rank() over (order by currency_ratio) as currency_rank from ( select sts.ss_item_sk as item, (cast(sum(coalesce(sr.sr_return_quantity,0)) as dec(15,4))/cast(sum(coalesce(sts.ss_quantity,0)) as dec(15,4) )) as return_ratio, (cast(sum(coalesce(sr.sr_return_amt,0)) as dec(15,4))/cast(sum(coalesce(sts.ss_net_paid,0)) as dec(15,4) )) as currency_ratio from tpcds.store_sales sts left outer join tpcds.store_returns sr on (sts.ss_ticket_number = sr.sr_ticket_number and sts.ss_item_sk = sr.sr_item_sk), tpcds.date_dim where sr.sr_return_amt > 10000 and sts.ss_net_profit > 1 and sts.ss_net_paid > 0 and sts.ss_quantity > 0 and ss_sold_date_sk = d_date_sk and d_year = 2002 and d_moy = 11 group by sts.ss_item_sk ) in_store ) store where ( store.return_rank <= 10 or store.currency_rank <= 10 ) order by 1,4,5 fetch first 100 rows only;
select case when (select count(*) from tpcds.store_sales where ss_quantity between 1 and 20) > 0.001*219075 then (select avg(ss_ext_list_price) from tpcds.store_sales where ss_quantity between 1 and 20) else (select avg(ss_net_profit) from tpcds.store_sales where ss_quantity between 1 and 20) end bucket1 , case when (select count(*) from tpcds.store_sales where ss_quantity between 21 and 40) > 0.001*55037 then (select avg(ss_ext_list_price) from tpcds.store_sales where ss_quantity between 21 and 40) else (select avg(ss_net_profit) from tpcds.store_sales where ss_quantity between 21 and 40) end bucket2, case when (select count(*) from tpcds.store_sales where ss_quantity between 41 and 60) > 0.001*28293 then (select avg(ss_ext_list_price) from tpcds.store_sales where ss_quantity between 41 and 60) else (select avg(ss_net_profit) from tpcds.store_sales where ss_quantity between 41 and 60) end bucket3, case when (select count(*) from tpcds.store_sales where ss_quantity between 61 and 80) > 0.001*104204 then (select avg(ss_ext_list_price) from tpcds.store_sales where ss_quantity between 61 and 80) else (select avg(ss_net_profit) from tpcds.store_sales where ss_quantity between 61 and 80) end bucket4, case when (select count(*) from tpcds.store_sales where ss_quantity between 81 and 100) > 0.001*141976 then (select avg(ss_ext_list_price) from tpcds.store_sales where ss_quantity between 81 and 100) else (select avg(ss_net_profit) from tpcds.store_sales where ss_quantity between 81 and 100) end bucket5 from tpcds.reason where r_reason_sk = 1 fetch first 100 rows only;
with ss as (select ca_county,d_qoy, d_year,sum(ss_ext_sales_price) as store_sales from tpcds.store_sales,tpcds.date_dim,tpcds.customer_address where ss_sold_date_sk = d_date_sk and ss_addr_sk=ca_address_sk group by ca_county,d_qoy, d_year), ws as (select ca_county,d_qoy, d_year,sum(ws_ext_sales_price) as web_sales from tpcds.web_sales,tpcds.date_dim,tpcds.customer_address where ws_sold_date_sk = d_date_sk and ws_bill_addr_sk=ca_address_sk group by ca_county,d_qoy, d_year) select /* tt */ ss1.ca_county, ss1.d_year, ws2.web_sales/ws1.web_sales web_q1_q2_increase, ss2.store_sales/ss1.store_sales store_q1_q2_increase, ws3.web_sales/ws2.web_sales web_q2_q3_increase, ss3.store_sales/ss2.store_sales store_q2_q3_increase from ss ss1, ss ss2, ss ss3, ws ws1, ws ws2, ws ws3 where ss1.d_qoy = 1 and ss1.d_year = 2001 and ss1.ca_county = ss2.ca_county and ss2.d_qoy = 2 and ss2.d_year = 2001 and ss2.ca_county = ss3.ca_county and ss3.d_qoy = 3 and ss3.d_year = 2001 and ss1.ca_county = ws1.ca_county and ws1.d_qoy = 1 and ws1.d_year = 2001 and ws1.ca_county = ws2.ca_county and ws2.d_qoy = 2 and ws2.d_year = 2001 and ws1.ca_county = ws3.ca_county and ws3.d_qoy = 3 and ws3.d_year =2001 and case when ws1.web_sales > 0 then ws2.web_sales/ws1.web_sales else null end > case when ss1.store_sales > 0 then ss2.store_sales/ss1.store_sales else null end and case when ws2.web_sales > 0 then ws3.web_sales/ws2.web_sales else null end > case when ss2.store_sales > 0 then ss3.store_sales/ss2.store_sales else null end order by ss1.ca_county;
select ss_customer_sk, sum(act_sales) sumsales from (select ss_item_sk, ss_ticket_number, ss_customer_sk, case when sr_return_quantity is not null then (ss_quantity-sr_return_quantity)*ss_sales_price else (ss_quantity*ss_sales_price) end act_sales from tpcds.store_sales left outer join tpcds.store_returns on (sr_item_sk = ss_item_sk and sr_ticket_number = ss_ticket_number), tpcds.reason where sr_reason_sk = r_reason_sk and r_reason_desc = 'reason 67') t group by ss_customer_sk order by sumsales, ss_customer_sk fetch first 100 rows only;
select count(*) from ( select distinct c_last_name, c_first_name, d_date from tpcds.store_sales, tpcds.date_dim, tpcds.customer where tpcds.store_sales.ss_sold_date_sk = tpcds.date_dim.d_date_sk and tpcds.store_sales.ss_customer_sk = tpcds.customer.c_customer_sk and d_year = 1999 intersect select distinct c_last_name, c_first_name, d_date from tpcds.catalog_sales, tpcds.date_dim, tpcds.customer where tpcds.catalog_sales.cs_sold_date_sk = tpcds.date_dim.d_date_sk and tpcds.catalog_sales.cs_bill_customer_sk = tpcds.customer.c_customer_sk and d_year = 1999 intersect select distinct c_last_name, c_first_name, d_date from tpcds.web_sales, tpcds.date_dim, tpcds.customer where tpcds.web_sales.ws_sold_date_sk = tpcds.date_dim.d_date_sk and tpcds.web_sales.ws_bill_customer_sk = tpcds.customer.c_customer_sk and d_year = 1999 ) hot_cust fetch first 100 rows only;
select * from( select i_category, i_class, i_brand, s_store_name, s_company_name, d_moy, sum(ss_sales_price) sum_sales, avg(sum(ss_sales_price)) over (partition by i_category, i_brand, s_store_name, s_company_name) avg_monthly_sales from tpcds.item, tpcds.store_sales, tpcds.date_dim, tpcds.store where ss_item_sk = i_item_sk and ss_sold_date_sk = d_date_sk and ss_store_sk = s_store_sk and d_year in (1999) and ((i_category in ('Electronics','Sports','Shoes') and i_class in ('dvd/vcr players','guns','kids') ) or (i_category in ('Music','Women','Men') and i_class in ('pop','maternity','shirts') )) group by i_category, i_class, i_brand, s_store_name, s_company_name, d_moy) tmp1 where case when (avg_monthly_sales <> 0) then (abs(sum_sales - avg_monthly_sales) / avg_monthly_sales) else null end > 0.1 order by sum_sales - avg_monthly_sales, s_store_name fetch first 100 rows only;
select ca_zip, sum(cs_sales_price) from tpcds.catalog_sales, tpcds.customer, tpcds.customer_address, tpcds.date_dim where cs_bill_customer_sk = c_customer_sk and c_current_addr_sk = ca_address_sk and ( substr(ca_zip,1,5) in ('85669', '86197','88274','83405','86475', '85392', '85460', '80348', '81792') or ca_state in ('CA','WA','GA') or cs_sales_price > 500) and cs_sold_date_sk = d_date_sk and d_qoy = 1 and d_year = 1998 group by ca_zip order by ca_zip fetch first 100 rows only;
select dt.d_year, tpcds.item.i_brand_id brand_id, tpcds.item.i_brand brand, sum(ss_ext_sales_price) ext_price from tpcds.date_dim dt, tpcds.store_sales, tpcds.item where dt.d_date_sk = tpcds.store_sales.ss_sold_date_sk and tpcds.store_sales.ss_item_sk = tpcds.item.i_item_sk and tpcds.item.i_manager_id = 1 and dt.d_moy=11 and dt.d_year=1998 group by dt.d_year, tpcds.item.i_brand, tpcds.item.i_brand_id order by dt.d_year, ext_price desc, brand_id fetch first 100 rows only ;
select dt.d_year, tpcds.item.i_category_id, tpcds.item.i_category, sum(ss_ext_sales_price) from tpcds.date_dim dt, tpcds.store_sales, tpcds.item where dt.d_date_sk = tpcds.store_sales.ss_sold_date_sk and tpcds.store_sales.ss_item_sk = tpcds.item.i_item_sk and tpcds.item.i_manager_id = 1 and dt.d_moy=12 and dt.d_year=1998 group by dt.d_year, tpcds.item.i_category_id, tpcds.item.i_category order by sum(ss_ext_sales_price) desc,dt.d_year, tpcds.item.i_category_id, tpcds.item.i_category fetch first 100 rows only ;
select s_store_name, sum(ss_net_profit) from tpcds.store_sales, tpcds.date_dim, tpcds.store, (select ca_zip from ( (SELECT substr(ca_zip,1,5) ca_zip FROM tpcds.customer_address WHERE substr(ca_zip,1,5) IN ( '28770','22406','84990','37601','12148','16006', '48732','14368','15447','12262','70483', '37463','15631','17727','10225','59176', '37668','65776','96083','90946','18674', '73506','13509','32137','33329','50534', '38862','42996','90592','99213','71363', '72890','33471','14265','61953','81674', '23523','13291','76248','13250','83607', '58749','27117','73867','88501','13213', '36285','16498','75897','85038','79615', '29044','46745','85381','97906','25235', '41927','13652','20504','22254','28616', '75431','98243','96508','43266','27218', '84385','30353','24834','93747','64144', '92994','27098','93158','56997','87436', '27931','27487','65275','55960','68863', '66290','29308','11111','89041','40351', '83542','96502','73055','76810','53337', '89572','37471','11257','90071','42520', '15877','75323','42374','36082','15023', '42900','70892','18404','46638','94480', '17423','31700','52142','54785','86220', '14887','50307','67866','49674','70436', '45055','84774','32385','72038','60421', '23462','11446','89945','98404','65478', '20749','11845','89409','59193','57687', '31236','33530','67003','89889','33164', '12292','60881','32203','71270','14911', '18844','18397','26767','39437','92108', '17246','53357','74612','53120','68632', '92018','14345','98111','65622','54706', '44130','10562','78330','69194','46593', '15627','14535','44655','78184','88106', '57470','98878','16415','64353','57172', '40824','12324','52012','74349','80323', '98734','70474','59159','51115','15003', '19908','18966','57066','55696','85300', '40581','39259','15304','54812','40369', '78396','58585','80079','68927','69080', '45340','30410','23271','23904','63204', '45908','54573','90070','68085','34418', '73265','70679','33661','45403','62985', '12823','33928','97817','63093','37708', '21009','40023','17744','45052','47249', '92933','59027','45762','23371','88295', '35445','77562','19072','18946','14501', '69144','10053','60091','10314','77319', '15961','19445','83317','64805','41996', '76825','13170','15264','83225','87248', '27749','39882','92688','81484','44811', '54208','55926','46237','55279','98696', '49830','60510','24261','90107','94192', '41609','77937','58737','28892','38216', '22861','92818','95022','59181','51971', '42538','20625','61787','43567','20148', '79376','20804','62246','49326','34819', '66972','15524','27116','57341','47760', '33953','37850','57830','18743','14182', '12789','18557','59696','96434','11101', '76340','12011','46097','27759','24084', '57428','30971','14719','82718','78582', '45030','91113','33945','71368','56369', '15129','96700','49947','11505','82544', '69959','75495','90786','49227','80358', '79028','27643','16330','47496','16775', '96139','39035','77772','93128','36485', '95729','10519','54218','43962','42289', '35895','34201','54832','30179','55376', '46077','87339','43883','30289','36111', '14359','89298','20746','64670','83780', '36901','11968','44193','87950','61889', '41788','35049','34862','10379','36987', '20434','26946','87414','29553','96117', '90604','33734','11414','16206','15583', '45042','48182','74138','10101','58855', '17133','79043','37181','36630','20262', '55210','62061','71395','32984','64643', '43620','92523','54314','98650','75682', '67356','59497','58949','62109','88703', '57638','92292','20159','38548')) intersect (select ca_zip from (SELECT substr(ca_zip,1,5) ca_zip,count(*) cnt FROM tpcds.customer_address, tpcds.customer WHERE ca_address_sk = c_current_addr_sk and c_preferred_cust_flag='Y' group by ca_zip having count(*) > 10)A1))A2) V1 where ss_store_sk = s_store_sk and ss_sold_date_sk = d_date_sk and d_qoy = 1 and d_year = 2001 and (substr(s_zip,1,2) = substr(V1.ca_zip,1,2)) group by s_store_name order by s_store_name fetch first 100 rows only;
select i_item_id, i_item_desc, i_current_price from tpcds.item, tpcds.inventory, tpcds.date_dim, tpcds.store_sales where i_current_price between 31 and 31+30 and inv_item_sk = i_item_sk and d_date_sk=inv_date_sk and d_date between cast('2002-05-18' as date) and (cast('2002-05-18' as date) + 60 days) and i_manufact_id in (867,107,602,451) and inv_quantity_on_hand between 100 and 500 and ss_item_sk = i_item_sk group by i_item_id,i_item_desc,i_current_price order by i_item_id fetch first 100 rows only;
with v1 as( select i_category, i_brand, cc_name, d_year, d_moy, sum(cs_sales_price) sum_sales, avg(sum(cs_sales_price)) over (partition by i_category, i_brand, cc_name, d_year) avg_monthly_sales, rank() over (partition by i_category, i_brand, cc_name order by d_year, d_moy) rn from tpcds.item, tpcds.catalog_sales, tpcds.date_dim, tpcds.call_center where cs_item_sk = i_item_sk and cs_sold_date_sk = d_date_sk and cc_call_center_sk= cs_call_center_sk and ( d_year = 1999 or ( d_year = 1999-1 and d_moy =12) or ( d_year = 1999+1 and d_moy =1) ) group by i_category, i_brand, cc_name , d_year, d_moy), v2 as( select v1.i_category, v1.i_brand, v1.cc_name, v1.d_year, v1.d_moy, v1.avg_monthly_sales, v1.sum_sales, v1_lag.sum_sales psum, v1_lead.sum_sales nsum from v1, v1 v1_lag, v1 v1_lead where v1.i_category = v1_lag.i_category and v1.i_category = v1_lead.i_category and v1.i_brand = v1_lag.i_brand and v1.i_brand = v1_lead.i_brand and v1. cc_name = v1_lag. cc_name and v1. cc_name = v1_lead. cc_name and v1.rn = v1_lag.rn + 1 and v1.rn = v1_lead.rn - 1) select * from v2 where d_year = 1999 and avg_monthly_sales > 0 and case when avg_monthly_sales > 0 then abs(sum_sales - avg_monthly_sales) / avg_monthly_sales else null end > 0.1 order by sum_sales - avg_monthly_sales, cc_name fetch first 100 rows only;
select s_store_name, i_item_desc, sc.revenue from tpcds.store, tpcds.item, (select ss_store_sk, avg(revenue) as ave from (select ss_store_sk, ss_item_sk, sum(ss_sales_price) as revenue from tpcds.store_sales, tpcds.date_dim where ss_sold_date_sk = d_date_sk and d_year = 1999 group by ss_store_sk, ss_item_sk) sa group by ss_store_sk) sb, (select ss_store_sk, ss_item_sk, sum(ss_sales_price) as revenue from tpcds.store_sales, tpcds.date_dim where ss_sold_date_sk = d_date_sk and d_year = 1999 group by ss_store_sk, ss_item_sk) sc where sb.ss_store_sk = sc.ss_store_sk and sc.revenue <= 0.1 * sb.ave and s_store_sk = sc.ss_store_sk and i_item_sk = sc.ss_item_sk order by s_store_name, i_item_desc fetch first 100 rows only;
select i_brand_id brand_id, i_brand brand,t_hour,t_minute, sum(ext_price) ext_price from tpcds.item, (select ws_ext_sales_price as ext_price, ws_sold_date_sk as sold_date_sk, ws_item_sk as sold_item_sk, ws_sold_time_sk as time_sk from tpcds.web_sales,tpcds.date_dim where d_date_sk = ws_sold_date_sk and d_moy=12 and d_year=2002 union all select cs_ext_sales_price as ext_price, cs_sold_date_sk as sold_date_sk, cs_item_sk as sold_item_sk, cs_sold_time_sk as time_sk from tpcds.catalog_sales,tpcds.date_dim where d_date_sk = cs_sold_date_sk and d_moy=12 and d_year=2002 union all select ss_ext_sales_price as ext_price, ss_sold_date_sk as sold_date_sk, ss_item_sk as sold_item_sk, ss_sold_time_sk as time_sk from tpcds.store_sales,tpcds.date_dim where d_date_sk = ss_sold_date_sk and d_moy=12 and d_year=2002 ) as tmp,tpcds.time_dim where sold_item_sk = i_item_sk and i_manager_id=1 and time_sk = t_time_sk and (t_meal_time = 'breakfast' or t_meal_time = 'dinner') group by i_brand, i_brand_id,t_hour,t_minute order by ext_price desc, i_brand_id ;
select c_last_name, c_first_name, c_salutation, c_preferred_cust_flag, ss_ticket_number, cnt from (select ss_ticket_number, ss_customer_sk, count(*) cnt from tpcds.store_sales,tpcds.date_dim,tpcds.store,tpcds.household_demographics where tpcds.store_sales.ss_sold_date_sk = tpcds.date_dim.d_date_sk and tpcds.store_sales.ss_store_sk = tpcds.store.s_store_sk and tpcds.store_sales.ss_hdemo_sk = tpcds.household_demographics.hd_demo_sk and (tpcds.date_dim.d_dom between 1 and 3 or tpcds.date_dim.d_dom between 25 and 28) and (tpcds.household_demographics.hd_buy_potential = '501-1000' or tpcds.household_demographics.hd_buy_potential = '5001-10000') and tpcds.household_demographics.hd_vehicle_count > 0 and (case when tpcds.household_demographics.hd_vehicle_count > 0 then tpcds.household_demographics.hd_dep_count/ tpcds.household_demographics.hd_vehicle_count else null end) > 1.2 and tpcds.date_dim.d_year in (1998,1998+1,1998+2) and tpcds.store.s_county in ('Ziebach County','Williamson County','Walker County','Ziebach County', 'Ziebach County','Ziebach County','Williamson County','Ziebach County') group by ss_ticket_number,ss_customer_sk) dn,tpcds.customer where ss_customer_sk = c_customer_sk and cnt between 15 and 20 order by c_last_name,c_first_name,c_salutation,c_preferred_cust_flag desc;
select sum (ss_quantity) from tpcds.store_sales, tpcds.store, tpcds.customer_demographics, tpcds.customer_address, tpcds.date_dim where s_store_sk = ss_store_sk and ss_sold_date_sk = d_date_sk and d_year = 1998 and ( ( cd_demo_sk = ss_cdemo_sk and cd_marital_status = 'M' and cd_education_status = 'Primary' and ss_sales_price between 100.00 and 150.00 ) or ( cd_demo_sk = ss_cdemo_sk and cd_marital_status = 'M' and cd_education_status = 'Primary' and ss_sales_price between 50.00 and 100.00 ) or ( cd_demo_sk = ss_cdemo_sk and cd_marital_status = 'M' and cd_education_status = 'Primary' and ss_sales_price between 150.00 and 200.00 ) ) and ( ( ss_addr_sk = ca_address_sk and ca_country = 'United States' and ca_state in ('TX', 'GA', 'MI') and ss_net_profit between 0 and 2000 ) or (ss_addr_sk = ca_address_sk and ca_country = 'United States' and ca_state in ('ND', 'OR', 'OH') and ss_net_profit between 150 and 3000 ) or (ss_addr_sk = ca_address_sk and ca_country = 'United States' and ca_state in ('WI', 'IA', 'MO') and ss_net_profit between 50 and 25000 ) ) ;
with customer_total_return as (select wr_returning_customer_sk as ctr_customer_sk, ca_state as ctr_state, sum(wr_return_amt) as ctr_total_return from tpcds.web_returns, tpcds.date_dim, tpcds.customer_address where wr_returned_date_sk = d_date_sk and d_year =1999 and wr_returning_addr_sk = ca_address_sk group by wr_returning_customer_sk, ca_state) select c_customer_id,c_salutation,c_first_name,c_last_name,c_preferred_cust_flag, c_birth_day,c_birth_month,c_birth_year,c_birth_country,c_login,c_email_address, c_last_review_date,ctr_total_return from customer_total_return ctr1, tpcds.customer_address, tpcds.customer where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2 from customer_total_return ctr2 where ctr1.ctr_state = ctr2.ctr_state) and ca_address_sk = c_current_addr_sk and ca_state = 'CO' and ctr1.ctr_customer_sk = c_customer_sk order by c_customer_id,c_salutation,c_first_name,c_last_name,c_preferred_cust_flag, c_birth_day,c_birth_month,c_birth_year,c_birth_country,c_login,c_email_address, c_last_review_date,ctr_total_return fetch first 100 rows only;
select count(*) from ((select distinct c_last_name, c_first_name, d_date from tpcds.store_sales, tpcds.date_dim, tpcds.customer where tpcds.store_sales.ss_sold_date_sk = tpcds.date_dim.d_date_sk and tpcds.store_sales.ss_customer_sk = tpcds.customer.c_customer_sk and d_year = 1998) except (select distinct c_last_name, c_first_name, d_date from tpcds.catalog_sales, tpcds.date_dim, tpcds.customer where tpcds.catalog_sales.cs_sold_date_sk = tpcds.date_dim.d_date_sk and tpcds.catalog_sales.cs_bill_customer_sk = tpcds.customer.c_customer_sk and d_year = 1998) except (select distinct c_last_name, c_first_name, d_date from tpcds.web_sales, tpcds.date_dim, tpcds.customer where tpcds.web_sales.ws_sold_date_sk = tpcds.date_dim.d_date_sk and tpcds.web_sales.ws_bill_customer_sk = tpcds.customer.c_customer_sk and d_year = 1998) ) cool_cust ;
select c_last_name, c_first_name, c_salutation, c_preferred_cust_flag, ss_ticket_number, cnt from (select ss_ticket_number, ss_customer_sk, count(*) cnt from tpcds.store_sales,tpcds.date_dim,tpcds.store,tpcds.household_demographics where tpcds.store_sales.ss_sold_date_sk = tpcds.date_dim.d_date_sk and tpcds.store_sales.ss_store_sk = tpcds.store.s_store_sk and tpcds.store_sales.ss_hdemo_sk = tpcds.household_demographics.hd_demo_sk and tpcds.date_dim.d_dom between 1 and 2 and (tpcds.household_demographics.hd_buy_potential = '1001-5000' or tpcds.household_demographics.hd_buy_potential = '0-500') and tpcds.household_demographics.hd_vehicle_count > 0 and case when tpcds.household_demographics.hd_vehicle_count > 0 then tpcds.household_demographics.hd_dep_count/ tpcds.household_demographics.hd_vehicle_count else null end > 1 and tpcds.date_dim.d_year in (1999,1999+1,1999+2) and tpcds.store.s_county in ('Williamson County','Ziebach County','Walker County','Ziebach County') group by ss_ticket_number,ss_customer_sk) dj,tpcds.customer where ss_customer_sk = c_customer_sk and cnt between 1 and 5 order by cnt desc;
select c_customer_id as customer_id, c_last_name || ', ' || c_first_name as customername from tpcds.customer, tpcds.customer_address, tpcds.customer_demographics, tpcds.household_demographics, tpcds.income_band, tpcds.store_returns where ca_city = 'Salem' and c_current_addr_sk = ca_address_sk and ib_lower_bound >= 38258 and ib_upper_bound <= 38258 + 50000 and ib_income_band_sk = hd_income_band_sk and cd_demo_sk = c_current_cdemo_sk and hd_demo_sk = c_current_hdemo_sk and sr_cdemo_sk = cd_demo_sk order by c_customer_id fetch first 100 rows only;
select i_brand_id brand_id, i_brand brand, sum(ss_ext_sales_price) ext_price from tpcds.date_dim, tpcds.store_sales, tpcds.item where d_date_sk = ss_sold_date_sk and ss_item_sk = i_item_sk and i_manager_id=53 and d_moy=12 and d_year=2001 group by i_brand, i_brand_id order by ext_price desc, i_brand_id fetch first 100 rows only ;
with ss as ( select i_item_id,sum(ss_ext_sales_price) total_sales from tpcds.store_sales, tpcds.date_dim, tpcds.customer_address, tpcds.item where i_item_id in (select i_item_id from tpcds.item where i_color in ('mint','orange','olive')) and ss_item_sk = i_item_sk and ss_sold_date_sk = d_date_sk and d_year = 1999 and d_moy = 7 and ss_addr_sk = ca_address_sk and ca_gmt_offset = -6 group by i_item_id), cs as ( select i_item_id,sum(cs_ext_sales_price) total_sales from tpcds.catalog_sales, tpcds.date_dim, tpcds.customer_address, tpcds.item where i_item_id in (select i_item_id from tpcds.item where i_color in ('mint','orange','olive')) and cs_item_sk = i_item_sk and cs_sold_date_sk = d_date_sk and d_year = 1999 and d_moy = 7 and cs_bill_addr_sk = ca_address_sk and ca_gmt_offset = -6 group by i_item_id), ws as ( select i_item_id,sum(ws_ext_sales_price) total_sales from tpcds.web_sales, tpcds.date_dim, tpcds.customer_address, tpcds.item where i_item_id in (select i_item_id from tpcds.item where i_color in ('mint','orange','olive')) and ws_item_sk = i_item_sk and ws_sold_date_sk = d_date_sk and d_year = 1999 and d_moy = 7 and ws_bill_addr_sk = ca_address_sk and ca_gmt_offset = -6 group by i_item_id) select i_item_id ,sum(total_sales) total_sales from (select * from ss union all select * from cs union all select * from ws) tmp1 group by i_item_id order by total_sales fetch first 100 rows only;
with wscs as (select sold_date_sk, sales_price from (select ws_sold_date_sk sold_date_sk, ws_ext_sales_price sales_price from tpcds.web_sales) x union all (select cs_sold_date_sk sold_date_sk, cs_ext_sales_price sales_price from tpcds.catalog_sales)), wswscs as (select d_week_seq, sum(case when (d_day_name='Sunday') then sales_price else null end) sun_sales, sum(case when (d_day_name='Monday') then sales_price else null end) mon_sales, sum(case when (d_day_name='Tuesday') then sales_price else null end) tue_sales, sum(case when (d_day_name='Wednesday') then sales_price else null end) wed_sales, sum(case when (d_day_name='Thursday') then sales_price else null end) thu_sales, sum(case when (d_day_name='Friday') then sales_price else null end) fri_sales, sum(case when (d_day_name='Saturday') then sales_price else null end) sat_sales from wscs, tpcds.date_dim where d_date_sk = sold_date_sk group by d_week_seq) select d_week_seq1, round(sun_sales1/sun_sales2,2), round(mon_sales1/mon_sales2,2), round(tue_sales1/tue_sales2,2), round(wed_sales1/wed_sales2,2), round(thu_sales1/thu_sales2,2), round(fri_sales1/fri_sales2,2), round(sat_sales1/sat_sales2,2) from (select wswscs.d_week_seq d_week_seq1, sun_sales sun_sales1, mon_sales mon_sales1, tue_sales tue_sales1, wed_sales wed_sales1, thu_sales thu_sales1, fri_sales fri_sales1, sat_sales sat_sales1 from wswscs,tpcds.date_dim where tpcds.date_dim.d_week_seq = wswscs.d_week_seq and d_year = 2001) y, (select wswscs.d_week_seq d_week_seq2, sun_sales sun_sales2, mon_sales mon_sales2, tue_sales tue_sales2, wed_sales wed_sales2, thu_sales thu_sales2, fri_sales fri_sales2, sat_sales sat_sales2 from wswscs, tpcds.date_dim where tpcds.date_dim.d_week_seq = wswscs.d_week_seq and d_year = 2001+1) z where d_week_seq1=d_week_seq2-53 order by d_week_seq1;
select i_item_id, avg(cs_quantity) agg1, avg(cs_list_price) agg2, avg(cs_coupon_amt) agg3, avg(cs_sales_price) agg4 from tpcds.catalog_sales, tpcds.customer_demographics, tpcds.date_dim, tpcds.item, tpcds.promotion where cs_sold_date_sk = d_date_sk and cs_item_sk = i_item_sk and cs_bill_cdemo_sk = cd_demo_sk and cs_promo_sk = p_promo_sk and cd_gender = 'F' and cd_marital_status = 'D' and cd_education_status = 'Primary' and (p_channel_email = 'N' or p_channel_event = 'N') and d_year = 1998 group by i_item_id order by i_item_id fetch first 100 rows only;
select w_state, i_item_id, sum(case when (cast(d_date as date) < cast ('2001-05-21' as date)) then cs_sales_price - coalesce(cr_refunded_cash,0) else 0 end) as sales_before, sum(case when (cast(d_date as date) >= cast ('2001-05-21' as date)) then cs_sales_price - coalesce(cr_refunded_cash,0) else 0 end) as sales_after from tpcds.catalog_sales left outer join tpcds.catalog_returns on (cs_order_number = cr_order_number and cs_item_sk = cr_item_sk), tpcds.warehouse, tpcds.item, tpcds.date_dim where i_current_price between 0.99 and 1.49 and i_item_sk = cs_item_sk and cs_warehouse_sk = w_warehouse_sk and cs_sold_date_sk = d_date_sk and d_date between (cast ('2001-05-21' as date) - 30 days) and (cast ('2001-05-21' as date) + 30 days) group by w_state,i_item_id order by w_state,i_item_id fetch first 100 rows only;
select * from (select i_manufact_id, sum(ss_sales_price) sum_sales, avg(sum(ss_sales_price)) over (partition by i_manufact_id) avg_quarterly_sales from tpcds.item, tpcds.store_sales, tpcds.date_dim, tpcds.store where ss_item_sk = i_item_sk and ss_sold_date_sk = d_date_sk and ss_store_sk = s_store_sk and d_year in (1999) and ((i_category in ('Books','Children','Electronics') and i_class in ('personal','portable','reference','self-help') and i_brand in ('scholaramalgamalg #14','scholaramalgamalg #7', 'exportiunivamalg #9','scholaramalgamalg #9')) or(i_category in ('Women','Music','Men') and i_class in ('accessories','classical','fragrances','pants') and i_brand in ('amalgimporto #1','edu packscholar #1','exportiimporto #1', 'importoamalg #1'))) group by i_manufact_id, d_qoy ) tmp1 where case when avg_quarterly_sales > 0 then abs (sum_sales - avg_quarterly_sales)/ avg_quarterly_sales else null end > 0.1 order by avg_quarterly_sales, sum_sales, i_manufact_id fetch first 100 rows only;
select c_last_name,c_first_name,substr(s_city,1,30),ss_ticket_number,amt,profit from (select ss_ticket_number, ss_customer_sk, tpcds.store.s_city, sum(ss_coupon_amt) amt, sum(ss_net_profit) profit from tpcds.store_sales,tpcds.date_dim,tpcds.store,tpcds.household_demographics where tpcds.store_sales.ss_sold_date_sk = tpcds.date_dim.d_date_sk and tpcds.store_sales.ss_store_sk = tpcds.store.s_store_sk and tpcds.store_sales.ss_hdemo_sk = tpcds.household_demographics.hd_demo_sk and (tpcds.household_demographics.hd_dep_count = 1 or tpcds.household_demographics.hd_vehicle_count > 0) and tpcds.date_dim.d_dow = 1 and tpcds.date_dim.d_year in (1999,1999+1,1999+2) and tpcds.store.s_number_employees between 200 and 295 group by ss_ticket_number,ss_customer_sk,ss_addr_sk,tpcds.store.s_city) ms,tpcds.customer where ss_customer_sk = c_customer_sk order by c_last_name,c_first_name,substr(s_city,1,30), profit fetch first 100 rows only;
select i_item_id, ca_country, ca_state, ca_county, avg( cast(cs_quantity as numeric(12,2))) agg1, avg( cast(cs_list_price as numeric(12,2))) agg2, avg( cast(cs_coupon_amt as numeric(12,2))) agg3, avg( cast(cs_sales_price as numeric(12,2))) agg4, avg( cast(cs_net_profit as numeric(12,2))) agg5, avg( cast(c_birth_year as numeric(12,2))) agg6, avg( cast(cd1.cd_dep_count as numeric(12,2))) agg7 from tpcds.catalog_sales, tpcds.customer_demographics cd1, tpcds.customer_demographics cd2, tpcds.customer, tpcds.customer_address, tpcds.date_dim, tpcds.item where cs_sold_date_sk = d_date_sk and cs_item_sk = i_item_sk and cs_bill_cdemo_sk = cd1.cd_demo_sk and cs_bill_customer_sk = c_customer_sk and cd1.cd_gender = 'M' and cd1.cd_education_status = 'Advanced Degree' and c_current_cdemo_sk = cd2.cd_demo_sk and c_current_addr_sk = ca_address_sk and c_birth_month in (10,4,3,5,9,7) and d_year = 1998 and ca_state in ('VA','GA','VA', 'WV','VA','KS','AZ') group by rollup (i_item_id, ca_country, ca_state, ca_county) order by ca_country, ca_state, ca_county, i_item_id fetch first 100 rows only;
select substr(w_warehouse_name,1,20), sm_type, cc_name, sum(case when (cs_ship_date_sk - cs_sold_date_sk <= 30 ) then 1 else 0 end) as "30 days", sum(case when (cs_ship_date_sk - cs_sold_date_sk > 30) and (cs_ship_date_sk - cs_sold_date_sk <= 60) then 1 else 0 end ) as "31-60 days", sum(case when (cs_ship_date_sk - cs_sold_date_sk > 60) and (cs_ship_date_sk - cs_sold_date_sk <= 90) then 1 else 0 end) as "61-90 days", sum(case when (cs_ship_date_sk - cs_sold_date_sk > 90) and (cs_ship_date_sk - cs_sold_date_sk <= 120) then 1 else 0 end) as "91-120 days", sum(case when (cs_ship_date_sk - cs_sold_date_sk > 120) then 1 else 0 end) as ">120 days" from tpcds.catalog_sales, tpcds.warehouse, tpcds.ship_mode, tpcds.call_center, tpcds.date_dim where extract (year from d_date) = 1998 and cs_ship_date_sk = d_date_sk and cs_warehouse_sk = w_warehouse_sk and cs_ship_mode_sk = sm_ship_mode_sk and cs_call_center_sk = cc_call_center_sk group by substr(w_warehouse_name,1,20), sm_type, cc_name order by substr(w_warehouse_name,1,20), sm_type, cc_name fetch first 100 rows only;
--select c_last_name, c_first_name, ca_city, bought_city, ss_ticket_number, extended_price, extended_tax, list_price from (select ss_ticket_number, ss_customer_sk, ca_city bought_city, sum(ss_ext_sales_price) extended_price, sum(ss_ext_list_price) list_price, sum(ss_ext_tax) extended_tax from tpcds.store_sales, tpcds.date_dim, tpcds.store, tpcds.household_demographics, tpcds.customer_address where tpcds.store_sales.ss_sold_date_sk = tpcds.date_dim.d_date_sk and tpcds.store_sales.ss_store_sk = tpcds.store.s_store_sk and tpcds.store_sales.ss_hdemo_sk = tpcds.household_demographics.hd_demo_sk and tpcds.store_sales.ss_addr_sk = tpcds.customer_address.ca_address_sk and tpcds.date_dim.d_dom between 1 and 2 and (tpcds.household_demographics.hd_dep_count = 7 or tpcds.household_demographics.hd_vehicle_count= 2) and tpcds.date_dim.d_year in (1999,1999+1,1999+2) and tpcds.store.s_city in ('Riverside','Midway') group by ss_ticket_number, ss_customer_sk, ss_addr_sk,ca_city) dn, tpcds.customer, tpcds.customer_address current_addr where ss_customer_sk = c_customer_sk and tpcds.customer.c_current_addr_sk = current_addr.ca_address_sk and current_addr.ca_city <> bought_city order by c_last_name, ss_ticket_number fetch first 100 rows only;
--select promotions,total,cast(promotions as decimal(15,4))/cast(total as decimal(15,4))*100 from (select sum(ss_ext_sales_price) promotions from tpcds.store_sales, tpcds.store, tpcds.promotion, tpcds.date_dim, tpcds.customer, tpcds.customer_address, tpcds.item where ss_sold_date_sk = d_date_sk and ss_store_sk = s_store_sk and ss_promo_sk = p_promo_sk and ss_customer_sk= c_customer_sk and ca_address_sk = c_current_addr_sk and ss_item_sk = i_item_sk and ca_gmt_offset = -6 and i_category = 'Books' and (p_channel_dmail = 'Y' or p_channel_email = 'Y' or p_channel_tv = 'Y') and s_gmt_offset = -6 and d_year = 1999 and d_moy = 12) promotional_sales, (select sum(ss_ext_sales_price) total from tpcds.store_sales, tpcds.store, tpcds.date_dim, tpcds.customer, tpcds.customer_address, tpcds.item where ss_sold_date_sk = d_date_sk and ss_store_sk = s_store_sk and ss_customer_sk= c_customer_sk and ca_address_sk = c_current_addr_sk and ss_item_sk = i_item_sk and ca_gmt_offset = -6 and i_category = 'Books' and s_gmt_offset = -6 and d_year = 1999 and d_moy = 12) all_sales order by promotions, total fetch first 100 rows only;
--select channel, col_name, d_year, d_qoy, i_category, COUNT(*) sales_cnt, SUM(ext_sales_price) sales_amt FROM ( SELECT 'store' as channel, 'ss_addr_sk' col_name, d_year, d_qoy, i_category, ss_ext_sales_price ext_sales_price FROM tpcds.store_sales, tpcds.item, tpcds.date_dim WHERE ss_addr_sk IS NULL AND ss_sold_date_sk=d_date_sk AND ss_item_sk=i_item_sk UNION ALL SELECT 'web' as channel, 'ws_web_site_sk' col_name, d_year, d_qoy, i_category, ws_ext_sales_price ext_sales_price FROM tpcds.web_sales, tpcds.item, tpcds.date_dim WHERE ws_web_site_sk IS NULL AND ws_sold_date_sk=d_date_sk AND ws_item_sk=i_item_sk UNION ALL SELECT 'catalog' as channel, 'cs_ship_customer_sk' col_name, d_year, d_qoy, i_category, cs_ext_sales_price ext_sales_price FROM tpcds.catalog_sales, tpcds.item, tpcds.date_dim WHERE cs_ship_customer_sk IS NULL AND cs_sold_date_sk=d_date_sk AND cs_item_sk=i_item_sk) foo GROUP BY channel, col_name, d_year, d_qoy, i_category ORDER BY channel, col_name, d_year, d_qoy, i_category fetch first 100 rows only;

