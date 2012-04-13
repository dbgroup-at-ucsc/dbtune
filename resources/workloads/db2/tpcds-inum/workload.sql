-- @ID=TEMPLATE_96
SELECT COUNT(*)
  FROM tpcds.store_sales,
       tpcds.household_demographics,
       tpcds.time_dim,
       tpcds.store
 WHERE ss_sold_time_sk = t_time_sk
       AND ss_hdemo_sk = hd_demo_sk
       AND ss_store_sk = s_store_sk
       AND t_hour = 8
       AND t_minute >= 30
       AND hd_dep_count = 2
       AND s_store_name = 'ese'
 ORDER BY COUNT(*);

-- @ID=TEMPLATE_07
SELECT i_item_id,
       AVG(ss_quantity)    agg1,
       AVG(ss_list_price)  agg2,
       AVG(ss_coupon_amt)  agg3,
       AVG(ss_sales_price) agg4
  FROM tpcds.store_sales,
       tpcds.customer_demographics,
       tpcds.date_dim,
       tpcds.item,
       tpcds.promotion
 WHERE ss_sold_date_sk = d_date_sk
       AND ss_item_sk = i_item_sk
       AND ss_cdemo_sk = cd_demo_sk
       AND ss_promo_sk = p_promo_sk
       AND cd_gender = 'F'
       AND cd_marital_status = 'M'
       AND cd_education_status = 'College'
       AND ( p_channel_email = 'N'
              OR p_channel_event = 'N' )
       AND d_year = 2001
 GROUP BY i_item_id
 ORDER BY i_item_id;

-- @ID=TEMPLATE_19
SELECT i_brand_id              brand_id,
       i_brand                 brand,
       i_manufact_id,
       i_manufact,
       SUM(ss_ext_sales_price) ext_price
  FROM tpcds.date_dim,
       tpcds.store_sales,
       tpcds.item,
       tpcds.customer,
       tpcds.customer_address,
       tpcds.store
 WHERE d_date_sk = ss_sold_date_sk
       AND ss_item_sk = i_item_sk
       AND i_manager_id = 20
       AND d_moy = 11
       AND d_year = 2000
       AND ss_customer_sk = c_customer_sk
       AND c_current_addr_sk = ca_address_sk
       AND SUBSTR(ca_zip, 1, 5) <> SUBSTR(s_zip, 1, 5)
       AND ss_store_sk = s_store_sk
 GROUP BY i_brand,
          i_brand_id,
          i_manufact_id,
          i_manufact
 ORDER BY ext_price DESC,
          i_brand,
          i_brand_id,
          i_manufact_id,
          i_manufact;

-- @ID=TEMPLATE_43
SELECT s_store_name,
       s_store_id,
       SUM(CASE
             WHEN ( d_day_name = 'Sunday' ) THEN ss_sales_price
             ELSE NULL
           END) sun_sales,
       SUM(CASE
             WHEN ( d_day_name = 'Monday' ) THEN ss_sales_price
             ELSE NULL
           END) mon_sales,
       SUM(CASE
             WHEN ( d_day_name = 'Tuesday' ) THEN ss_sales_price
             ELSE NULL
           END) tue_sales,
       SUM(CASE
             WHEN ( d_day_name = 'Wednesday' ) THEN ss_sales_price
             ELSE NULL
           END) wed_sales,
       SUM(CASE
             WHEN ( d_day_name = 'Thursday' ) THEN ss_sales_price
             ELSE NULL
           END) thu_sales,
       SUM(CASE
             WHEN ( d_day_name = 'Friday' ) THEN ss_sales_price
             ELSE NULL
           END) fri_sales,
       SUM(CASE
             WHEN ( d_day_name = 'Saturday' ) THEN ss_sales_price
             ELSE NULL
           END) sat_sales
  FROM tpcds.date_dim,
       tpcds.store_sales,
       tpcds.store
 WHERE d_date_sk = ss_sold_date_sk
       AND s_store_sk = ss_store_sk
       AND s_gmt_offset = -6
       AND d_year = 1999
 GROUP BY s_store_name,
          s_store_id
 ORDER BY s_store_name,
          s_store_id,
          sun_sales,
          mon_sales,
          tue_sales,
          wed_sales,
          thu_sales,
          fri_sales,
          sat_sales;

-- @ID=TEMPLATE_62
SELECT SUBSTR(w_warehouse_name, 1, 20),
       sm_type,
       web_name,
       SUM(CASE
             WHEN ( ws_ship_date_sk - ws_sold_date_sk <= 30 ) THEN 1
             ELSE 0
           END) AS "30 days",
       SUM(CASE
             WHEN ( ws_ship_date_sk - ws_sold_date_sk > 30 )
                  AND ( ws_ship_date_sk - ws_sold_date_sk <= 60 ) THEN 1
             ELSE 0
           END) AS "31-60 days",
       SUM(CASE
             WHEN ( ws_ship_date_sk - ws_sold_date_sk > 60 )
                  AND ( ws_ship_date_sk - ws_sold_date_sk <= 90 ) THEN 1
             ELSE 0
           END) AS "61-90 days",
       SUM(CASE
             WHEN ( ws_ship_date_sk - ws_sold_date_sk > 90 )
                  AND ( ws_ship_date_sk - ws_sold_date_sk <= 120 ) THEN 1
             ELSE 0
           END) AS "91-120 days",
       SUM(CASE
             WHEN ( ws_ship_date_sk - ws_sold_date_sk > 120 ) THEN 1
             ELSE 0
           END) AS ">120 days"
  FROM tpcds.web_sales,
       tpcds.warehouse,
       tpcds.ship_mode,
       tpcds.web_site,
       tpcds.date_dim
 WHERE YEAR(d_date) = 1998
       AND ws_ship_date_sk = d_date_sk
       AND ws_warehouse_sk = w_warehouse_sk
       AND ws_ship_mode_sk = sm_ship_mode_sk
       AND ws_web_site_sk = web_site_sk
 GROUP BY SUBSTR(w_warehouse_name, 1, 20),
          sm_type,
          web_name
 ORDER BY SUBSTR(w_warehouse_name, 1, 20),
          sm_type,
          web_name;

-- @ID=TEMPLATE_98
SELECT i_item_desc,
       i_category,
       i_class,
       i_current_price,
       SUM(ss_ext_sales_price)                                 AS itemrevenue,
       CASE WHEN SUM(ss_ext_sales_price) > 0 THEN SUM(ss_ext_sales_price) * 100 / SUM(ss_ext_sales_price) ELSE 0 END AS revenueratio
  FROM tpcds.store_sales,
       tpcds.item,
       tpcds.date_dim
 WHERE ss_item_sk = i_item_sk
       AND i_category IN ( 'Jewelry', 'Men', 'Children' )
       AND ss_sold_date_sk = d_date_sk
       AND d_date >= '2000-01-28'
       AND d_date <= '2000-02-28'
 GROUP BY i_item_id,
          i_item_desc,
          i_category,
          i_class,
          i_current_price
 ORDER BY i_category,
          i_class,
          i_item_id,
          i_item_desc,
          revenueratio;

-- @ID=TEMPLATE_03
SELECT d_year,
       i_brand_id         brand_id,
       i_brand            brand,
       SUM(ss_ext_sales_price) ext_price
  FROM tpcds.date_dim,
       tpcds.store_sales,
       tpcds.item
 WHERE d_date_sk = ss_sold_date_sk
       AND ss_item_sk = i_item_sk
       AND i_manufact_id = 741
       AND d_moy = 11
 GROUP BY d_year,
          i_brand,
          i_brand_id
 ORDER BY d_year,
          ext_price DESC,
          brand_id;

-- @ID=TEMPLATE_93
SELECT ss_customer_sk,
       SUM(act_sales) sumsales
  FROM (SELECT ss_item_sk,
               ss_ticket_number,
               ss_customer_sk,
               CASE
                 WHEN sr_return_quantity IS NOT NULL THEN ( ss_quantity - sr_return_quantity ) * ss_sales_price
                 ELSE ( ss_quantity * ss_sales_price )
               END act_sales
          FROM tpcds.store_sales
               LEFT OUTER JOIN tpcds.store_returns
                 ON ( sr_item_sk = ss_item_sk
                      AND sr_ticket_number = ss_ticket_number ),
               tpcds.reason
         WHERE sr_reason_sk = r_reason_sk
               AND r_reason_desc = 'reason 67') t
 GROUP BY ss_customer_sk
 ORDER BY sumsales,
          ss_customer_sk;

-- @ID=TEMPLATE_15
SELECT ca_zip,
       SUM(cs_sales_price)
  FROM tpcds.catalog_sales,
       tpcds.customer,
       tpcds.customer_address,
       tpcds.date_dim
 WHERE cs_bill_customer_sk = c_customer_sk
       AND c_current_addr_sk = ca_address_sk
       AND ( SUBSTR(ca_zip, 1, 5) IN ( '85669', '86197', '88274', '83405',
                                       '86475', '85392', '85460', '80348', '81792' )
              OR ca_state IN ( 'CA', 'WA', 'GA' )
              OR cs_sales_price > 500 )
       AND cs_sold_date_sk = d_date_sk
       AND d_qoy = 1
       AND d_year = 1998
 GROUP BY ca_zip
 ORDER BY ca_zip;

-- @ID=TEMPLATE_52
SELECT d_year,
       i_brand_id         brand_id,
       i_brand            brand,
       SUM(ss_ext_sales_price) ext_price
  FROM tpcds.date_dim,
       tpcds.store_sales,
       tpcds.item
 WHERE d_date_sk = ss_sold_date_sk
       AND ss_item_sk = i_item_sk
       AND i_manager_id = 1
       AND d_moy = 11
       AND d_year = 1998
 GROUP BY d_year,
          i_brand,
          i_brand_id
 ORDER BY d_year,
          ext_price DESC,
          brand_id;

-- @ID=TEMPLATE_42
SELECT d_year,
       i_category_id,
       i_category,
       SUM(ss_ext_sales_price)
  FROM tpcds.date_dim,
       tpcds.store_sales,
       tpcds.item
 WHERE d_date_sk = ss_sold_date_sk
       AND ss_item_sk = i_item_sk
       AND i_manager_id = 1
       AND d_moy = 12
       AND d_year = 1998
 GROUP BY d_year,
          i_category_id,
          i_category
 ORDER BY SUM(ss_ext_sales_price) DESC,
          d_year,
          i_category_id,
          i_category;

-- @ID=TEMPLATE_12
SELECT i_item_desc,
       i_category,
       i_class,
       i_current_price,
       SUM(ws_ext_sales_price)                                      AS itemrevenue,
       CASE WHEN SUM(ws_ext_sales_price) > 0 THEN SUM(ws_ext_sales_price) * 100 / SUM(ws_ext_sales_price) ELSE 0 END AS revenueratio
  FROM tpcds.web_sales,
       tpcds.item,
       tpcds.date_dim
 WHERE ws_item_sk = i_item_sk
       AND i_category IN ( 'Men', 'Women', 'Jewelry' )
       AND ws_sold_date_sk = d_date_sk
       AND d_date >= '1999-04-06'
       AND d_date <= '1999-05-06'
 GROUP BY i_item_id,
          i_item_desc,
          i_category,
          i_class,
          i_current_price
 ORDER BY i_category,
          i_class,
          i_item_id,
          i_item_desc,
          revenueratio;

-- @ID=TEMPLATE_20
SELECT i_item_desc,
       i_category,
       i_class,
       i_current_price,
       SUM(cs_ext_sales_price)                                      AS itemrevenue,
       CASE WHEN SUM(cs_ext_sales_price) > 0 THEN SUM(cs_ext_sales_price) * 100 / SUM(cs_ext_sales_price) ELSE 0 END AS revenueratio
  FROM tpcds.catalog_sales,
       tpcds.item,
       tpcds.date_dim
 WHERE cs_item_sk = i_item_sk
       AND i_category IN ( 'Books', 'Jewelry', 'Children' )
       AND cs_sold_date_sk = d_date_sk
       AND d_date >= '1998-03-26'
       AND d_date <= '1998-04-26'
 GROUP BY i_item_id,
          i_item_desc,
          i_category,
          i_class,
          i_current_price
 ORDER BY i_category,
          i_class,
          i_item_id,
          i_item_desc,
          revenueratio;

-- @ID=TEMPLATE_82
SELECT i_item_id,
       i_item_desc,
       i_current_price
  FROM tpcds.item,
       tpcds.inventory,
       tpcds.date_dim,
       tpcds.store_sales
 WHERE i_current_price BETWEEN 31 AND 31 + 30
       AND inv_item_sk = i_item_sk
       AND d_date_sk = inv_date_sk
       AND d_date >= '2002-05-18'
       AND d_date <= '2002-07-18'
       AND i_manufact_id IN ( 867, 107, 602, 451 )
       AND inv_quantity_on_hand BETWEEN 100 AND 500
       AND ss_item_sk = i_item_sk
 GROUP BY i_item_id,
          i_item_desc,
          i_current_price
 ORDER BY i_item_id;

-- @ID=TEMPLATE_84
SELECT c_customer_id    AS customer_id,
       c_last_name
        || ', '
        || c_first_name AS customername
  FROM tpcds.customer,
       tpcds.customer_address,
       tpcds.customer_demographics,
       tpcds.household_demographics,
       tpcds.income_band,
       tpcds.store_returns
 WHERE ca_city = 'Salem'
       AND c_current_addr_sk = ca_address_sk
       AND ib_lower_bound >= 38258
       AND ib_upper_bound <= 38258 + 50000
       AND ib_income_band_sk = hd_income_band_sk
       AND cd_demo_sk = c_current_cdemo_sk
       AND hd_demo_sk = c_current_hdemo_sk
       AND sr_cdemo_sk = cd_demo_sk
 ORDER BY c_customer_id;

-- @ID=TEMPLATE_55
SELECT i_brand_id              brand_id,
       i_brand                 brand,
       SUM(ss_ext_sales_price) ext_price
  FROM tpcds.date_dim,
       tpcds.store_sales,
       tpcds.item
 WHERE d_date_sk = ss_sold_date_sk
       AND ss_item_sk = i_item_sk
       AND i_manager_id = 53
       AND d_moy = 12
       AND d_year = 2001
 GROUP BY i_brand,
          i_brand_id
 ORDER BY ext_price DESC,
          i_brand_id;

-- @ID=TEMPLATE_26
SELECT i_item_id,
       AVG(cs_quantity)    agg1,
       AVG(cs_list_price)  agg2,
       AVG(cs_coupon_amt)  agg3,
       AVG(cs_sales_price) agg4
  FROM tpcds.catalog_sales,
       tpcds.customer_demographics,
       tpcds.date_dim,
       tpcds.item,
       tpcds.promotion
 WHERE cs_sold_date_sk = d_date_sk
       AND cs_item_sk = i_item_sk
       AND cs_bill_cdemo_sk = cd_demo_sk
       AND cs_promo_sk = p_promo_sk
       AND cd_gender = 'F'
       AND cd_marital_status = 'D'
       AND cd_education_status = 'Primary'
       AND ( p_channel_email = 'N'
              OR p_channel_event = 'N' )
       AND d_year = 1998
 GROUP BY i_item_id
 ORDER BY i_item_id;

-- @ID=TEMPLATE_40
SELECT w_state,
       i_item_id,
       SUM(CASE
             WHEN ( CAST(d_date AS DATE) < CAST ('2001-05-21' AS DATE) ) THEN cs_sales_price - COALESCE(cr_refunded_cash
                                                                                               , 0)
             ELSE 0
           END) AS sales_before,
       SUM(CASE
             WHEN ( CAST(d_date AS DATE) >= CAST ('2001-05-21' AS DATE) ) THEN
             cs_sales_price - COALESCE(cr_refunded_cash, 0)
             ELSE 0
           END) AS sales_after
  FROM tpcds.catalog_sales
       LEFT OUTER JOIN tpcds.catalog_returns
         ON ( cs_order_number = cr_order_number
              AND cs_item_sk = cr_item_sk ),
       tpcds.warehouse,
       tpcds.item,
       tpcds.date_dim
 WHERE i_current_price BETWEEN 0.99 AND 1.49
       AND i_item_sk = cs_item_sk
       AND cs_warehouse_sk = w_warehouse_sk
       AND cs_sold_date_sk = d_date_sk
       AND d_date >= '2001-04-21'
       AND d_date <= '2001-06-21'
 GROUP BY w_state,
          i_item_id
 ORDER BY w_state,
          i_item_id;

-- @ID=TEMPLATE_99
SELECT SUBSTR(w_warehouse_name, 1, 20),
       sm_type,
       cc_name,
       SUM(CASE
             WHEN ( cs_ship_date_sk - cs_sold_date_sk <= 30 ) THEN 1
             ELSE 0
           END) AS "30 days",
       SUM(CASE
             WHEN ( cs_ship_date_sk - cs_sold_date_sk > 30 )
                  AND ( cs_ship_date_sk - cs_sold_date_sk <= 60 ) THEN 1
             ELSE 0
           END) AS "31-60 days",
       SUM(CASE
             WHEN ( cs_ship_date_sk - cs_sold_date_sk > 60 )
                  AND ( cs_ship_date_sk - cs_sold_date_sk <= 90 ) THEN 1
             ELSE 0
           END) AS "61-90 days",
       SUM(CASE
             WHEN ( cs_ship_date_sk - cs_sold_date_sk > 90 )
                  AND ( cs_ship_date_sk - cs_sold_date_sk <= 120 ) THEN 1
             ELSE 0
           END) AS "91-120 days",
       SUM(CASE
             WHEN ( cs_ship_date_sk - cs_sold_date_sk > 120 ) THEN 1
             ELSE 0
           END) AS ">120 days"
  FROM tpcds.catalog_sales,
       tpcds.warehouse,
       tpcds.ship_mode,
       tpcds.call_center,
       tpcds.date_dim
 WHERE YEAR(d_date) = 1998
       AND cs_ship_date_sk = d_date_sk
       AND cs_warehouse_sk = w_warehouse_sk
       AND cs_ship_mode_sk = sm_ship_mode_sk
       AND cs_call_center_sk = cc_call_center_sk
 GROUP BY SUBSTR(w_warehouse_name, 1, 20),
          sm_type,
          cc_name
 ORDER BY SUBSTR(w_warehouse_name, 1, 20),
          sm_type,
          cc_name;

-- @ID=TEMPLATE_63
SELECT *
  FROM (SELECT i_manager_id,
	       SUM(ss_sales_price) sum_sales,
	       AVG(ss_sales_price) avg_monthly_sales
	  FROM tpcds.item,
	       tpcds.store_sales,
	       tpcds.date_dim,
	       tpcds.store
	 WHERE ss_item_sk = i_item_sk
	       AND ss_sold_date_sk = d_date_sk
	       AND ss_store_sk = s_store_sk
	       AND d_year IN ( 1999 )
	       AND (  (  i_category IN ('Books', 'Children', 'Electronics')
			 AND i_class IN ('personal', 'portable', 'refernece', 'self-help')
			 AND i_brand IN ('scholaramalgamalg #14', 'scholaramalgamalg #7', 'exportiunivamalg #9', 'scholaramalgamalg #9')
		      )
		      OR ( i_category IN ('Women', 'Music', 'Men')
			   AND i_class IN ('accessories', 'classical', 'fragrances', 'pants')
			   AND i_brand IN ('amalgimporto #1', 'edu packscholar #1', 'exportiimporto #1', 'importoamalg #1')
			 )
		   )
	 GROUP BY i_manager_id,
		  d_moy) tmp1
 WHERE CASE
	 WHEN avg_monthly_sales > 0 THEN ABS (sum_sales - avg_monthly_sales) / avg_monthly_sales
	 ELSE NULL
       END > 0.1
 ORDER BY i_manager_id,
	  avg_monthly_sales,
	  sum_sales;

-- @ID=TEMPLATE_37
SELECT i_item_id,
       i_item_desc,
       i_current_price
  FROM tpcds.item,
       tpcds.inventory,
       tpcds.date_dim,
       tpcds.catalog_sales
 WHERE i_current_price BETWEEN 24 AND 24 + 30
       AND inv_item_sk = i_item_sk
       AND d_date_sk = inv_date_sk
       AND d_date >= '2001-02-08'
       AND d_date <= '2001-04-08'
       AND i_manufact_id IN ( 946, 774, 749, 744 )
       AND inv_quantity_on_hand BETWEEN 100 AND 500
       AND cs_item_sk = i_item_sk
 GROUP BY i_item_id,
	  i_item_desc,
	  i_current_price
 ORDER BY i_item_id;

-- @ID=TEMPLATE_48
SELECT SUM (ss_quantity)
  FROM tpcds.store_sales,
       tpcds.store,
       tpcds.customer_demographics,
       tpcds.customer_address,
       tpcds.date_dim
 WHERE s_store_sk = ss_store_sk
       AND ss_sold_date_sk = d_date_sk
       AND d_year = 1998
       AND ( ( cd_demo_sk = ss_cdemo_sk
	       AND cd_marital_status = 'M'
	       AND cd_education_status = 'Primary'
	       AND ss_sales_price BETWEEN 100.00 AND 150.00 )
	      OR ( cd_demo_sk = ss_cdemo_sk
		   AND cd_marital_status = 'M'
		   AND cd_education_status = 'Primary'
		   AND ss_sales_price BETWEEN 50.00 AND 100.00 )
	      OR ( cd_demo_sk = ss_cdemo_sk
		   AND cd_marital_status = 'M'
		   AND cd_education_status = 'Primary'
		   AND ss_sales_price BETWEEN 150.00 AND 200.00 ) )
       AND ( ( ss_addr_sk = ca_address_sk
	       AND ca_country = 'United States'
	       AND ca_state IN ( 'TX', 'GA', 'MI' )
	       AND ss_net_profit BETWEEN 0 AND 2000 )
	      OR ( ss_addr_sk = ca_address_sk
		   AND ca_country = 'United States'
		   AND ca_state IN ( 'ND', 'OR', 'OH' )
		   AND ss_net_profit BETWEEN 150 AND 3000 )
	      OR ( ss_addr_sk = ca_address_sk
		   AND ca_country = 'United States'
		   AND ca_state IN ( 'WI', 'IA', 'MO' )
		   AND ss_net_profit BETWEEN 50 AND 25000 ) );

-- @ID=TEMPLATE_73
SELECT c_last_name,
       c_first_name,
       c_salutation,
       c_preferred_cust_flag,
       ss_ticket_number,
       cnt
  FROM (SELECT ss_ticket_number,
	       ss_customer_sk,
	       COUNT(*) cnt
	  FROM tpcds.store_sales,
	       tpcds.date_dim,
	       tpcds.store,
	       tpcds.household_demographics
	 WHERE ss_sold_date_sk = d_date_sk
	       AND ss_store_sk = s_store_sk
	       AND ss_hdemo_sk = hd_demo_sk
	       AND d_dom BETWEEN 1 AND 2
	       AND ( hd_buy_potential = '1001-5000'
		      OR hd_buy_potential = '0-500' )
	       AND hd_vehicle_count > 0
	       AND CASE
		     WHEN hd_vehicle_count > 0 THEN hd_dep_count / hd_vehicle_count
		     ELSE NULL
		   END > 1
	       AND d_year IN ( 1999, 1999 + 1, 1999 + 2 )
	       AND s_county IN ( 'Williamson County', 'Ziebach County', 'Walker County', 'Ziebach County' )
	 GROUP BY ss_ticket_number,
		  ss_customer_sk) dj,
       tpcds.customer
 WHERE ss_customer_sk = c_customer_sk
       AND cnt BETWEEN 1 AND 5
 ORDER BY cnt DESC;

-- @ID=TEMPLATE_53
SELECT *
  FROM (SELECT i_manufact_id,
	       SUM(ss_sales_price) sum_sales,
	       AVG(ss_sales_price) avg_quarterly_sales
	  FROM tpcds.item,
	       tpcds.store_sales,
	       tpcds.date_dim,
	       tpcds.store
	 WHERE ss_item_sk = i_item_sk
	       AND ss_sold_date_sk = d_date_sk
	       AND ss_store_sk = s_store_sk
	       AND d_year IN ( 1999 )
	       AND ( ( i_category IN ( 'Books', 'Children', 'Electronics' )
		       AND i_class IN ( 'personal', 'portable', 'reference', 'self-help' )
		       AND i_brand IN ( 'scholaramalgamalg #14', 'scholaramalgamalg #7', 'exportiunivamalg #9',
					'scholaramalgamalg #9'
				      ) )
		      OR ( i_category IN ( 'Women', 'Music', 'Men' )
			   AND i_class IN ( 'accessories', 'classical', 'fragrances', 'pants' )
			   AND i_brand IN ( 'amalgimporto #1', 'edu packscholar #1', 'exportiimporto #1',
					    'importoamalg #1' ) )
		   )
	 GROUP BY i_manufact_id,
		  d_qoy) tmp1
 WHERE CASE
	 WHEN avg_quarterly_sales > 0 THEN ABS (sum_sales - avg_quarterly_sales) / avg_quarterly_sales
	 ELSE NULL
       END > 0.1
 ORDER BY avg_quarterly_sales,
	  sum_sales,
	  i_manufact_id;

-- @ID=TEMPLATE_79
SELECT c_last_name,
       c_first_name,
       SUBSTR(s_city, 1, 30),
       ss_ticket_number,
       amt,
       profit
  FROM (SELECT ss_ticket_number,
	       ss_customer_sk,
	       s_city,
	       SUM(ss_coupon_amt) amt,
	       SUM(ss_net_profit) profit
	  FROM tpcds.store_sales,
	       tpcds.date_dim,
	       tpcds.store,
	       tpcds.household_demographics
	 WHERE ss_sold_date_sk = d_date_sk
	       AND ss_store_sk = s_store_sk
	       AND ss_hdemo_sk = hd_demo_sk
	       AND ( hd_dep_count = 1
		      OR hd_vehicle_count > 0 )
	       AND d_dow = 1
	       AND d_year IN ( 1999, 1999 + 1, 1999 + 2 )
	       AND s_number_employees BETWEEN 200 AND 295
	 GROUP BY ss_ticket_number,
		  ss_customer_sk,
		  ss_addr_sk,
		  s_city) ms,
       tpcds.customer
 WHERE ss_customer_sk = c_customer_sk
 ORDER BY c_last_name,
	  c_first_name,
	  SUBSTR(s_city, 1, 30),
	  profit;
