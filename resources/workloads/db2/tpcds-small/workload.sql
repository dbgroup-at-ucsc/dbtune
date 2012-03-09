SELECT COUNT(*)
FROM   tpcds.store_sales,
       tpcds.household_demographics,
       tpcds.time_dim,
       tpcds.store
WHERE  ss_sold_time_sk = t_time_sk
       AND ss_hdemo_sk = hd_demo_sk
       AND ss_store_sk = s_store_sk
       AND t_hour = 8
       AND t_minute >= 30
       AND hd_dep_count = 2
       AND s_store_name = 'ese'
ORDER  BY COUNT(*);

SELECT i_item_id,
       Avg(ss_quantity)    agg1,
       Avg(ss_list_price)  agg2,
       Avg(ss_coupon_amt)  agg3,
       Avg(ss_sales_price) agg4
FROM   tpcds.store_sales,
       tpcds.customer_demographics,
       tpcds.date_dim,
       tpcds.item,
       tpcds.promotion
WHERE  ss_sold_date_sk = d_date_sk
       AND ss_item_sk = i_item_sk
       AND ss_cdemo_sk = cd_demo_sk
       AND ss_promo_sk = p_promo_sk
       AND cd_gender = 'F'
       AND cd_marital_status = 'M'
       AND cd_education_status = 'College'
       AND ( p_channel_email = 'N'
              OR p_channel_event = 'N' )
       AND d_year = 2001
GROUP  BY i_item_id
ORDER  BY i_item_id;

SELECT i_brand_id              brand_id,
       i_brand                 brand,
       i_manufact_id,
       i_manufact,
       Sum(ss_ext_sales_price) ext_price
FROM   tpcds.date_dim,
       tpcds.store_sales,
       tpcds.item,
       tpcds.customer,
       tpcds.customer_address,
       tpcds.store
WHERE  d_date_sk = ss_sold_date_sk
       AND ss_item_sk = i_item_sk
       AND i_manager_id = 20
       AND d_moy = 11
       AND d_year = 2000
       AND ss_customer_sk = c_customer_sk
       AND c_current_addr_sk = ca_address_sk
       AND Substr(ca_zip, 1, 5) <> Substr(s_zip, 1, 5)
       AND ss_store_sk = s_store_sk
GROUP  BY i_brand,
          i_brand_id,
          i_manufact_id,
          i_manufact
ORDER  BY ext_price DESC,
          i_brand,
          i_brand_id,
          i_manufact_id,
          i_manufact;

SELECT *
FROM  (SELECT w_warehouse_name,
              i_item_id,
              Sum(CASE
                    WHEN ( CAST(d_date AS DATE) < CAST ('2002-05-09' AS DATE) )
                  THEN
                    inv_quantity_on_hand
                    ELSE 0
                  END) AS inv_before,
              Sum(CASE
                    WHEN ( CAST(d_date AS DATE) >= CAST ('2002-05-09' AS DATE) )
                  THEN
                    inv_quantity_on_hand
                    ELSE 0
                  END) AS inv_after
       FROM   tpcds.inventory,
              tpcds.warehouse,
              tpcds.item,
              tpcds.date_dim
       WHERE  i_current_price BETWEEN 0.99 AND 1.49
              AND i_item_sk = inv_item_sk
              AND inv_warehouse_sk = w_warehouse_sk
              AND inv_date_sk = d_date_sk
              AND d_date >= '2002-04-09'
              AND d_date <= '2002-06-09'
       GROUP  BY w_warehouse_name,
                 i_item_id) x
WHERE  ( CASE
           WHEN inv_before > 0 THEN inv_after / inv_before
           ELSE NULL
         END ) BETWEEN 2.0 / 3.0 AND 3.0 / 2.0
ORDER  BY w_warehouse_name,
          i_item_id;

SELECT s_store_name,
       s_store_id,
       Sum(CASE
             WHEN ( d_day_name = 'Sunday' ) THEN ss_sales_price
             ELSE NULL
           END) sun_sales,
       Sum(CASE
             WHEN ( d_day_name = 'Monday' ) THEN ss_sales_price
             ELSE NULL
           END) mon_sales,
       Sum(CASE
             WHEN ( d_day_name = 'Tuesday' ) THEN ss_sales_price
             ELSE NULL
           END) tue_sales,
       Sum(CASE
             WHEN ( d_day_name = 'Wednesday' ) THEN ss_sales_price
             ELSE NULL
           END) wed_sales,
       Sum(CASE
             WHEN ( d_day_name = 'Thursday' ) THEN ss_sales_price
             ELSE NULL
           END) thu_sales,
       Sum(CASE
             WHEN ( d_day_name = 'Friday' ) THEN ss_sales_price
             ELSE NULL
           END) fri_sales,
       Sum(CASE
             WHEN ( d_day_name = 'Saturday' ) THEN ss_sales_price
             ELSE NULL
           END) sat_sales
FROM   tpcds.date_dim,
       tpcds.store_sales,
       tpcds.store
WHERE  d_date_sk = ss_sold_date_sk
       AND s_store_sk = ss_store_sk
       AND s_gmt_offset = -6
       AND d_year = 1999
GROUP  BY s_store_name,
          s_store_id
ORDER  BY s_store_name,
          s_store_id,
          sun_sales,
          mon_sales,
          tue_sales,
          wed_sales,
          thu_sales,
          fri_sales,
          sat_sales;

SELECT i_item_id,
       i_item_desc,
       i_current_price
FROM   tpcds.item,
       tpcds.inventory,
       tpcds.date_dim,
       tpcds.catalog_sales
WHERE  i_current_price BETWEEN 24 AND 24 + 30
       AND inv_item_sk = i_item_sk
       AND d_date_sk = inv_date_sk
       AND d_date >= '2001-02-08'
       AND d_date <= '2001-04-08'
       AND i_manufact_id IN ( 946, 774, 749, 744 )
       AND inv_quantity_on_hand BETWEEN 100 AND 500
       AND cs_item_sk = i_item_sk
GROUP  BY i_item_id,
          i_item_desc,
          i_current_price
ORDER  BY i_item_id;

SELECT ss_customer_sk,
       Sum(act_sales) sumsales
FROM   (SELECT ss_item_sk,
               ss_ticket_number,
               ss_customer_sk,
               CASE
                 WHEN sr_return_quantity IS NOT NULL THEN
                 ( ss_quantity - sr_return_quantity ) * ss_sales_price
                 ELSE ( ss_quantity * ss_sales_price )
               END act_sales
        FROM   tpcds.store_sales
               LEFT OUTER JOIN tpcds.store_returns
                 ON ( sr_item_sk = ss_item_sk
                      AND sr_ticket_number = ss_ticket_number ),
               tpcds.reason
        WHERE  sr_reason_sk = r_reason_sk
               AND r_reason_desc = 'reason 67') t
GROUP  BY ss_customer_sk
ORDER  BY sumsales,
          ss_customer_sk;

SELECT i_item_id,
       i_item_desc,
       i_current_price
FROM   tpcds.item,
       tpcds.inventory,
       tpcds.date_dim,
       tpcds.store_sales
WHERE  i_current_price BETWEEN 31 AND 31 + 30
       AND inv_item_sk = i_item_sk
       AND d_date_sk = inv_date_sk
       AND d_date >= '2002-05-18'
       AND d_date <= '2002-07-18'
       AND i_manufact_id IN ( 867, 107, 602, 451 )
       AND inv_quantity_on_hand BETWEEN 100 AND 500
       AND ss_item_sk = i_item_sk
GROUP  BY i_item_id,
          i_item_desc,
          i_current_price
ORDER  BY i_item_id;

SELECT c_last_name,
       c_first_name,
       c_salutation,
       c_preferred_cust_flag,
       ss_ticket_number,
       cnt
FROM   (SELECT ss_ticket_number,
               ss_customer_sk,
               COUNT(*) cnt
        FROM   tpcds.store_sales,
               tpcds.date_dim,
               tpcds.store,
               tpcds.household_demographics
        WHERE  ss_sold_date_sk = d_date_sk
               AND ss_store_sk = s_store_sk
               AND ss_hdemo_sk = hd_demo_sk
               AND ( d_dom BETWEEN 1 AND 3
                      OR d_dom BETWEEN 25 AND 28 )
               AND ( hd_buy_potential = '501-1000'
                      OR hd_buy_potential = '5001-10000'
                   )
               AND hd_vehicle_count > 0
               AND ( CASE
                       WHEN hd_vehicle_count > 0 THEN
                       hd_dep_count /
                       hd_vehicle_count
                       ELSE NULL
                     END ) > 1.2
               AND d_year IN ( 1998, 1998 + 1, 1998 + 2 )
               AND s_county IN ( 'Ziebach County', 'Williamson County',
                                       'Walker County',
                                       'Ziebach County',
                                       'Ziebach County', 'Ziebach County',
                                       'Williamson County',
                                           'Ziebach County' )
        GROUP  BY ss_ticket_number,
                  ss_customer_sk) dn,
       tpcds.customer
WHERE  ss_customer_sk = c_customer_sk
       AND cnt BETWEEN 15 AND 20
ORDER  BY c_last_name,
          c_first_name,
          c_salutation,
          c_preferred_cust_flag DESC;

SELECT c_last_name,
       c_first_name,
       c_salutation,
       c_preferred_cust_flag,
       ss_ticket_number,
       cnt
FROM   (SELECT ss_ticket_number,
               ss_customer_sk,
               COUNT(*) cnt
        FROM   tpcds.store_sales,
               tpcds.date_dim,
               tpcds.store,
               tpcds.household_demographics
        WHERE  ss_sold_date_sk = d_date_sk
               AND ss_store_sk = s_store_sk
               AND ss_hdemo_sk = hd_demo_sk
               AND d_dom BETWEEN 1 AND 2
               AND ( hd_buy_potential = '1001-5000'
                      OR hd_buy_potential = '0-500' )
               AND hd_vehicle_count > 0
               AND CASE
                     WHEN hd_vehicle_count > 0 THEN
                     hd_dep_count /
                     hd_vehicle_count
                     ELSE NULL
                   END > 1
               AND d_year IN ( 1999, 1999 + 1, 1999 + 2 )
               AND s_county IN ( 'Williamson County', 'Ziebach County',
                                       'Walker County',
                                       'Ziebach County' )
        GROUP  BY ss_ticket_number,
                  ss_customer_sk) dj,
       tpcds.customer
WHERE  ss_customer_sk = c_customer_sk
       AND cnt BETWEEN 1 AND 5
ORDER  BY cnt DESC;

SELECT c_customer_id    AS customer_id,
       c_last_name
        || ', '
        || c_first_name AS customername
FROM   tpcds.customer,
       tpcds.customer_address,
       tpcds.customer_demographics,
       tpcds.household_demographics,
       tpcds.income_band,
       tpcds.store_returns
WHERE  ca_city = 'Salem'
       AND c_current_addr_sk = ca_address_sk
       AND ib_lower_bound >= 38258
       AND ib_upper_bound <= 38258 + 50000
       AND ib_income_band_sk = hd_income_band_sk
       AND cd_demo_sk = c_current_cdemo_sk
       AND hd_demo_sk = c_current_hdemo_sk
       AND sr_cdemo_sk = cd_demo_sk
ORDER  BY c_customer_id;

SELECT i_brand_id              brand_id,
       i_brand                 brand,
       Sum(ss_ext_sales_price) ext_price
FROM   tpcds.date_dim,
       tpcds.store_sales,
       tpcds.item
WHERE  d_date_sk = ss_sold_date_sk
       AND ss_item_sk = i_item_sk
       AND i_manager_id = 53
       AND d_moy = 12
       AND d_year = 2001
GROUP  BY i_brand,
          i_brand_id
ORDER  BY ext_price DESC,
          i_brand_id;

SELECT w_state,
       i_item_id,
       Sum(CASE
             WHEN ( CAST(d_date AS DATE) < CAST ('2001-05-21' AS DATE) ) THEN
             cs_sales_price - Coalesce(cr_refunded_cash, 0)
             ELSE 0
           END) AS sales_before,
       Sum(CASE
             WHEN ( CAST(d_date AS DATE) >= CAST ('2001-05-21' AS DATE) ) THEN
             cs_sales_price - Coalesce(cr_refunded_cash, 0)
             ELSE 0
           END) AS sales_after
FROM   tpcds.catalog_sales
       LEFT OUTER JOIN tpcds.catalog_returns
         ON ( cs_order_number = cr_order_number
              AND cs_item_sk = cr_item_sk ),
       tpcds.warehouse,
       tpcds.item,
       tpcds.date_dim
WHERE  i_current_price BETWEEN 0.99 AND 1.49
       AND i_item_sk = cs_item_sk
       AND cs_warehouse_sk = w_warehouse_sk
       AND cs_sold_date_sk = d_date_sk
       AND d_date >= '2001-04-21'
       AND d_date <= '2001-06-21'
GROUP  BY w_state,
          i_item_id
ORDER  BY w_state,
          i_item_id;
