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


