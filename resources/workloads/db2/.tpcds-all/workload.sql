-- query96.tpl
SELECT COUNT(*)
FROM   store_sales,
       household_demographics,
       time_dim,
       store
WHERE  ss_sold_time_sk = time_dim.t_time_sk
       AND ss_hdemo_sk = household_demographics.hd_demo_sk
       AND ss_store_sk = s_store_sk
       AND time_dim.t_hour = 8
       AND time_dim.t_minute >= 30
       AND household_demographics.hd_dep_count = 2
       AND store.s_store_name = 'ese'
ORDER  BY COUNT(*);

-- query7.tpl
SELECT i_item_id,
       AVG(ss_quantity)    agg1,
       AVG(ss_list_price)  agg2,
       AVG(ss_coupon_amt)  agg3,
       AVG(ss_sales_price) agg4
FROM   store_sales,
       customer_demographics,
       date_dim,
       item,
       promotion
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

-- query75.tpl
WITH all_sales
     AS (SELECT d_year,
                i_brand_id,
                i_class_id,
                i_category_id,
                i_manufact_id,
                SUM(sales_cnt) AS sales_cnt,
                SUM(sales_amt) AS sales_amt
         FROM   (SELECT d_year,
                        i_brand_id,
                        i_class_id,
                        i_category_id,
                        i_manufact_id,
                        cs_quantity - COALESCE(cr_return_quantity, 0)        AS sales_cnt,
                        cs_ext_sales_price - COALESCE(cr_return_amount, 0.0) AS sales_amt
                 FROM   catalog_sales
                        JOIN item
                          ON i_item_sk = cs_item_sk
                        JOIN date_dim
                          ON d_date_sk = cs_sold_date_sk
                        LEFT JOIN catalog_returns
                          ON ( cs_order_number = cr_order_number
                               AND cs_item_sk = cr_item_sk )
                 WHERE  i_category = 'Music'
                 UNION
                 SELECT d_year,
                        i_brand_id,
                        i_class_id,
                        i_category_id,
                        i_manufact_id,
                        ss_quantity - COALESCE(sr_return_quantity, 0)     AS sales_cnt,
                        ss_ext_sales_price - COALESCE(sr_return_amt, 0.0) AS sales_amt
                 FROM   store_sales
                        JOIN item
                          ON i_item_sk = ss_item_sk
                        JOIN date_dim
                          ON d_date_sk = ss_sold_date_sk
                        LEFT JOIN store_returns
                          ON ( ss_ticket_number = sr_ticket_number
                               AND ss_item_sk = sr_item_sk )
                 WHERE  i_category = 'Music'
                 UNION
                 SELECT d_year,
                        i_brand_id,
                        i_class_id,
                        i_category_id,
                        i_manufact_id,
                        ws_quantity - COALESCE(wr_return_quantity, 0)     AS sales_cnt,
                        ws_ext_sales_price - COALESCE(wr_return_amt, 0.0) AS sales_amt
                 FROM   web_sales
                        JOIN item
                          ON i_item_sk = ws_item_sk
                        JOIN date_dim
                          ON d_date_sk = ws_sold_date_sk
                        LEFT JOIN web_returns
                          ON ( ws_order_number = wr_order_number
                               AND ws_item_sk = wr_item_sk )
                 WHERE  i_category = 'Music') sales_detail
         GROUP  BY d_year,
                   i_brand_id,
                   i_class_id,
                   i_category_id,
                   i_manufact_id)
SELECT prev_yr.d_year                        AS prev_year,
       curr_yr.d_year                        AS YEAR,
       curr_yr.i_brand_id,
       curr_yr.i_class_id,
       curr_yr.i_category_id,
       curr_yr.i_manufact_id,
       prev_yr.sales_cnt                     AS prev_yr_cnt,
       curr_yr.sales_cnt                     AS curr_yr_cnt,
       curr_yr.sales_cnt - prev_yr.sales_cnt AS sales_cnt_diff,
       curr_yr.sales_amt - prev_yr.sales_amt AS sales_amt_diff
FROM   all_sales curr_yr,
       all_sales prev_yr
WHERE  curr_yr.i_brand_id = prev_yr.i_brand_id
       AND curr_yr.i_class_id = prev_yr.i_class_id
       AND curr_yr.i_category_id = prev_yr.i_category_id
       AND curr_yr.i_manufact_id = prev_yr.i_manufact_id
       AND curr_yr.d_year = 2000
       AND prev_yr.d_year = 2000 - 1
       AND CAST(curr_yr.sales_cnt AS DECIMAL(17, 2)) / CAST(prev_yr.sales_cnt AS DECIMAL(17, 2)) < 0.9
ORDER  BY sales_cnt_diff;

-- query44.tpl
SELECT asceding.rnk,
       i1.i_product_name best_performing,
       i2.i_product_name worst_performing
FROM  (SELECT *
       FROM   (SELECT item_sk,
                      RANK() OVER (ORDER BY rank_col ASC) rnk
               FROM   (SELECT ss_item_sk         item_sk,
                              AVG(ss_net_profit) rank_col
                       FROM   store_sales ss1
                       WHERE  ss_store_sk = 42
                       GROUP  BY ss_item_sk
                       HAVING AVG(ss_net_profit) > 0.9 * (SELECT AVG(ss_net_profit) rank_col
                                                          FROM   store_sales
                                                          WHERE  ss_store_sk = 42
                                                                 AND ss_promo_sk IS NULL
                                                          GROUP  BY ss_store_sk))v1)v11
       WHERE  rnk < 11) asceding,
      (SELECT *
       FROM   (SELECT item_sk,
                      RANK() OVER (ORDER BY rank_col DESC) rnk
               FROM   (SELECT ss_item_sk         item_sk,
                              AVG(ss_net_profit) rank_col
                       FROM   store_sales ss1
                       WHERE  ss_store_sk = 42
                       GROUP  BY ss_item_sk
                       HAVING AVG(ss_net_profit) > 0.9 * (SELECT AVG(ss_net_profit) rank_col
                                                          FROM   store_sales
                                                          WHERE  ss_store_sk = 42
                                                                 AND ss_promo_sk IS NULL
                                                          GROUP  BY ss_store_sk))v2)v21
       WHERE  rnk < 11) descending,
      item i1,
      item i2
WHERE  asceding.rnk = descending.rnk
       AND i1.i_item_sk = asceding.item_sk
       AND i2.i_item_sk = descending.item_sk
ORDER  BY asceding.rnk;

-- query39.tpl
WITH inv
     AS (SELECT w_warehouse_name,
                w_warehouse_sk,
                i_item_sk,
                d_moy,
                stdev,
                mean,
                CASE mean
                  WHEN 0 THEN NULL
                  ELSE stdev / mean
                END cov
         FROM  (SELECT w_warehouse_name,
                       w_warehouse_sk,
                       i_item_sk,
                       d_moy,
                       STDDEV_SAMP(inv_quantity_on_hand) stdev,
                       AVG(inv_quantity_on_hand)         mean
                FROM   inventory,
                       item,
                       warehouse,
                       date_dim
                WHERE  inv_item_sk = i_item_sk
                       AND inv_warehouse_sk = w_warehouse_sk
                       AND inv_date_sk = d_date_sk
                       AND d_year = 2002
                GROUP  BY w_warehouse_name,
                          w_warehouse_sk,
                          i_item_sk,
                          d_moy) foo
         WHERE  CASE mean
                  WHEN 0 THEN 0
                  ELSE stdev / mean
                END > 1)
SELECT inv1.w_warehouse_sk,
       inv1.i_item_sk,
       inv1.d_moy,
       inv1.mean,
       inv1.cov,
       inv2.w_warehouse_sk,
       inv2.i_item_sk,
       inv2.d_moy,
       inv2.mean,
       inv2.cov
FROM   inv inv1,
       inv inv2
WHERE  inv1.i_item_sk = inv2.i_item_sk
       AND inv1.w_warehouse_sk = inv2.w_warehouse_sk
       AND inv1.d_moy = 3
       AND inv2.d_moy = 3 + 1
ORDER  BY inv1.w_warehouse_sk,
          inv1.i_item_sk,
          inv1.d_moy,
          inv1.mean,
          inv1.cov,
          inv2.d_moy,
          inv2.mean,
          inv2.cov;

WITH inv
     AS (SELECT w_warehouse_name,
                w_warehouse_sk,
                i_item_sk,
                d_moy,
                stdev,
                mean,
                CASE mean
                  WHEN 0 THEN NULL
                  ELSE stdev / mean
                END cov
         FROM  (SELECT w_warehouse_name,
                       w_warehouse_sk,
                       i_item_sk,
                       d_moy,
                       STDDEV_SAMP(inv_quantity_on_hand) stdev,
                       AVG(inv_quantity_on_hand)         mean
                FROM   inventory,
                       item,
                       warehouse,
                       date_dim
                WHERE  inv_item_sk = i_item_sk
                       AND inv_warehouse_sk = w_warehouse_sk
                       AND inv_date_sk = d_date_sk
                       AND d_year = 2002
                GROUP  BY w_warehouse_name,
                          w_warehouse_sk,
                          i_item_sk,
                          d_moy) foo
         WHERE  CASE mean
                  WHEN 0 THEN 0
                  ELSE stdev / mean
                END > 1)
SELECT inv1.w_warehouse_sk,
       inv1.i_item_sk,
       inv1.d_moy,
       inv1.mean,
       inv1.cov,
       inv2.w_warehouse_sk,
       inv2.i_item_sk,
       inv2.d_moy,
       inv2.mean,
       inv2.cov
FROM   inv inv1,
       inv inv2
WHERE  inv1.i_item_sk = inv2.i_item_sk
       AND inv1.w_warehouse_sk = inv2.w_warehouse_sk
       AND inv1.d_moy = 3
       AND inv2.d_moy = 3 + 1
       AND inv1.cov > 1.5
ORDER  BY inv1.w_warehouse_sk,
          inv1.i_item_sk,
          inv1.d_moy,
          inv1.mean,
          inv1.cov,
          inv2.d_moy,
          inv2.mean,
          inv2.cov;

-- query80.tpl
WITH ssr
     AS (SELECT s_store_id                                    AS store_id,
                SUM(ss_ext_sales_price)                       AS sales,
                SUM(COALESCE(sr_return_amt, 0))               AS RETURNS,
                SUM(ss_net_profit - COALESCE(sr_net_loss, 0)) AS profit
         FROM   store_sales
                LEFT OUTER JOIN store_returns
                  ON ( ss_item_sk = sr_item_sk
                       AND ss_ticket_number = sr_ticket_number ),
                date_dim,
                store,
                item,
                promotion
         WHERE  ss_sold_date_sk = d_date_sk
                AND d_date BETWEEN CAST('2000-08-19' AS DATE) AND ( CAST('2000-08-19' AS DATE) + 30 DAYS )
                AND ss_store_sk = s_store_sk
                AND ss_item_sk = i_item_sk
                AND i_current_price > 50
                AND ss_promo_sk = p_promo_sk
                AND p_channel_tv = 'N'
         GROUP  BY s_store_id),
     csr
     AS (SELECT cp_catalog_page_id                            AS catalog_page_id,
                SUM(cs_ext_sales_price)                       AS sales,
                SUM(COALESCE(cr_return_amount, 0))            AS RETURNS,
                SUM(cs_net_profit - COALESCE(cr_net_loss, 0)) AS profit
         FROM   catalog_sales
                LEFT OUTER JOIN catalog_returns
                  ON ( cs_item_sk = cr_item_sk
                       AND cs_order_number = cr_order_number ),
                date_dim,
                catalog_page,
                item,
                promotion
         WHERE  cs_sold_date_sk = d_date_sk
                AND d_date BETWEEN CAST('2000-08-19' AS DATE) AND ( CAST('2000-08-19' AS DATE) + 30 DAYS )
                AND cs_catalog_page_sk = cp_catalog_page_sk
                AND cs_item_sk = i_item_sk
                AND i_current_price > 50
                AND cs_promo_sk = p_promo_sk
                AND p_channel_tv = 'N'
         GROUP  BY cp_catalog_page_id),
     wsr
     AS (SELECT web_site_id,
                SUM(ws_ext_sales_price)                       AS sales,
                SUM(COALESCE(wr_return_amt, 0))               AS RETURNS,
                SUM(ws_net_profit - COALESCE(wr_net_loss, 0)) AS profit
         FROM   web_sales
                LEFT OUTER JOIN web_returns
                  ON ( ws_item_sk = wr_item_sk
                       AND ws_order_number = wr_order_number ),
                date_dim,
                web_site,
                item,
                promotion
         WHERE  ws_sold_date_sk = d_date_sk
                AND d_date BETWEEN CAST('2000-08-19' AS DATE) AND ( CAST('2000-08-19' AS DATE) + 30 DAYS )
                AND ws_web_site_sk = web_site_sk
                AND ws_item_sk = i_item_sk
                AND i_current_price > 50
                AND ws_promo_sk = p_promo_sk
                AND p_channel_tv = 'N'
         GROUP  BY web_site_id)
SELECT channel,
       id,
       SUM(sales)   AS sales,
       SUM(RETURNS) AS RETURNS,
       SUM(profit)  AS profit
FROM   (SELECT 'store channel' AS channel,
               'store'
                || store_id    AS id,
               sales,
               RETURNS,
               profit
        FROM   ssr
        UNION ALL
        SELECT 'catalog channel'   AS channel,
               'catalog_page'
                || catalog_page_id AS id,
               sales,
               RETURNS,
               profit
        FROM   csr
        UNION ALL
        SELECT 'web channel'   AS channel,
               'web_site'
                || web_site_id AS id,
               sales,
               RETURNS,
               profit
        FROM   wsr) x
GROUP  BY ROLLUP ( channel, id )
ORDER  BY channel,
          id;

-- query32.tpl
SELECT SUM(cs_ext_discount_amt) AS "excess discount amount"
FROM   catalog_sales,
       item,
       date_dim
WHERE  i_manufact_id = 645
       AND i_item_sk = cs_item_sk
       AND d_date BETWEEN '2001-03-05' AND ( CAST('2001-03-05' AS DATE) + 90 DAYS )
       AND d_date_sk = cs_sold_date_sk
       AND cs_ext_discount_amt > (SELECT 1.3 * AVG(cs_ext_discount_amt)
                                  FROM   catalog_sales,
                                         date_dim
                                  WHERE  cs_item_sk = i_item_sk
                                         AND d_date BETWEEN '2001-03-05' AND ( CAST('2001-03-05' AS DATE) + 90 DAYS )
                                         AND d_date_sk = cs_sold_date_sk);

-- query19.tpl
SELECT i_brand_id              brand_id,
       i_brand                 brand,
       i_manufact_id,
       i_manufact,
       SUM(ss_ext_sales_price) ext_price
FROM   date_dim,
       store_sales,
       item,
       customer,
       customer_address,
       store
WHERE  d_date_sk = ss_sold_date_sk
       AND ss_item_sk = i_item_sk
       AND i_manager_id = 20
       AND d_moy = 11
       AND d_year = 2000
       AND ss_customer_sk = c_customer_sk
       AND c_current_addr_sk = ca_address_sk
       AND SUBSTR(ca_zip, 1, 5) <> SUBSTR(s_zip, 1, 5)
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

-- query25.tpl
SELECT i_item_id,
       i_item_desc,
       s_store_id,
       s_store_name,
       SUM(ss_net_profit) AS store_sales_profit,
       SUM(sr_net_loss)   AS store_returns_loss,
       SUM(cs_net_profit) AS catalog_sales_profit
FROM   store_sales,
       store_returns,
       catalog_sales,
       date_dim d1,
       date_dim d2,
       date_dim d3,
       store,
       item
WHERE  d1.d_moy = 4
       AND d1.d_year = 2002
       AND d1.d_date_sk = ss_sold_date_sk
       AND i_item_sk = ss_item_sk
       AND s_store_sk = ss_store_sk
       AND ss_customer_sk = sr_customer_sk
       AND ss_item_sk = sr_item_sk
       AND ss_ticket_number = sr_ticket_number
       AND sr_returned_date_sk = d2.d_date_sk
       AND d2.d_moy BETWEEN 4 AND 4 + 6
       AND d2.d_year = 2002
       AND sr_customer_sk = cs_bill_customer_sk
       AND sr_item_sk = cs_item_sk
       AND cs_sold_date_sk = d3.d_date_sk
       AND d3.d_moy BETWEEN 4 AND 4 + 6
       AND d3.d_year = 2002
GROUP  BY i_item_id,
          i_item_desc,
          s_store_id,
          s_store_name
ORDER  BY i_item_id,
          i_item_desc,
          s_store_id,
          s_store_name;

-- query78.tpl
WITH ws
     AS (SELECT d_year                 AS ws_sold_year,
                ws_item_sk,
                ws_bill_customer_sk    ws_customer_sk,
                SUM(ws_quantity)       ws_qty,
                SUM(ws_wholesale_cost) ws_wc,
                SUM(ws_sales_price)    ws_sp
         FROM   web_sales
                LEFT JOIN web_returns
                  ON wr_order_number = ws_order_number
                     AND ws_item_sk = wr_item_sk
                JOIN date_dim
                  ON ws_sold_date_sk = d_date_sk
         WHERE  wr_order_number IS NULL
         GROUP  BY d_year,
                   ws_item_sk,
                   ws_bill_customer_sk),
     cs
     AS (SELECT d_year                 AS cs_sold_year,
                cs_item_sk,
                cs_bill_customer_sk    cs_customer_sk,
                SUM(cs_quantity)       cs_qty,
                SUM(cs_wholesale_cost) cs_wc,
                SUM(cs_sales_price)    cs_sp
         FROM   catalog_sales
                LEFT JOIN catalog_returns
                  ON cr_order_number = cs_order_number
                     AND cs_item_sk = cr_item_sk
                JOIN date_dim
                  ON cs_sold_date_sk = d_date_sk
         WHERE  cr_order_number IS NULL
         GROUP  BY d_year,
                   cs_item_sk,
                   cs_bill_customer_sk),
     ss
     AS (SELECT d_year                 AS ss_sold_year,
                ss_item_sk,
                ss_customer_sk,
                SUM(ss_quantity)       ss_qty,
                SUM(ss_wholesale_cost) ss_wc,
                SUM(ss_sales_price)    ss_sp
         FROM   store_sales
                LEFT JOIN store_returns
                  ON sr_ticket_number = ss_ticket_number
                     AND ss_item_sk = sr_item_sk
                JOIN date_dim
                  ON ss_sold_date_sk = d_date_sk
         WHERE  sr_ticket_number IS NULL
         GROUP  BY d_year,
                   ss_item_sk,
                   ss_customer_sk)
SELECT ss_sold_year,
       ss_item_sk,
       ss_customer_sk,
       ROUND(ss_qty / ( COALESCE(ws_qty + cs_qty, 1) ), 2) ratio,
       ss_qty                                              store_qty,
       ss_wc                                               store_wholesale_cost,
       ss_sp                                               store_sales_price,
       COALESCE(ws_qty, 0) + COALESCE(cs_qty, 0)           other_chan_qty,
       COALESCE(ws_wc, 0) + COALESCE(cs_wc, 0)             other_chan_wholesale_cost,
       COALESCE(ws_sp, 0) + COALESCE(cs_sp, 0)             other_chan_sales_price
FROM   ss
       LEFT JOIN ws
         ON ( ws_sold_year = ss_sold_year
              AND ws_item_sk = ss_item_sk
              AND ws_customer_sk = ss_customer_sk )
       LEFT JOIN cs
         ON ( cs_sold_year = ss_sold_year
              AND cs_item_sk = cs_item_sk
              AND cs_customer_sk = ss_customer_sk )
WHERE  COALESCE(ws_qty, 0) > 0
       AND COALESCE(cs_qty, 0) > 0
       AND ss_sold_year = 2001
ORDER  BY ss_sold_year,
          ss_item_sk,
          ss_customer_sk,
          ss_qty DESC,
          ss_wc DESC,
          ss_sp DESC,
          COALESCE(ws_qty, 0) + COALESCE(cs_qty, 0),
          COALESCE(ws_wc, 0) + COALESCE(cs_wc, 0),
          COALESCE(ws_sp, 0) + COALESCE(cs_sp, 0),
          ROUND(ss_qty / ( COALESCE(ws_qty + cs_qty, 1) ), 2);

-- query86.tpl
SELECT SUM(ws_net_paid)                         AS total_sum,
       i_category,
       i_class,
       GROUPING(i_category) + GROUPING(i_class) AS lochierarchy,
       RANK() OVER ( PARTITION BY GROUPING(i_category)+GROUPING(i_class), CASE WHEN GROUPING(i_class) = 0 THEN
       i_category END
       ORDER BY SUM(ws_net_paid) DESC)          AS rank_within_parent
FROM   web_sales,
       date_dim d1,
       item
WHERE  d1.d_year = 1999
       AND d1.d_date_sk = ws_sold_date_sk
       AND i_item_sk = ws_item_sk
GROUP  BY ROLLUP( i_category, i_class )
ORDER  BY lochierarchy DESC,
          CASE
            WHEN lochierarchy = 0 THEN i_category
          END,
          rank_within_parent;

-- query1.tpl
WITH customer_total_return
     AS (SELECT sr_customer_sk     AS ctr_customer_sk,
                sr_store_sk        AS ctr_store_sk,
                SUM(sr_return_amt) AS ctr_total_return
         FROM   store_returns,
                date_dim
         WHERE  sr_returned_date_sk = d_date_sk
                AND d_year = 2002
         GROUP  BY sr_customer_sk,
                   sr_store_sk)
SELECT c_customer_id
FROM   customer_total_return ctr1,
       store,
       customer
WHERE  ctr1.ctr_total_return > (SELECT AVG(ctr_total_return) * 1.2
                                FROM   customer_total_return ctr2
                                WHERE  ctr1.ctr_store_sk = ctr2.ctr_store_sk)
       AND s_store_sk = ctr1.ctr_store_sk
       AND s_state = 'AL'
       AND ctr1.ctr_customer_sk = c_customer_sk
ORDER  BY c_customer_id;

-- query91.tpl
SELECT cc_call_center_id call_center,
       cc_name           call_center_name,
       cc_manager        manager,
       SUM(cr_net_loss)  returns_loss
FROM   call_center,
       catalog_returns,
       date_dim,
       customer,
       customer_address,
       customer_demographics,
       household_demographics
WHERE  cr_call_center_sk = cc_call_center_sk
       AND cr_returned_date_sk = d_date_sk
       AND cr_returning_customer_sk = c_customer_sk
       AND cd_demo_sk = c_current_cdemo_sk
       AND hd_demo_sk = c_current_hdemo_sk
       AND ca_address_sk = c_current_addr_sk
       AND d_year = 2002
       AND d_moy = 12
       AND ( ( cd_marital_status = 'M'
               AND cd_education_status = 'Unknown' )
              OR ( cd_marital_status = 'W'
                   AND cd_education_status = 'Advanced Degree' ) )
       AND hd_buy_potential LIKE '5001-10000%'
       AND ca_gmt_offset = -7
GROUP  BY cc_call_center_id,
          cc_name,
          cc_manager,
          cd_marital_status,
          cd_education_status
ORDER  BY SUM(cr_net_loss) DESC;

-- query21.tpl
SELECT *
FROM  (SELECT w_warehouse_name,
              i_item_id,
              SUM(CASE
                    WHEN ( CAST(d_date AS DATE) < CAST ('2002-05-09' AS DATE) ) THEN inv_quantity_on_hand
                    ELSE 0
                  END) AS inv_before,
              SUM(CASE
                    WHEN ( CAST(d_date AS DATE) >= CAST ('2002-05-09' AS DATE) ) THEN inv_quantity_on_hand
                    ELSE 0
                  END) AS inv_after
       FROM   inventory,
              warehouse,
              item,
              date_dim
       WHERE  i_current_price BETWEEN 0.99 AND 1.49
              AND i_item_sk = inv_item_sk
              AND inv_warehouse_sk = w_warehouse_sk
              AND inv_date_sk = d_date_sk
              AND d_date BETWEEN ( CAST ('2002-05-09' AS DATE) - 30 DAYS ) AND ( CAST ('2002-05-09' AS DATE) + 30 DAYS )
       GROUP  BY w_warehouse_name,
                 i_item_id) x
WHERE  ( CASE
           WHEN inv_before > 0 THEN inv_after / inv_before
           ELSE NULL
         END ) BETWEEN 2.0 / 3.0 AND 3.0 / 2.0
ORDER  BY w_warehouse_name,
          i_item_id;

-- query43.tpl
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
FROM   date_dim,
       store_sales,
       store
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

-- query27.tpl
SELECT i_item_id,
       s_state,
       GROUPING(s_state)   g_state,
       AVG(ss_quantity)    agg1,
       AVG(ss_list_price)  agg2,
       AVG(ss_coupon_amt)  agg3,
       AVG(ss_sales_price) agg4
FROM   store_sales,
       customer_demographics,
       date_dim,
       store,
       item
WHERE  ss_sold_date_sk = d_date_sk
       AND ss_item_sk = i_item_sk
       AND ss_store_sk = s_store_sk
       AND ss_cdemo_sk = cd_demo_sk
       AND cd_gender = 'M'
       AND cd_marital_status = 'D'
       AND cd_education_status = 'Primary'
       AND d_year = 2002
       AND s_state IN ( 'SD', 'AL', 'TN', 'SD',
                        'SD', 'SD' )
GROUP  BY ROLLUP ( i_item_id, s_state )
ORDER  BY i_item_id,
          s_state;

-- query94.tpl
SELECT COUNT(DISTINCT ws_order_number) AS "order count",
       SUM(ws_ext_ship_cost)           AS "total shipping cost",
       SUM(ws_net_profit)              AS "total net profit"
FROM   web_sales ws1,
       date_dim,
       customer_address,
       web_site
WHERE  d_date BETWEEN '2002-3-01' AND ( CAST('2002-3-01' AS DATE) + 60 DAYS )
       AND ws1.ws_ship_date_sk = d_date_sk
       AND ws1.ws_ship_addr_sk = ca_address_sk
       AND ca_state = 'IL'
       AND ws1.ws_web_site_sk = web_site_sk
       AND web_company_name = 'pri'
       AND EXISTS (SELECT *
                   FROM   web_sales ws2
                   WHERE  ws1.ws_order_number = ws2.ws_order_number
                          AND ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
       AND NOT EXISTS(SELECT *
                      FROM   web_returns wr1
                      WHERE  ws1.ws_order_number = wr1.wr_order_number)
ORDER  BY COUNT(DISTINCT ws_order_number);

-- query45.tpl
SELECT ca_zip,
       SUM(ws_sales_price)
FROM   web_sales,
       customer,
       customer_address,
       date_dim,
       item
WHERE  ws_bill_customer_sk = c_customer_sk
       AND c_current_addr_sk = ca_address_sk
       AND ws_item_sk = i_item_sk
       AND ( SUBSTR(ca_zip, 1, 5) IN ( '85669', '86197', '88274', '83405',
                                       '86475', '85392', '85460', '80348', '81792' )
              OR i_item_id IN (SELECT i_item_id
                               FROM   item
                               WHERE  i_item_sk IN ( 2, 3, 5, 7,
                                                     11, 13, 17, 19,
                                                     23, 29 )) )
       AND ws_sold_date_sk = d_date_sk
       AND d_qoy = 2
       AND d_year = 1999
GROUP  BY ca_zip
ORDER  BY ca_zip;

-- query58.tpl
WITH ss_items
     AS (SELECT i_item_id               item_id,
                SUM(ss_ext_sales_price) ss_item_rev
         FROM   store_sales,
                item,
                date_dim
         WHERE  ss_item_sk = i_item_sk
                AND d_date IN (SELECT d_date
                               FROM   date_dim
                               WHERE  d_week_seq = (SELECT d_week_seq
                                                    FROM   date_dim
                                                    WHERE  d_date = '2000-03-29'))
                AND ss_sold_date_sk = d_date_sk
         GROUP  BY i_item_id),
     cs_items
     AS (SELECT i_item_id               item_id,
                SUM(cs_ext_sales_price) cs_item_rev
         FROM   catalog_sales,
                item,
                date_dim
         WHERE  cs_item_sk = i_item_sk
                AND d_date IN (SELECT d_date
                               FROM   date_dim
                               WHERE  d_week_seq = (SELECT d_week_seq
                                                    FROM   date_dim
                                                    WHERE  d_date = '2000-03-29'))
                AND cs_sold_date_sk = d_date_sk
         GROUP  BY i_item_id),
     ws_items
     AS (SELECT i_item_id               item_id,
                SUM(ws_ext_sales_price) ws_item_rev
         FROM   web_sales,
                item,
                date_dim
         WHERE  ws_item_sk = i_item_sk
                AND d_date IN (SELECT d_date
                               FROM   date_dim
                               WHERE  d_week_seq = (SELECT d_week_seq
                                                    FROM   date_dim
                                                    WHERE  d_date = '2000-03-29'))
                AND ws_sold_date_sk = d_date_sk
         GROUP  BY i_item_id)
SELECT ss_items.item_id,
       ss_item_rev,
       ss_item_rev / ( ss_item_rev + cs_item_rev + ws_item_rev ) / 3 * 100 ss_dev,
       cs_item_rev,
       cs_item_rev / ( ss_item_rev + cs_item_rev + ws_item_rev ) / 3 * 100 cs_dev,
       ws_item_rev,
       ws_item_rev / ( ss_item_rev + cs_item_rev + ws_item_rev ) / 3 * 100 ws_dev,
       ( ss_item_rev + cs_item_rev + ws_item_rev ) / 3                     average
FROM   ss_items,
       cs_items,
       ws_items
WHERE  ss_items.item_id = cs_items.item_id
       AND ss_items.item_id = ws_items.item_id
       AND ss_item_rev BETWEEN 0.9 * cs_item_rev AND 1.1 * cs_item_rev
       AND ss_item_rev BETWEEN 0.9 * ws_item_rev AND 1.1 * ws_item_rev
       AND cs_item_rev BETWEEN 0.9 * ss_item_rev AND 1.1 * ss_item_rev
       AND cs_item_rev BETWEEN 0.9 * ws_item_rev AND 1.1 * ws_item_rev
       AND ws_item_rev BETWEEN 0.9 * ss_item_rev AND 1.1 * ss_item_rev
       AND ws_item_rev BETWEEN 0.9 * cs_item_rev AND 1.1 * cs_item_rev
ORDER  BY item_id,
          ss_item_rev;

-- query64.tpl
WITH cs_ui
     AS (SELECT cs_item_sk,
                SUM(cs_ext_list_price)                                       AS sale,
                SUM(cr_refunded_cash + cr_reversed_charge + cr_store_credit) AS refund
         FROM   catalog_sales,
                catalog_returns
         WHERE  cs_item_sk = cr_item_sk
                AND cs_order_number = cr_order_number
         GROUP  BY cs_item_sk
         HAVING SUM(cs_ext_list_price) > 2 * SUM(cr_refunded_cash + cr_reversed_charge + cr_store_credit)),
     cross_sales
     AS (SELECT i_product_name         product_name,
                i_item_sk              item_sk,
                s_store_name           store_name,
                s_zip                  store_zip,
                ad1.ca_street_number   b_street_number,
                ad1.ca_street_name     b_streen_name,
                ad1.ca_city            b_city,
                ad1.ca_zip             b_zip,
                ad2.ca_street_number   c_street_number,
                ad2.ca_street_name     c_street_name,
                ad2.ca_city            c_city,
                ad2.ca_zip             c_zip,
                d1.d_year              AS syear,
                d2.d_year              AS fsyear,
                d3.d_year              s2year,
                COUNT(*)               cnt,
                SUM(ss_wholesale_cost) s1,
                SUM(ss_list_price)     s2,
                SUM(ss_coupon_amt)     s3
         FROM   store_sales,
                store_returns,
                cs_ui,
                date_dim d1,
                date_dim d2,
                date_dim d3,
                store,
                customer,
                customer_demographics cd1,
                customer_demographics cd2,
                promotion,
                household_demographics hd1,
                household_demographics hd2,
                customer_address ad1,
                customer_address ad2,
                income_band ib1,
                income_band ib2,
                item
         WHERE  ss_store_sk = s_store_sk
                AND ss_sold_date_sk = d1.d_date_sk
                AND ss_customer_sk = c_customer_sk
                AND ss_cdemo_sk = cd1.cd_demo_sk
                AND ss_hdemo_sk = hd1.hd_demo_sk
                AND ss_addr_sk = ad1.ca_address_sk
                AND ss_item_sk = i_item_sk
                AND ss_item_sk = sr_item_sk
                AND ss_ticket_number = sr_ticket_number
                AND ss_item_sk = cs_ui.cs_item_sk
                AND c_current_cdemo_sk = cd2.cd_demo_sk
                AND c_current_hdemo_sk = hd2.hd_demo_sk
                AND c_current_addr_sk = ad2.ca_address_sk
                AND c_first_sales_date_sk = d2.d_date_sk
                AND c_first_shipto_date_sk = d3.d_date_sk
                AND ss_promo_sk = p_promo_sk
                AND hd1.hd_income_band_sk = ib1.ib_income_band_sk
                AND hd2.hd_income_band_sk = ib2.ib_income_band_sk
                AND cd1.cd_marital_status <> cd2.cd_marital_status
                AND i_color IN ( 'powder', 'rosy', 'smoke', 'linen',
                                 'red', 'orchid' )
                AND i_current_price BETWEEN 63 AND 63 + 10
                AND i_current_price BETWEEN 63 + 1 AND 63 + 15
         GROUP  BY i_product_name,
                   i_item_sk,
                   s_store_name,
                   s_zip,
                   ad1.ca_street_number,
                   ad1.ca_street_name,
                   ad1.ca_city,
                   ad1.ca_zip,
                   ad2.ca_street_number,
                   ad2.ca_street_name,
                   ad2.ca_city,
                   ad2.ca_zip,
                   d1.d_year,
                   d2.d_year,
                   d3.d_year)
SELECT cs1.product_name,
       cs1.store_name,
       cs1.store_zip,
       cs1.b_street_number,
       cs1.b_streen_name,
       cs1.b_city,
       cs1.b_zip,
       cs1.c_street_number,
       cs1.c_street_name,
       cs1.c_city,
       cs1.c_zip,
       cs1.syear,
       cs1.cnt,
       cs1.s1,
       cs1.s2,
       cs1.s3,
       cs2.s1,
       cs2.s2,
       cs2.s3,
       cs2.syear,
       cs2.cnt
FROM   cross_sales cs1,
       cross_sales cs2
WHERE  cs1.item_sk = cs2.item_sk
       AND cs1.syear = 2001
       AND cs2.syear = 2001 + 1
       AND cs2.cnt <= cs1.cnt
       AND cs1.store_name = cs2.store_name
       AND cs1.store_zip = cs2.store_zip
ORDER  BY cs1.product_name,
          cs1.store_name,
          cs2.cnt;

-- query36.tpl
SELECT SUM(ss_net_profit) / SUM(ss_ext_sales_price)             AS gross_margin,
       i_category,
       i_class,
       GROUPING(i_category) + GROUPING(i_class)                 AS lochierarchy,
       RANK() OVER ( PARTITION BY GROUPING(i_category)+GROUPING(i_class), CASE WHEN GROUPING(i_class) = 0 THEN
       i_category END
       ORDER BY SUM(ss_net_profit)/SUM(ss_ext_sales_price) ASC) AS rank_within_parent
FROM   store_sales,
       date_dim d1,
       item,
       store
WHERE  d1.d_year = 1999
       AND d1.d_date_sk = ss_sold_date_sk
       AND i_item_sk = ss_item_sk
       AND s_store_sk = ss_store_sk
       AND s_state IN ( 'SD', 'AL', 'TN', 'SD',
                        'SD', 'SD', 'AL', 'SD' )
GROUP  BY ROLLUP( i_category, i_class )
ORDER  BY lochierarchy DESC,
          CASE
            WHEN lochierarchy = 0 THEN i_category
          END,
          rank_within_parent;

-- query33.tpl
WITH ss
     AS (SELECT i_manufact_id,
                SUM(ss_ext_sales_price) total_sales
         FROM   store_sales,
                date_dim,
                customer_address,
                item
         WHERE  i_manufact_id IN (SELECT i_manufact_id
                                  FROM   item
                                  WHERE  i_category IN ( 'Electronics' ))
                AND ss_item_sk = i_item_sk
                AND ss_sold_date_sk = d_date_sk
                AND d_year = 2001
                AND d_moy = 5
                AND ss_addr_sk = ca_address_sk
                AND ca_gmt_offset = -5
         GROUP  BY i_manufact_id),
     cs
     AS (SELECT i_manufact_id,
                SUM(cs_ext_sales_price) total_sales
         FROM   catalog_sales,
                date_dim,
                customer_address,
                item
         WHERE  i_manufact_id IN (SELECT i_manufact_id
                                  FROM   item
                                  WHERE  i_category IN ( 'Electronics' ))
                AND cs_item_sk = i_item_sk
                AND cs_sold_date_sk = d_date_sk
                AND d_year = 2001
                AND d_moy = 5
                AND cs_bill_addr_sk = ca_address_sk
                AND ca_gmt_offset = -5
         GROUP  BY i_manufact_id),
     ws
     AS (SELECT i_manufact_id,
                SUM(ws_ext_sales_price) total_sales
         FROM   web_sales,
                date_dim,
                customer_address,
                item
         WHERE  i_manufact_id IN (SELECT i_manufact_id
                                  FROM   item
                                  WHERE  i_category IN ( 'Electronics' ))
                AND ws_item_sk = i_item_sk
                AND ws_sold_date_sk = d_date_sk
                AND d_year = 2001
                AND d_moy = 5
                AND ws_bill_addr_sk = ca_address_sk
                AND ca_gmt_offset = -5
         GROUP  BY i_manufact_id)
SELECT i_manufact_id,
       SUM(total_sales) total_sales
FROM   (SELECT *
        FROM   ss
        UNION ALL
        SELECT *
        FROM   cs
        UNION ALL
        SELECT *
        FROM   ws) tmp1
GROUP  BY i_manufact_id
ORDER  BY total_sales;

-- query46.tpl
SELECT c_last_name,
       c_first_name,
       ca_city,
       bought_city,
       ss_ticket_number,
       amt,
       profit
FROM   (SELECT ss_ticket_number,
               ss_customer_sk,
               ca_city            bought_city,
               SUM(ss_coupon_amt) amt,
               SUM(ss_net_profit) profit
        FROM   store_sales,
               date_dim,
               store,
               household_demographics,
               customer_address
        WHERE  store_sales.ss_sold_date_sk = date_dim.d_date_sk
               AND store_sales.ss_store_sk = store.s_store_sk
               AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
               AND store_sales.ss_addr_sk = customer_address.ca_address_sk
               AND ( household_demographics.hd_dep_count = 7
                      OR household_demographics.hd_vehicle_count = 0 )
               AND date_dim.d_dow IN ( 6, 0 )
               AND date_dim.d_year IN ( 1998, 1998 + 1, 1998 + 2 )
               AND store.s_city IN ( 'Oak Grove', 'Riverside', 'Fairview', 'Five Points', 'Midway' )
        GROUP  BY ss_ticket_number,
                  ss_customer_sk,
                  ss_addr_sk,
                  ca_city) dn,
       customer,
       customer_address current_addr
WHERE  ss_customer_sk = c_customer_sk
       AND customer.c_current_addr_sk = current_addr.ca_address_sk
       AND current_addr.ca_city <> bought_city
ORDER  BY c_last_name,
          c_first_name,
          ca_city,
          bought_city,
          ss_ticket_number;

-- query62.tpl
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
FROM   web_sales,
       warehouse,
       ship_mode,
       web_site,
       date_dim
WHERE  EXTRACT(YEAR FROM d_date) = 1998
       AND ws_ship_date_sk = d_date_sk
       AND ws_warehouse_sk = w_warehouse_sk
       AND ws_ship_mode_sk = sm_ship_mode_sk
       AND ws_web_site_sk = web_site_sk
GROUP  BY SUBSTR(w_warehouse_name, 1, 20),
          sm_type,
          web_name
ORDER  BY SUBSTR(w_warehouse_name, 1, 20),
          sm_type,
          web_name;

-- query16.tpl
SELECT COUNT(DISTINCT cs_order_number) AS "order count",
       SUM(cs_ext_ship_cost)           AS "total shipping cost",
       SUM(cs_net_profit)              AS "total net profit"
FROM   catalog_sales cs1,
       date_dim,
       customer_address,
       call_center
WHERE  d_date BETWEEN '2000-3-01' AND ( CAST('2000-3-01' AS DATE) + 60 DAYS )
       AND cs1.cs_ship_date_sk = d_date_sk
       AND cs1.cs_ship_addr_sk = ca_address_sk
       AND ca_state = 'VA'
       AND cs1.cs_call_center_sk = cc_call_center_sk
       AND cc_county IN ( 'Williamson County', 'Ziebach County', 'Walker County', 'Ziebach County', 'Ziebach County' )
       AND EXISTS (SELECT *
                   FROM   catalog_sales cs2
                   WHERE  cs1.cs_order_number = cs2.cs_order_number
                          AND cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
       AND NOT EXISTS(SELECT *
                      FROM   catalog_returns cr1
                      WHERE  cs1.cs_order_number = cr1.cr_order_number)
ORDER  BY COUNT(DISTINCT cs_order_number);

-- query10.tpl
SELECT cd_gender,
       cd_marital_status,
       cd_education_status,
       COUNT(*) cnt1,
       cd_purchase_estimate,
       COUNT(*) cnt2,
       cd_credit_rating,
       COUNT(*) cnt3,
       cd_dep_count,
       COUNT(*) cnt4,
       cd_dep_employed_count,
       COUNT(*) cnt5,
       cd_dep_college_count,
       COUNT(*) cnt6
FROM   customer c,
       customer_address ca,
       customer_demographics
WHERE  c.c_current_addr_sk = ca.ca_address_sk
       AND ca_county IN ( 'Clay County', 'Albemarle County', 'Union County', 'Lake County', 'Dubuque County' )
       AND cd_demo_sk = c.c_current_cdemo_sk
       AND EXISTS (SELECT *
                   FROM   store_sales,
                          date_dim
                   WHERE  c.c_customer_sk = ss_customer_sk
                          AND ss_sold_date_sk = d_date_sk
                          AND d_year = 2001
                          AND d_moy BETWEEN 1 AND 1 + 3)
       AND ( EXISTS (SELECT *
                     FROM   web_sales,
                            date_dim
                     WHERE  c.c_customer_sk = ws_bill_customer_sk
                            AND ws_sold_date_sk = d_date_sk
                            AND d_year = 2001
                            AND d_moy BETWEEN 1 AND 1 + 3)
              OR EXISTS (SELECT *
                         FROM   catalog_sales,
                                date_dim
                         WHERE  c.c_customer_sk = cs_ship_customer_sk
                                AND cs_sold_date_sk = d_date_sk
                                AND d_year = 2001
                                AND d_moy BETWEEN 1 AND 1 + 3) )
GROUP  BY cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating,
          cd_dep_count,
          cd_dep_employed_count,
          cd_dep_college_count
ORDER  BY cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating,
          cd_dep_count,
          cd_dep_employed_count,
          cd_dep_college_count;

-- query63.tpl
SELECT *
FROM   (SELECT i_manager_id,
               SUM(ss_sales_price)                                       sum_sales,
               AVG(SUM(ss_sales_price)) OVER (PARTITION BY i_manager_id) avg_monthly_sales
        FROM   item,
               store_sales,
               date_dim,
               store
        WHERE  ss_item_sk = i_item_sk
               AND ss_sold_date_sk = d_date_sk
               AND ss_store_sk = s_store_sk
               AND d_year IN ( 1999 )
               AND ( ( i_category IN ( 'Books', 'Children', 'Electronics' )
                       AND i_class IN ( 'personal', 'portable', 'refernece', 'self-help' )
                       AND i_brand IN ( 'scholaramalgamalg #14', 'scholaramalgamalg #7', 'exportiunivamalg #9',
                                        'scholaramalgamalg #9'
                                      ) )
                      OR ( i_category IN ( 'Women', 'Music', 'Men' )
                           AND i_class IN ( 'accessories', 'classical', 'fragrances', 'pants' )
                           AND i_brand IN ( 'amalgimporto #1', 'edu packscholar #1', 'exportiimporto #1',
                                            'importoamalg #1' ) )
                   )
        GROUP  BY i_manager_id,
                  d_moy) tmp1
WHERE  CASE
         WHEN avg_monthly_sales > 0 THEN ABS (sum_sales - avg_monthly_sales) / avg_monthly_sales
         ELSE NULL
       END > 0.1
ORDER  BY i_manager_id,
          avg_monthly_sales,
          sum_sales;

-- query69.tpl
SELECT cd_gender,
       cd_marital_status,
       cd_education_status,
       COUNT(*) cnt1,
       cd_purchase_estimate,
       COUNT(*) cnt2,
       cd_credit_rating,
       COUNT(*) cnt3
FROM   customer c,
       customer_address ca,
       customer_demographics
WHERE  c.c_current_addr_sk = ca.ca_address_sk
       AND ca_state IN ( 'SD', 'TX', 'NM' )
       AND cd_demo_sk = c.c_current_cdemo_sk
       AND EXISTS (SELECT *
                   FROM   store_sales,
                          date_dim
                   WHERE  c.c_customer_sk = ss_customer_sk
                          AND ss_sold_date_sk = d_date_sk
                          AND d_year = 2000
                          AND d_moy BETWEEN 1 AND 1 + 2)
       AND ( NOT EXISTS (SELECT *
                         FROM   web_sales,
                                date_dim
                         WHERE  c.c_customer_sk = ws_bill_customer_sk
                                AND ws_sold_date_sk = d_date_sk
                                AND d_year = 2000
                                AND d_moy BETWEEN 1 AND 1 + 2)
             AND NOT EXISTS (SELECT *
                             FROM   catalog_sales,
                                    date_dim
                             WHERE  c.c_customer_sk = cs_ship_customer_sk
                                    AND cs_sold_date_sk = d_date_sk
                                    AND d_year = 2000
                                    AND d_moy BETWEEN 1 AND 1 + 2) )
GROUP  BY cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating
ORDER  BY cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating;

-- query60.tpl
WITH ss
     AS (SELECT i_item_id,
                SUM(ss_ext_sales_price) total_sales
         FROM   store_sales,
                date_dim,
                customer_address,
                item
         WHERE  i_item_id IN (SELECT i_item_id
                              FROM   item
                              WHERE  i_category IN ( 'Men' ))
                AND ss_item_sk = i_item_sk
                AND ss_sold_date_sk = d_date_sk
                AND d_year = 1999
                AND d_moy = 8
                AND ss_addr_sk = ca_address_sk
                AND ca_gmt_offset = -6
         GROUP  BY i_item_id),
     cs
     AS (SELECT i_item_id,
                SUM(cs_ext_sales_price) total_sales
         FROM   catalog_sales,
                date_dim,
                customer_address,
                item
         WHERE  i_item_id IN (SELECT i_item_id
                              FROM   item
                              WHERE  i_category IN ( 'Men' ))
                AND cs_item_sk = i_item_sk
                AND cs_sold_date_sk = d_date_sk
                AND d_year = 1999
                AND d_moy = 8
                AND cs_bill_addr_sk = ca_address_sk
                AND ca_gmt_offset = -6
         GROUP  BY i_item_id),
     ws
     AS (SELECT i_item_id,
                SUM(ws_ext_sales_price) total_sales
         FROM   web_sales,
                date_dim,
                customer_address,
                item
         WHERE  i_item_id IN (SELECT i_item_id
                              FROM   item
                              WHERE  i_category IN ( 'Men' ))
                AND ws_item_sk = i_item_sk
                AND ws_sold_date_sk = d_date_sk
                AND d_year = 1999
                AND d_moy = 8
                AND ws_bill_addr_sk = ca_address_sk
                AND ca_gmt_offset = -6
         GROUP  BY i_item_id)
SELECT i_item_id,
       SUM(total_sales) total_sales
FROM   (SELECT *
        FROM   ss
        UNION ALL
        SELECT *
        FROM   cs
        UNION ALL
        SELECT *
        FROM   ws) tmp1
GROUP  BY i_item_id
ORDER  BY i_item_id,
          total_sales;

-- query59.tpl
WITH wss
     AS (SELECT d_week_seq,
                ss_store_sk,
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
         FROM   store_sales,
                date_dim
         WHERE  d_date_sk = ss_sold_date_sk
         GROUP  BY d_week_seq,
                   ss_store_sk)
SELECT s_store_name1,
       s_store_id1,
       d_week_seq1,
       sun_sales1 / sun_sales2,
       mon_sales1 / mon_sales2,
       tue_sales1 / tue_sales1,
       wed_sales1 / wed_sales2,
       thu_sales1 / thu_sales2,
       fri_sales1 / fri_sales2,
       sat_sales1 / sat_sales2
FROM   (SELECT s_store_name   s_store_name1,
               wss.d_week_seq d_week_seq1,
               s_store_id     s_store_id1,
               sun_sales      sun_sales1,
               mon_sales      mon_sales1,
               tue_sales      tue_sales1,
               wed_sales      wed_sales1,
               thu_sales      thu_sales1,
               fri_sales      fri_sales1,
               sat_sales      sat_sales1
        FROM   wss,
               store,
               date_dim d
        WHERE  d.d_week_seq = wss.d_week_seq
               AND ss_store_sk = s_store_sk
               AND d_year = 2000) y,
       (SELECT s_store_name   s_store_name2,
               wss.d_week_seq d_week_seq2,
               s_store_id     s_store_id2,
               sun_sales      sun_sales2,
               mon_sales      mon_sales2,
               tue_sales      tue_sales2,
               wed_sales      wed_sales2,
               thu_sales      thu_sales2,
               fri_sales      fri_sales2,
               sat_sales      sat_sales2
        FROM   wss,
               store,
               date_dim d
        WHERE  d.d_week_seq = wss.d_week_seq
               AND ss_store_sk = s_store_sk
               AND d_year = 2000 + 1) x
WHERE  s_store_id1 = s_store_id2
       AND d_week_seq1 = d_week_seq2 - 52
ORDER  BY s_store_name1,
          s_store_id1,
          d_week_seq1;

-- query37.tpl
SELECT i_item_id,
       i_item_desc,
       i_current_price
FROM   item,
       inventory,
       date_dim,
       catalog_sales
WHERE  i_current_price BETWEEN 24 AND 24 + 30
       AND inv_item_sk = i_item_sk
       AND d_date_sk = inv_date_sk
       AND d_date BETWEEN CAST('2001-02-08' AS DATE) AND ( CAST('2001-02-08' AS DATE) + 60 DAYS )
       AND i_manufact_id IN ( 946, 774, 749, 744 )
       AND inv_quantity_on_hand BETWEEN 100 AND 500
       AND cs_item_sk = i_item_sk
GROUP  BY i_item_id,
          i_item_desc,
          i_current_price
ORDER  BY i_item_id;

-- query98.tpl
SELECT i_item_desc,
       i_category,
       i_class,
       i_current_price,
       SUM(ss_ext_sales_price)                                                                  AS itemrevenue,
       SUM(ss_ext_sales_price) * 100 / SUM(SUM(ss_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio
FROM   store_sales,
       item,
       date_dim
WHERE  ss_item_sk = i_item_sk
       AND i_category IN ( 'Jewelry', 'Men', 'Children' )
       AND ss_sold_date_sk = d_date_sk
       AND d_date BETWEEN CAST('2000-01-28' AS DATE) AND ( CAST('2000-01-28' AS DATE) + 30 DAYS )
GROUP  BY i_item_id,
          i_item_desc,
          i_category,
          i_class,
          i_current_price
ORDER  BY i_category,
          i_class,
          i_item_id,
          i_item_desc,
          revenueratio;

-- query85.tpl
SELECT SUBSTR(r_reason_desc, 1, 20),
       AVG(ws_quantity),
       AVG(wr_refunded_cash),
       AVG(wr_fee)
FROM   web_sales,
       web_returns,
       web_page,
       customer_demographics cd1,
       customer_demographics cd2,
       customer_address,
       date_dim,
       reason
WHERE  ws_web_page_sk = wp_web_page_sk
       AND ws_item_sk = wr_item_sk
       AND ws_order_number = wr_order_number
       AND ws_sold_date_sk = d_date_sk
       AND d_year = 2002
       AND cd1.cd_demo_sk = wr_refunded_cdemo_sk
       AND cd2.cd_demo_sk = wr_returning_cdemo_sk
       AND ca_address_sk = wr_refunded_addr_sk
       AND r_reason_sk = wr_reason_sk
       AND ( ( cd1.cd_marital_status = 'D'
               AND cd1.cd_marital_status = cd2.cd_marital_status
               AND cd1.cd_education_status = 'College'
               AND cd1.cd_education_status = cd2.cd_education_status
               AND ws_sales_price BETWEEN 100.00 AND 150.00 )
              OR ( cd1.cd_marital_status = 'S'
                   AND cd1.cd_marital_status = cd2.cd_marital_status
                   AND cd1.cd_education_status = 'Secondary'
                   AND cd1.cd_education_status = cd2.cd_education_status
                   AND ws_sales_price BETWEEN 50.00 AND 100.00 )
              OR ( cd1.cd_marital_status = 'W'
                   AND cd1.cd_marital_status = cd2.cd_marital_status
                   AND cd1.cd_education_status = '2 yr Degree'
                   AND cd1.cd_education_status = cd2.cd_education_status
                   AND ws_sales_price BETWEEN 150.00 AND 200.00 ) )
       AND ( ( ca_country = 'United States'
               AND ca_state IN ( 'TX', 'OH', 'IN' )
               AND ws_net_profit BETWEEN 100 AND 200 )
              OR ( ca_country = 'United States'
                   AND ca_state IN ( 'AR', 'CA', 'NM' )
                   AND ws_net_profit BETWEEN 150 AND 300 )
              OR ( ca_country = 'United States'
                   AND ca_state IN ( 'TX', 'AR', 'ND' )
                   AND ws_net_profit BETWEEN 50 AND 250 ) )
GROUP  BY r_reason_desc
ORDER  BY SUBSTR(r_reason_desc, 1, 20),
          AVG(ws_quantity),
          AVG(wr_refunded_cash),
          AVG(wr_fee);

-- query70.tpl
SELECT SUM(ss_net_profit)                     AS total_sum,
       s_state,
       s_county,
       GROUPING(s_state) + GROUPING(s_county) AS lochierarchy,
       RANK() OVER ( PARTITION BY GROUPING(s_state)+GROUPING(s_county), CASE WHEN GROUPING(s_county) = 0 THEN s_state
       END ORDER
       BY SUM(ss_net_profit) DESC)            AS rank_within_parent
FROM   store_sales,
       date_dim d1,
       store
WHERE  d1.d_year = 2002
       AND d1.d_date_sk = ss_sold_date_sk
       AND s_store_sk = ss_store_sk
       AND s_state IN (SELECT s_state
                       FROM   (SELECT s_state                                                              AS s_state,
                                      RANK() OVER ( PARTITION BY s_state ORDER BY SUM(ss_net_profit) DESC) AS ranking
                               FROM   store_sales,
                                      store,
                                      date_dim
                               WHERE  d_year = 2002
                                      AND d_date_sk = ss_sold_date_sk
                                      AND s_store_sk = ss_store_sk
                               GROUP  BY s_state) tmp1
                       WHERE  ranking <= 5)
GROUP  BY ROLLUP( s_state, s_county )
ORDER  BY lochierarchy DESC,
          CASE
            WHEN lochierarchy = 0 THEN s_state
          END,
          rank_within_parent;

-- query67.tpl
SELECT *
FROM   (SELECT i_category,
               i_class,
               i_brand,
               i_product_name,
               d_year,
               d_qoy,
               d_moy,
               s_store_id,
               sumsales,
               RANK() OVER (PARTITION BY i_category ORDER BY sumsales DESC) rk
        FROM   (SELECT i_category,
                       i_class,
                       i_brand,
                       i_product_name,
                       d_year,
                       d_qoy,
                       d_moy,
                       s_store_id,
                       SUM(COALESCE(ss_sales_price * ss_quantity, 0)) sumsales
                FROM   store_sales,
                       date_dim,
                       store,
                       item
                WHERE  ss_sold_date_sk = d_date_sk
                       AND ss_item_sk = i_item_sk
                       AND ss_store_sk = s_store_sk
                       AND d_year = 1999
                GROUP  BY ROLLUP( i_category, i_class, i_brand, i_product_name,
                 d_year, d_qoy, d_moy, s_store_id ))dw1) dw2
WHERE  rk <= 100
ORDER  BY i_category,
          i_class,
          i_brand,
          i_product_name,
          d_year,
          d_qoy,
          d_moy,
          s_store_id,
          sumsales,
          rk;

-- query28.tpl
SELECT *
FROM   (SELECT AVG(ss_list_price)            b1_lp,
               COUNT(ss_list_price)          b1_cnt,
               COUNT(DISTINCT ss_list_price) b1_cntd
        FROM   store_sales
        WHERE  ss_quantity BETWEEN 0 AND 5
               AND ( ss_list_price BETWEEN 137 AND 137 + 10
                      OR ss_coupon_amt BETWEEN 12463 AND 12463 + 1000
                      OR ss_wholesale_cost BETWEEN 11 AND 11 + 20 )) b1,
       (SELECT AVG(ss_list_price)            b2_lp,
               COUNT(ss_list_price)          b2_cnt,
               COUNT(DISTINCT ss_list_price) b2_cntd
        FROM   store_sales
        WHERE  ss_quantity BETWEEN 6 AND 10
               AND ( ss_list_price BETWEEN 33 AND 33 + 10
                      OR ss_coupon_amt BETWEEN 11715 AND 11715 + 1000
                      OR ss_wholesale_cost BETWEEN 28 AND 28 + 20 )) b2,
       (SELECT AVG(ss_list_price)            b3_lp,
               COUNT(ss_list_price)          b3_cnt,
               COUNT(DISTINCT ss_list_price) b3_cntd
        FROM   store_sales
        WHERE  ss_quantity BETWEEN 11 AND 15
               AND ( ss_list_price BETWEEN 129 AND 129 + 10
                      OR ss_coupon_amt BETWEEN 976 AND 976 + 1000
                      OR ss_wholesale_cost BETWEEN 15 AND 15 + 20 )) b3,
       (SELECT AVG(ss_list_price)            b4_lp,
               COUNT(ss_list_price)          b4_cnt,
               COUNT(DISTINCT ss_list_price) b4_cntd
        FROM   store_sales
        WHERE  ss_quantity BETWEEN 16 AND 20
               AND ( ss_list_price BETWEEN 16 AND 16 + 10
                      OR ss_coupon_amt BETWEEN 13211 AND 13211 + 1000
                      OR ss_wholesale_cost BETWEEN 10 AND 10 + 20 )) b4,
       (SELECT AVG(ss_list_price)            b5_lp,
               COUNT(ss_list_price)          b5_cnt,
               COUNT(DISTINCT ss_list_price) b5_cntd
        FROM   store_sales
        WHERE  ss_quantity BETWEEN 21 AND 25
               AND ( ss_list_price BETWEEN 104 AND 104 + 10
                      OR ss_coupon_amt BETWEEN 5947 AND 5947 + 1000
                      OR ss_wholesale_cost BETWEEN 0 AND 0 + 20 )) b5,
       (SELECT AVG(ss_list_price)            b6_lp,
               COUNT(ss_list_price)          b6_cnt,
               COUNT(DISTINCT ss_list_price) b6_cntd
        FROM   store_sales
        WHERE  ss_quantity BETWEEN 26 AND 30
               AND ( ss_list_price BETWEEN 27 AND 27 + 10
                      OR ss_coupon_amt BETWEEN 10996 AND 10996 + 1000
                      OR ss_wholesale_cost BETWEEN 32 AND 32 + 20 )) b6;

-- query81.tpl
WITH customer_total_return
     AS (SELECT cr_returning_customer_sk   AS ctr_customer_sk,
                ca_state                   AS ctr_state,
                SUM(cr_return_amt_inc_tax) AS ctr_total_return
         FROM   catalog_returns,
                date_dim,
                customer_address
         WHERE  cr_returned_date_sk = d_date_sk
                AND d_year = 1999
                AND cr_returning_addr_sk = ca_address_sk
         GROUP  BY cr_returning_customer_sk,
                   ca_state)
SELECT c_customer_id,
       c_salutation,
       c_first_name,
       c_last_name,
       ca_street_number,
       ca_street_name,
       ca_street_type,
       ca_suite_number,
       ca_city,
       ca_county,
       ca_state,
       ca_zip,
       ca_country,
       ca_gmt_offset,
       ca_location_type,
       ctr_total_return
FROM   customer_total_return ctr1,
       customer_address,
       customer
WHERE  ctr1.ctr_total_return > (SELECT AVG(ctr_total_return) * 1.2
                                FROM   customer_total_return ctr2
                                WHERE  ctr1.ctr_state = ctr2.ctr_state)
       AND ca_address_sk = c_current_addr_sk
       AND ca_state = 'CA'
       AND ctr1.ctr_customer_sk = c_customer_sk
ORDER  BY c_customer_id,
          c_salutation,
          c_first_name,
          c_last_name,
          ca_street_number,
          ca_street_name,
          ca_street_type,
          ca_suite_number,
          ca_city,
          ca_county,
          ca_state,
          ca_zip,
          ca_country,
          ca_gmt_offset,
          ca_location_type,
          ctr_total_return;

-- query97.tpl
WITH ssci
     AS (SELECT ss_customer_sk customer_sk,
                ss_item_sk     item_sk
         FROM   store_sales,
                date_dim
         WHERE  ss_sold_date_sk = d_date_sk
                AND d_year = 2001
         GROUP  BY ss_customer_sk,
                   ss_item_sk),
     csci
     AS (SELECT cs_bill_customer_sk customer_sk,
                cs_item_sk          item_sk
         FROM   catalog_sales,
                date_dim
         WHERE  cs_sold_date_sk = d_date_sk
                AND d_year = 2001
         GROUP  BY cs_bill_customer_sk,
                   cs_item_sk)
SELECT SUM(CASE
             WHEN ssci.customer_sk IS NOT NULL
                  AND csci.customer_sk IS NULL THEN 1
             ELSE 0
           END) store_only,
       SUM(CASE
             WHEN ssci.customer_sk IS NULL
                  AND csci.customer_sk IS NOT NULL THEN 1
             ELSE 0
           END) catalog_only,
       SUM(CASE
             WHEN ssci.customer_sk IS NOT NULL
                  AND csci.customer_sk IS NOT NULL THEN 1
             ELSE 0
           END) store_and_catalog
FROM   ssci
       FULL OUTER JOIN csci
         ON ( ssci.customer_sk = csci.customer_sk
              AND ssci.item_sk = csci.item_sk );

-- query66.tpl
SELECT w_warehouse_name,
       w_warehouse_sq_ft,
       w_city,
       w_county,
       w_state,
       w_country,
       ship_carriers,
       YEAR,
       SUM(jan_sales)                     AS jan_sales,
       SUM(feb_sales)                     AS feb_sales,
       SUM(mar_sales)                     AS mar_sales,
       SUM(apr_sales)                     AS apr_sales,
       SUM(may_sales)                     AS may_sales,
       SUM(jun_sales)                     AS jun_sales,
       SUM(jul_sales)                     AS jul_sales,
       SUM(aug_sales)                     AS aug_sales,
       SUM(sep_sales)                     AS sep_sales,
       SUM(oct_sales)                     AS oct_sales,
       SUM(nov_sales)                     AS nov_sales,
       SUM(dec_sales)                     AS dec_sales,
       SUM(jan_sales / w_warehouse_sq_ft) AS jan_sales_per_sq_foot,
       SUM(feb_sales / w_warehouse_sq_ft) AS feb_sales_per_sq_foot,
       SUM(mar_sales / w_warehouse_sq_ft) AS mar_sales_per_sq_foot,
       SUM(apr_sales / w_warehouse_sq_ft) AS apr_sales_per_sq_foot,
       SUM(may_sales / w_warehouse_sq_ft) AS may_sales_per_sq_foot,
       SUM(jun_sales / w_warehouse_sq_ft) AS jun_sales_per_sq_foot,
       SUM(jul_sales / w_warehouse_sq_ft) AS jul_sales_per_sq_foot,
       SUM(aug_sales / w_warehouse_sq_ft) AS aug_sales_per_sq_foot,
       SUM(sep_sales / w_warehouse_sq_ft) AS sep_sales_per_sq_foot,
       SUM(oct_sales / w_warehouse_sq_ft) AS oct_sales_per_sq_foot,
       SUM(nov_sales / w_warehouse_sq_ft) AS nov_sales_per_sq_foot,
       SUM(dec_sales / w_warehouse_sq_ft) AS dec_sales_per_sq_foot,
       SUM(jan_net)                       AS jan_net,
       SUM(feb_net)                       AS feb_net,
       SUM(mar_net)                       AS mar_net,
       SUM(apr_net)                       AS apr_net,
       SUM(may_net)                       AS may_net,
       SUM(jun_net)                       AS jun_net,
       SUM(jul_net)                       AS jul_net,
       SUM(aug_net)                       AS aug_net,
       SUM(sep_net)                       AS sep_net,
       SUM(oct_net)                       AS oct_net,
       SUM(nov_net)                       AS nov_net,
       SUM(dec_net)                       AS dec_net
FROM   ((SELECT w_warehouse_name,
                w_warehouse_sq_ft,
                w_city,
                w_county,
                w_state,
                w_country,
                'LATVIAN'
                 || ','
                 || 'DHL' AS ship_carriers,
                d_year    AS YEAR,
                SUM(CASE
                      WHEN d_moy = 1 THEN ws_sales_price * ws_quantity
                      ELSE 0
                    END)  AS jan_sales,
                SUM(CASE
                      WHEN d_moy = 2 THEN ws_sales_price * ws_quantity
                      ELSE 0
                    END)  AS feb_sales,
                SUM(CASE
                      WHEN d_moy = 3 THEN ws_sales_price * ws_quantity
                      ELSE 0
                    END)  AS mar_sales,
                SUM(CASE
                      WHEN d_moy = 4 THEN ws_sales_price * ws_quantity
                      ELSE 0
                    END)  AS apr_sales,
                SUM(CASE
                      WHEN d_moy = 5 THEN ws_sales_price * ws_quantity
                      ELSE 0
                    END)  AS may_sales,
                SUM(CASE
                      WHEN d_moy = 6 THEN ws_sales_price * ws_quantity
                      ELSE 0
                    END)  AS jun_sales,
                SUM(CASE
                      WHEN d_moy = 7 THEN ws_sales_price * ws_quantity
                      ELSE 0
                    END)  AS jul_sales,
                SUM(CASE
                      WHEN d_moy = 8 THEN ws_sales_price * ws_quantity
                      ELSE 0
                    END)  AS aug_sales,
                SUM(CASE
                      WHEN d_moy = 9 THEN ws_sales_price * ws_quantity
                      ELSE 0
                    END)  AS sep_sales,
                SUM(CASE
                      WHEN d_moy = 10 THEN ws_sales_price * ws_quantity
                      ELSE 0
                    END)  AS oct_sales,
                SUM(CASE
                      WHEN d_moy = 11 THEN ws_sales_price * ws_quantity
                      ELSE 0
                    END)  AS nov_sales,
                SUM(CASE
                      WHEN d_moy = 12 THEN ws_sales_price * ws_quantity
                      ELSE 0
                    END)  AS dec_sales,
                SUM(CASE
                      WHEN d_moy = 1 THEN ws_net_paid_inc_ship * ws_quantity
                      ELSE 0
                    END)  AS jan_net,
                SUM(CASE
                      WHEN d_moy = 2 THEN ws_net_paid_inc_ship * ws_quantity
                      ELSE 0
                    END)  AS feb_net,
                SUM(CASE
                      WHEN d_moy = 3 THEN ws_net_paid_inc_ship * ws_quantity
                      ELSE 0
                    END)  AS mar_net,
                SUM(CASE
                      WHEN d_moy = 4 THEN ws_net_paid_inc_ship * ws_quantity
                      ELSE 0
                    END)  AS apr_net,
                SUM(CASE
                      WHEN d_moy = 5 THEN ws_net_paid_inc_ship * ws_quantity
                      ELSE 0
                    END)  AS may_net,
                SUM(CASE
                      WHEN d_moy = 6 THEN ws_net_paid_inc_ship * ws_quantity
                      ELSE 0
                    END)  AS jun_net,
                SUM(CASE
                      WHEN d_moy = 7 THEN ws_net_paid_inc_ship * ws_quantity
                      ELSE 0
                    END)  AS jul_net,
                SUM(CASE
                      WHEN d_moy = 8 THEN ws_net_paid_inc_ship * ws_quantity
                      ELSE 0
                    END)  AS aug_net,
                SUM(CASE
                      WHEN d_moy = 9 THEN ws_net_paid_inc_ship * ws_quantity
                      ELSE 0
                    END)  AS sep_net,
                SUM(CASE
                      WHEN d_moy = 10 THEN ws_net_paid_inc_ship * ws_quantity
                      ELSE 0
                    END)  AS oct_net,
                SUM(CASE
                      WHEN d_moy = 11 THEN ws_net_paid_inc_ship * ws_quantity
                      ELSE 0
                    END)  AS nov_net,
                SUM(CASE
                      WHEN d_moy = 12 THEN ws_net_paid_inc_ship * ws_quantity
                      ELSE 0
                    END)  AS dec_net
         FROM   web_sales,
                warehouse,
                date_dim,
                time_dim,
                ship_mode
         WHERE  ws_warehouse_sk = w_warehouse_sk
                AND ws_sold_date_sk = d_date_sk
                AND ws_sold_time_sk = t_time_sk
                AND ws_ship_mode_sk = sm_ship_mode_sk
                AND d_year = 2001
                AND t_time BETWEEN 39920 AND 39920 + 28800
                AND sm_carrier IN ( 'LATVIAN', 'DHL' )
         GROUP  BY w_warehouse_name,
                   w_warehouse_sq_ft,
                   w_city,
                   w_county,
                   w_state,
                   w_country,
                   d_year)
        UNION ALL
        (SELECT w_warehouse_name,
                w_warehouse_sq_ft,
                w_city,
                w_county,
                w_state,
                w_country,
                'LATVIAN'
                 || ','
                 || 'DHL' AS ship_carriers,
                d_year    AS YEAR,
                SUM(CASE
                      WHEN d_moy = 1 THEN cs_sales_price * cs_quantity
                      ELSE 0
                    END)  AS jan_sales,
                SUM(CASE
                      WHEN d_moy = 2 THEN cs_sales_price * cs_quantity
                      ELSE 0
                    END)  AS feb_sales,
                SUM(CASE
                      WHEN d_moy = 3 THEN cs_sales_price * cs_quantity
                      ELSE 0
                    END)  AS mar_sales,
                SUM(CASE
                      WHEN d_moy = 4 THEN cs_sales_price * cs_quantity
                      ELSE 0
                    END)  AS apr_sales,
                SUM(CASE
                      WHEN d_moy = 5 THEN cs_sales_price * cs_quantity
                      ELSE 0
                    END)  AS may_sales,
                SUM(CASE
                      WHEN d_moy = 6 THEN cs_sales_price * cs_quantity
                      ELSE 0
                    END)  AS jun_sales,
                SUM(CASE
                      WHEN d_moy = 7 THEN cs_sales_price * cs_quantity
                      ELSE 0
                    END)  AS jul_sales,
                SUM(CASE
                      WHEN d_moy = 8 THEN cs_sales_price * cs_quantity
                      ELSE 0
                    END)  AS aug_sales,
                SUM(CASE
                      WHEN d_moy = 9 THEN cs_sales_price * cs_quantity
                      ELSE 0
                    END)  AS sep_sales,
                SUM(CASE
                      WHEN d_moy = 10 THEN cs_sales_price * cs_quantity
                      ELSE 0
                    END)  AS oct_sales,
                SUM(CASE
                      WHEN d_moy = 11 THEN cs_sales_price * cs_quantity
                      ELSE 0
                    END)  AS nov_sales,
                SUM(CASE
                      WHEN d_moy = 12 THEN cs_sales_price * cs_quantity
                      ELSE 0
                    END)  AS dec_sales,
                SUM(CASE
                      WHEN d_moy = 1 THEN cs_net_paid_inc_tax * cs_quantity
                      ELSE 0
                    END)  AS jan_net,
                SUM(CASE
                      WHEN d_moy = 2 THEN cs_net_paid_inc_tax * cs_quantity
                      ELSE 0
                    END)  AS feb_net,
                SUM(CASE
                      WHEN d_moy = 3 THEN cs_net_paid_inc_tax * cs_quantity
                      ELSE 0
                    END)  AS mar_net,
                SUM(CASE
                      WHEN d_moy = 4 THEN cs_net_paid_inc_tax * cs_quantity
                      ELSE 0
                    END)  AS apr_net,
                SUM(CASE
                      WHEN d_moy = 5 THEN cs_net_paid_inc_tax * cs_quantity
                      ELSE 0
                    END)  AS may_net,
                SUM(CASE
                      WHEN d_moy = 6 THEN cs_net_paid_inc_tax * cs_quantity
                      ELSE 0
                    END)  AS jun_net,
                SUM(CASE
                      WHEN d_moy = 7 THEN cs_net_paid_inc_tax * cs_quantity
                      ELSE 0
                    END)  AS jul_net,
                SUM(CASE
                      WHEN d_moy = 8 THEN cs_net_paid_inc_tax * cs_quantity
                      ELSE 0
                    END)  AS aug_net,
                SUM(CASE
                      WHEN d_moy = 9 THEN cs_net_paid_inc_tax * cs_quantity
                      ELSE 0
                    END)  AS sep_net,
                SUM(CASE
                      WHEN d_moy = 10 THEN cs_net_paid_inc_tax * cs_quantity
                      ELSE 0
                    END)  AS oct_net,
                SUM(CASE
                      WHEN d_moy = 11 THEN cs_net_paid_inc_tax * cs_quantity
                      ELSE 0
                    END)  AS nov_net,
                SUM(CASE
                      WHEN d_moy = 12 THEN cs_net_paid_inc_tax * cs_quantity
                      ELSE 0
                    END)  AS dec_net
         FROM   catalog_sales,
                warehouse,
                date_dim,
                time_dim,
                ship_mode
         WHERE  cs_warehouse_sk = w_warehouse_sk
                AND cs_sold_date_sk = d_date_sk
                AND cs_sold_time_sk = t_time_sk
                AND cs_ship_mode_sk = sm_ship_mode_sk
                AND d_year = 2001
                AND t_time BETWEEN 39920 AND 39920 + 28800
                AND sm_carrier IN ( 'LATVIAN', 'DHL' )
         GROUP  BY w_warehouse_name,
                   w_warehouse_sq_ft,
                   w_city,
                   w_county,
                   w_state,
                   w_country,
                   d_year)) x
GROUP  BY w_warehouse_name,
          w_warehouse_sq_ft,
          w_city,
          w_county,
          w_state,
          w_country,
          ship_carriers,
          YEAR
ORDER  BY w_warehouse_name;

-- query90.tpl
SELECT CAST(amc AS DECIMAL(15, 4)) / CAST(pmc AS DECIMAL(15, 4)) am_pm_ratio
FROM   (SELECT COUNT(*) amc
        FROM   web_sales,
               household_demographics,
               time_dim,
               web_page
        WHERE  ws_sold_time_sk = time_dim.t_time_sk
               AND ws_ship_hdemo_sk = household_demographics.hd_demo_sk
               AND ws_web_page_sk = web_page.wp_web_page_sk
               AND time_dim.t_hour BETWEEN 7 AND 7 + 1
               AND household_demographics.hd_dep_count = 0
               AND web_page.wp_char_count BETWEEN 5000 AND 5200) AT,
       (SELECT COUNT(*) pmc
        FROM   web_sales,
               household_demographics,
               time_dim,
               web_page
        WHERE  ws_sold_time_sk = time_dim.t_time_sk
               AND ws_ship_hdemo_sk = household_demographics.hd_demo_sk
               AND ws_web_page_sk = web_page.wp_web_page_sk
               AND time_dim.t_hour BETWEEN 17 AND 17 + 1
               AND household_demographics.hd_dep_count = 0
               AND web_page.wp_char_count BETWEEN 5000 AND 5200) pt
ORDER  BY am_pm_ratio;

-- query17.tpl
SELECT i_item_id,
       i_item_desc,
       s_state,
       COUNT(ss_quantity)                                        AS store_sales_quantitycount,
       AVG(ss_quantity)                                          AS store_sales_quantityave,
       STDDEV_SAMP(ss_quantity)                                  AS store_sales_quantitystdev,
       STDDEV_SAMP(ss_quantity) / AVG(ss_quantity)               AS store_sales_quantitycov,
       COUNT(sr_return_quantity)                                 as_store_returns_quantitycount,
       AVG(sr_return_quantity)                                   as_store_returns_quantityave,
       STDDEV_SAMP(sr_return_quantity)                           as_store_returns_quantitystdev,
       STDDEV_SAMP(sr_return_quantity) / AVG(sr_return_quantity) AS store_returns_quantitycov,
       COUNT(cs_quantity)                                        AS catalog_sales_quantitycount,
       AVG(cs_quantity)                                          AS catalog_sales_quantityave,
       STDDEV_SAMP(cs_quantity) / AVG(cs_quantity)               AS catalog_sales_quantitystdev,
       STDDEV_SAMP(cs_quantity) / AVG(cs_quantity)               AS catalog_sales_quantitycov
FROM   store_sales,
       store_returns,
       catalog_sales,
       date_dim d1,
       date_dim d2,
       date_dim d3,
       store,
       item
WHERE  d1.d_quarter_name = '2002Q1'
       AND d1.d_date_sk = ss_sold_date_sk
       AND i_item_sk = ss_item_sk
       AND s_store_sk = ss_store_sk
       AND ss_customer_sk = sr_customer_sk
       AND ss_item_sk = sr_item_sk
       AND ss_ticket_number = sr_ticket_number
       AND sr_returned_date_sk = d2.d_date_sk
       AND d2.d_quarter_name IN ( '2002Q1', '2002Q2', '2002Q3' )
       AND sr_customer_sk = cs_bill_customer_sk
       AND sr_item_sk = cs_item_sk
       AND cs_sold_date_sk = d3.d_date_sk
       AND d3.d_quarter_name IN ( '2002Q1', '2002Q2', '2002Q3' )
GROUP  BY i_item_id,
          i_item_desc,
          s_state
ORDER  BY i_item_id,
          i_item_desc,
          s_state;

-- query47.tpl
WITH v1
     AS (SELECT i_category,
                i_brand,
                s_store_name,
                s_company_name,
                d_year,
                d_moy,
                SUM(ss_sales_price)
                sum_sales
                   ,
                AVG(SUM(ss_sales_price)) OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name,
                   d_year) avg_monthly_sales,
                RANK() OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name ORDER BY d_year, d_moy)
                rn
         FROM   item,
                store_sales,
                date_dim,
                store
         WHERE  ss_item_sk = i_item_sk
                AND ss_sold_date_sk = d_date_sk
                AND ss_store_sk = s_store_sk
                AND ( d_year = 2001
                       OR ( d_year = 2001 - 1
                            AND d_moy = 12 )
                       OR ( d_year = 2001 + 1
                            AND d_moy = 1 ) )
         GROUP  BY i_category,
                   i_brand,
                   s_store_name,
                   s_company_name,
                   d_year,
                   d_moy),
     v2
     AS (SELECT v1.i_category,
                v1.i_brand,
                v1.s_store_name,
                v1.s_company_name,
                v1.d_year,
                v1.d_moy,
                v1.avg_monthly_sales,
                v1.sum_sales,
                v1_lag.sum_sales  psum,
                v1_lead.sum_sales nsum
         FROM   v1,
                v1 v1_lag,
                v1 v1_lead
         WHERE  v1.i_category = v1_lag.i_category
                AND v1.i_category = v1_lead.i_category
                AND v1.i_brand = v1_lag.i_brand
                AND v1.i_brand = v1_lead.i_brand
                AND v1.s_store_name = v1_lag.s_store_name
                AND v1.s_store_name = v1_lead.s_store_name
                AND v1.s_company_name = v1_lag.s_company_name
                AND v1.s_company_name = v1_lead.s_company_name
                AND v1.rn = v1_lag.rn + 1
                AND v1.rn = v1_lead.rn - 1)
SELECT *
FROM   v2
WHERE  d_year = 2001
       AND avg_monthly_sales > 0
       AND CASE
             WHEN avg_monthly_sales > 0 THEN ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales
             ELSE NULL
           END > 0.1
ORDER  BY sum_sales - avg_monthly_sales,
          s_store_name;

-- query95.tpl
WITH ws_wh
     AS (SELECT ws1.ws_order_number,
                ws1.ws_warehouse_sk wh1,
                ws2.ws_warehouse_sk wh2
         FROM   web_sales ws1,
                web_sales ws2
         WHERE  ws1.ws_order_number = ws2.ws_order_number
                AND ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
SELECT COUNT(DISTINCT ws_order_number) AS "order count",
       SUM(ws_ext_ship_cost)           AS "total shipping cost",
       SUM(ws_net_profit)              AS "total net profit"
FROM   web_sales ws1,
       date_dim,
       customer_address,
       web_site
WHERE  d_date BETWEEN '1999-3-01' AND ( CAST('1999-3-01' AS DATE) + 60 DAYS )
       AND ws1.ws_ship_date_sk = d_date_sk
       AND ws1.ws_ship_addr_sk = ca_address_sk
       AND ca_state = 'IN'
       AND ws1.ws_web_site_sk = web_site_sk
       AND web_company_name = 'pri'
       AND ws1.ws_order_number IN (SELECT ws_order_number
                                   FROM   ws_wh)
       AND ws1.ws_order_number IN (SELECT wr_order_number
                                   FROM   web_returns,
                                          ws_wh
                                   WHERE  wr_order_number = ws_wh.ws_order_number)
ORDER  BY COUNT(DISTINCT ws_order_number);

-- query92.tpl
SELECT SUM(ws_ext_discount_amt) AS "Excess Discount Amount"
FROM   web_sales,
       item,
       date_dim
WHERE  i_manufact_id = 675
       AND i_item_sk = ws_item_sk
       AND d_date BETWEEN '1998-01-30' AND ( CAST('1998-01-30' AS DATE) + 90 DAYS )
       AND d_date_sk = ws_sold_date_sk
       AND ws_ext_discount_amt > (SELECT 1.3 * AVG(ws_ext_discount_amt)
                                  FROM   web_sales,
                                         date_dim
                                  WHERE  ws_item_sk = i_item_sk
                                         AND d_date BETWEEN '1998-01-30' AND ( CAST('1998-01-30' AS DATE) + 90 DAYS )
                                         AND d_date_sk = ws_sold_date_sk)
ORDER  BY SUM(ws_ext_discount_amt);

-- query3.tpl
SELECT dt.d_year,
       item.i_brand_id         brand_id,
       item.i_brand            brand,
       SUM(ss_ext_sales_price) ext_price
FROM   date_dim dt,
       store_sales,
       item
WHERE  dt.d_date_sk = store_sales.ss_sold_date_sk
       AND store_sales.ss_item_sk = item.i_item_sk
       AND item.i_manufact_id = 741
       AND dt.d_moy = 11
GROUP  BY dt.d_year,
          item.i_brand,
          item.i_brand_id
ORDER  BY dt.d_year,
          ext_price DESC,
          brand_id;

-- query51.tpl
WITH web_v1
     AS (SELECT ws_item_sk item_sk,
                d_date,
                SUM(SUM(ws_sales_price)) OVER (PARTITION BY ws_item_sk ORDER BY d_date ROWS BETWEEN UNBOUNDED PRECEDING
                AND
                   CURRENT ROW)
                           cume_sales
         FROM   web_sales,
                date_dim
         WHERE  ws_sold_date_sk = d_date_sk
                AND d_year = 1998
                AND ws_item_sk IS NOT NULL
         GROUP  BY ws_item_sk,
                   d_date),
     store_v1
     AS (SELECT ss_item_sk item_sk,
                d_date,
                SUM(SUM(ss_sales_price)) OVER (PARTITION BY ss_item_sk ORDER BY d_date ROWS BETWEEN UNBOUNDED PRECEDING
                AND
                   CURRENT ROW)
                           cume_sales
         FROM   store_sales,
                date_dim
         WHERE  ss_sold_date_sk = d_date_sk
                AND d_year = 1998
                AND ss_item_sk IS NOT NULL
         GROUP  BY ss_item_sk,
                   d_date)
SELECT *
FROM   (SELECT item_sk,
               d_date,
               web_sales,
               store_sales,
               MAX(web_sales) OVER (PARTITION BY item_sk ORDER BY d_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT
               ROW)
               web_cumulative,
               MAX(store_sales) OVER (PARTITION BY item_sk ORDER BY d_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT
               ROW)
               store_cumulative
        FROM   (SELECT CASE
                         WHEN web.item_sk IS NOT NULL THEN web.item_sk
                         ELSE store.item_sk
                       END              item_sk,
                       CASE
                         WHEN web.d_date IS NOT NULL THEN web.d_date
                         ELSE store.d_date
                       END              d_date,
                       web.cume_sales   web_sales,
                       store.cume_sales store_sales
                FROM   web_v1 web
                       FULL OUTER JOIN store_v1 store
                         ON ( web.item_sk = store.item_sk
                              AND web.d_date = store.d_date ))x)y
WHERE  web_cumulative > store_cumulative
ORDER  BY item_sk,
          d_date;

-- query35.tpl
SELECT ca_state,
       cd_gender,
       cd_marital_status,
       COUNT(*) cnt1,
       MIN(cd_dep_count),
       MAX(cd_dep_count),
       AVG(cd_dep_count),
       cd_dep_employed_count,
       COUNT(*) cnt2,
       MIN(cd_dep_employed_count),
       MAX(cd_dep_employed_count),
       AVG(cd_dep_employed_count),
       cd_dep_college_count,
       COUNT(*) cnt3,
       MIN(cd_dep_college_count),
       MAX(cd_dep_college_count),
       AVG(cd_dep_college_count)
FROM   customer c,
       customer_address ca,
       customer_demographics
WHERE  c.c_current_addr_sk = ca.ca_address_sk
       AND cd_demo_sk = c.c_current_cdemo_sk
       AND EXISTS (SELECT *
                   FROM   store_sales,
                          date_dim
                   WHERE  c.c_customer_sk = ss_customer_sk
                          AND ss_sold_date_sk = d_date_sk
                          AND d_year = 1999
                          AND d_qoy < 4)
       AND ( EXISTS (SELECT *
                     FROM   web_sales,
                            date_dim
                     WHERE  c.c_customer_sk = ws_bill_customer_sk
                            AND ws_sold_date_sk = d_date_sk
                            AND d_year = 1999
                            AND d_qoy < 4)
              OR EXISTS (SELECT *
                         FROM   catalog_sales,
                                date_dim
                         WHERE  c.c_customer_sk = cs_ship_customer_sk
                                AND cs_sold_date_sk = d_date_sk
                                AND d_year = 1999
                                AND d_qoy < 4) )
GROUP  BY ca_state,
          cd_gender,
          cd_marital_status,
          cd_dep_count,
          cd_dep_employed_count,
          cd_dep_college_count
ORDER  BY ca_state,
          cd_gender,
          cd_marital_status,
          cd_dep_count,
          cd_dep_employed_count,
          cd_dep_college_count;

-- query49.tpl
SELECT 'web' AS channel,
       web.item,
       web.return_ratio,
       web.return_rank,
       web.currency_rank
FROM   (SELECT item,
               return_ratio,
               currency_ratio,
               RANK() OVER (ORDER BY return_ratio)   AS return_rank,
               RANK() OVER (ORDER BY currency_ratio) AS currency_rank
        FROM   (SELECT ws.ws_item_sk
                               AS
                               item,
                       ( CAST(SUM(COALESCE(wr.wr_return_quantity, 0)) AS DEC(15, 4)) / CAST(
                         SUM(COALESCE(ws.ws_quantity, 0)) AS DEC(15, 4)) )
                                                                      AS return_ratio,
                       ( CAST(SUM(COALESCE(wr.wr_return_amt, 0)) AS DEC(15, 4)) / CAST(
                         SUM(COALESCE(ws.ws_net_paid, 0)) AS DEC(15, 4))
                               ) AS
                       currency_ratio
                FROM   web_sales ws
                       LEFT OUTER JOIN web_returns wr
                         ON ( ws.ws_order_number = wr.wr_order_number
                              AND ws.ws_item_sk = wr.wr_item_sk ),
                       date_dim
                WHERE  wr.wr_return_amt > 10000
                       AND ws.ws_net_profit > 1
                       AND ws.ws_net_paid > 0
                       AND ws.ws_quantity > 0
                       AND ws_sold_date_sk = d_date_sk
                       AND d_year = 2002
                       AND d_moy = 11
                GROUP  BY ws.ws_item_sk) in_web) web
WHERE  ( web.return_rank <= 10
          OR web.currency_rank <= 10 )
UNION
SELECT 'catalog' AS channel,
       catalog.item,
       catalog.return_ratio,
       catalog.return_rank,
       catalog.currency_rank
FROM   (SELECT item,
               return_ratio,
               currency_ratio,
               RANK() OVER (ORDER BY return_ratio)   AS return_rank,
               RANK() OVER (ORDER BY currency_ratio) AS currency_rank
        FROM   (SELECT cs.cs_item_sk
                               AS
                               item,
                       ( CAST(SUM(COALESCE(cr.cr_return_quantity, 0)) AS DEC(15, 4)) / CAST(
                         SUM(COALESCE(cs.cs_quantity, 0)) AS DEC(15, 4)) )
                                                                         AS return_ratio,
                       ( CAST(SUM(COALESCE(cr.cr_return_amount, 0)) AS DEC(15, 4)) / CAST(
                         SUM(COALESCE(cs.cs_net_paid, 0)) AS DEC(15, 4)) ) AS
                       currency_ratio
                FROM   catalog_sales cs
                       LEFT OUTER JOIN catalog_returns cr
                         ON ( cs.cs_order_number = cr.cr_order_number
                              AND cs.cs_item_sk = cr.cr_item_sk ),
                       date_dim
                WHERE  cr.cr_return_amount > 10000
                       AND cs.cs_net_profit > 1
                       AND cs.cs_net_paid > 0
                       AND cs.cs_quantity > 0
                       AND cs_sold_date_sk = d_date_sk
                       AND d_year = 2002
                       AND d_moy = 11
                GROUP  BY cs.cs_item_sk) in_cat) catalog
WHERE  ( catalog.return_rank <= 10
          OR catalog.currency_rank <= 10 )
UNION
SELECT 'store' AS channel,
       store.item,
       store.return_ratio,
       store.return_rank,
       store.currency_rank
FROM   (SELECT item,
               return_ratio,
               currency_ratio,
               RANK() OVER (ORDER BY return_ratio)   AS return_rank,
               RANK() OVER (ORDER BY currency_ratio) AS currency_rank
        FROM   (SELECT sts.ss_item_sk
                               AS
                               item,
                       ( CAST(SUM(COALESCE(sr.sr_return_quantity, 0)) AS DEC(15, 4)) / CAST(
                         SUM(COALESCE(sts.ss_quantity, 0)) AS DEC(15, 4)) )
                                                                       AS return_ratio,
                       ( CAST(SUM(COALESCE(sr.sr_return_amt, 0)) AS DEC(15, 4)) / CAST(
                         SUM(COALESCE(sts.ss_net_paid, 0)) AS DEC(15, 4))
                               ) AS
                       currency_ratio
                FROM   store_sales sts
                       LEFT OUTER JOIN store_returns sr
                         ON ( sts.ss_ticket_number = sr.sr_ticket_number
                              AND sts.ss_item_sk = sr.sr_item_sk ),
                       date_dim
                WHERE  sr.sr_return_amt > 10000
                       AND sts.ss_net_profit > 1
                       AND sts.ss_net_paid > 0
                       AND sts.ss_quantity > 0
                       AND ss_sold_date_sk = d_date_sk
                       AND d_year = 2002
                       AND d_moy = 11
                GROUP  BY sts.ss_item_sk) in_store) store
WHERE  ( store.return_rank <= 10
          OR store.currency_rank <= 10 )
ORDER  BY 1,
          4,
          5;

-- query9.tpl
SELECT CASE
         WHEN (SELECT COUNT(*)
               FROM   store_sales
               WHERE  ss_quantity BETWEEN 1 AND 20) > 0.001 * 219075 THEN (SELECT AVG(ss_ext_list_price)
                                                                           FROM   store_sales
                                                                           WHERE  ss_quantity BETWEEN 1 AND 20)
         ELSE (SELECT AVG(ss_net_profit)
               FROM   store_sales
               WHERE  ss_quantity BETWEEN 1 AND 20)
       END bucket1,
       CASE
         WHEN (SELECT COUNT(*)
               FROM   store_sales
               WHERE  ss_quantity BETWEEN 21 AND 40) > 0.001 * 55037 THEN (SELECT AVG(ss_ext_list_price)
                                                                           FROM   store_sales
                                                                           WHERE  ss_quantity BETWEEN 21 AND 40)
         ELSE (SELECT AVG(ss_net_profit)
               FROM   store_sales
               WHERE  ss_quantity BETWEEN 21 AND 40)
       END bucket2,
       CASE
         WHEN (SELECT COUNT(*)
               FROM   store_sales
               WHERE  ss_quantity BETWEEN 41 AND 60) > 0.001 * 28293 THEN (SELECT AVG(ss_ext_list_price)
                                                                           FROM   store_sales
                                                                           WHERE  ss_quantity BETWEEN 41 AND 60)
         ELSE (SELECT AVG(ss_net_profit)
               FROM   store_sales
               WHERE  ss_quantity BETWEEN 41 AND 60)
       END bucket3,
       CASE
         WHEN (SELECT COUNT(*)
               FROM   store_sales
               WHERE  ss_quantity BETWEEN 61 AND 80) > 0.001 * 104204 THEN (SELECT AVG(ss_ext_list_price)
                                                                            FROM   store_sales
                                                                            WHERE  ss_quantity BETWEEN 61 AND 80)
         ELSE (SELECT AVG(ss_net_profit)
               FROM   store_sales
               WHERE  ss_quantity BETWEEN 61 AND 80)
       END bucket4,
       CASE
         WHEN (SELECT COUNT(*)
               FROM   store_sales
               WHERE  ss_quantity BETWEEN 81 AND 100) > 0.001 * 141976 THEN (SELECT AVG(ss_ext_list_price)
                                                                             FROM   store_sales
                                                                             WHERE  ss_quantity BETWEEN 81 AND 100)
         ELSE (SELECT AVG(ss_net_profit)
               FROM   store_sales
               WHERE  ss_quantity BETWEEN 81 AND 100)
       END bucket5
FROM   reason
WHERE  r_reason_sk = 1;

-- query31.tpl
WITH ss
     AS (SELECT ca_county,
                d_qoy,
                d_year,
                SUM(ss_ext_sales_price) AS store_sales
         FROM   store_sales,
                date_dim,
                customer_address
         WHERE  ss_sold_date_sk = d_date_sk
                AND ss_addr_sk = ca_address_sk
         GROUP  BY ca_county,
                   d_qoy,
                   d_year),
     ws
     AS (SELECT ca_county,
                d_qoy,
                d_year,
                SUM(ws_ext_sales_price) AS web_sales
         FROM   web_sales,
                date_dim,
                customer_address
         WHERE  ws_sold_date_sk = d_date_sk
                AND ws_bill_addr_sk = ca_address_sk
         GROUP  BY ca_county,
                   d_qoy,
                   d_year)
SELECT /* tt */ ss1.ca_county,
                ss1.d_year,
                ws2.web_sales / ws1.web_sales     web_q1_q2_increase,
                ss2.store_sales / ss1.store_sales store_q1_q2_increase,
                ws3.web_sales / ws2.web_sales     web_q2_q3_increase,
                ss3.store_sales / ss2.store_sales store_q2_q3_increase
FROM   ss ss1,
       ss ss2,
       ss ss3,
       ws ws1,
       ws ws2,
       ws ws3
WHERE  ss1.d_qoy = 1
       AND ss1.d_year = 2001
       AND ss1.ca_county = ss2.ca_county
       AND ss2.d_qoy = 2
       AND ss2.d_year = 2001
       AND ss2.ca_county = ss3.ca_county
       AND ss3.d_qoy = 3
       AND ss3.d_year = 2001
       AND ss1.ca_county = ws1.ca_county
       AND ws1.d_qoy = 1
       AND ws1.d_year = 2001
       AND ws1.ca_county = ws2.ca_county
       AND ws2.d_qoy = 2
       AND ws2.d_year = 2001
       AND ws1.ca_county = ws3.ca_county
       AND ws3.d_qoy = 3
       AND ws3.d_year = 2001
       AND CASE
             WHEN ws1.web_sales > 0 THEN ws2.web_sales / ws1.web_sales
             ELSE NULL
           END > CASE
                   WHEN ss1.store_sales > 0 THEN ss2.store_sales / ss1.store_sales
                   ELSE NULL
                 END
       AND CASE
             WHEN ws2.web_sales > 0 THEN ws3.web_sales / ws2.web_sales
             ELSE NULL
           END > CASE
                   WHEN ss2.store_sales > 0 THEN ss3.store_sales / ss2.store_sales
                   ELSE NULL
                 END
ORDER  BY ss1.ca_county;

-- query11.tpl
WITH year_total
     AS (SELECT c_customer_id                                customer_id,
                c_first_name                                 customer_first_name,
                c_last_name                                  customer_last_name,
                c_preferred_cust_flag,
                c_birth_country,
                c_login,
                c_email_address,
                d_year                                       dyear,
                SUM(ss_ext_list_price - ss_ext_discount_amt) year_total,
                's'                                          sale_type
         FROM   customer,
                store_sales,
                date_dim
         WHERE  c_customer_sk = ss_customer_sk
                AND ss_sold_date_sk = d_date_sk
         GROUP  BY c_customer_id,
                   c_first_name,
                   c_last_name,
                   d_year,
                   c_preferred_cust_flag,
                   c_birth_country,
                   c_login,
                   c_email_address,
                   d_year
         UNION ALL
         SELECT c_customer_id                                customer_id,
                c_first_name                                 customer_first_name,
                c_last_name                                  customer_last_name,
                c_preferred_cust_flag,
                c_birth_country,
                c_login,
                c_email_address,
                d_year                                       dyear,
                SUM(ws_ext_list_price - ws_ext_discount_amt) year_total,
                'w'                                          sale_type
         FROM   customer,
                web_sales,
                date_dim
         WHERE  c_customer_sk = ws_bill_customer_sk
                AND ws_sold_date_sk = d_date_sk
         GROUP  BY c_customer_id,
                   c_first_name,
                   c_last_name,
                   c_preferred_cust_flag,
                   c_birth_country,
                   c_login,
                   c_email_address,
                   d_year)
SELECT t_s_secyear.customer_id,
       t_s_secyear.customer_first_name,
       t_s_secyear.customer_last_name,
       t_s_secyear.c_preferred_cust_flag,
       t_s_secyear.c_birth_country,
       t_s_secyear.c_login
FROM   year_total t_s_firstyear,
       year_total t_s_secyear,
       year_total t_w_firstyear,
       year_total t_w_secyear
WHERE  t_s_secyear.customer_id = t_s_firstyear.customer_id
       AND t_s_firstyear.customer_id = t_w_secyear.customer_id
       AND t_s_firstyear.customer_id = t_w_firstyear.customer_id
       AND t_s_firstyear.sale_type = 's'
       AND t_w_firstyear.sale_type = 'w'
       AND t_s_secyear.sale_type = 's'
       AND t_w_secyear.sale_type = 'w'
       AND t_s_firstyear.dyear = 1999
       AND t_s_secyear.dyear = 1999 + 1
       AND t_w_firstyear.dyear = 1999
       AND t_w_secyear.dyear = 1999 + 1
       AND t_s_firstyear.year_total > 0
       AND t_w_firstyear.year_total > 0
       AND CASE
             WHEN t_w_firstyear.year_total > 0 THEN t_w_secyear.year_total / t_w_firstyear.year_total
             ELSE NULL
           END > CASE
                   WHEN t_s_firstyear.year_total > 0 THEN t_s_secyear.year_total / t_s_firstyear.year_total
                   ELSE NULL
                 END
ORDER  BY t_s_secyear.customer_id,
          t_s_secyear.customer_first_name,
          t_s_secyear.customer_last_name,
          t_s_secyear.c_preferred_cust_flag,
          t_s_secyear.c_birth_country,
          t_s_secyear.c_login;

-- query93.tpl
SELECT ss_customer_sk,
       SUM(act_sales) sumsales
FROM   (SELECT ss_item_sk,
               ss_ticket_number,
               ss_customer_sk,
               CASE
                 WHEN sr_return_quantity IS NOT NULL THEN ( ss_quantity - sr_return_quantity ) * ss_sales_price
                 ELSE ( ss_quantity * ss_sales_price )
               END act_sales
        FROM   store_sales
               LEFT OUTER JOIN store_returns
                 ON ( sr_item_sk = ss_item_sk
                      AND sr_ticket_number = ss_ticket_number ),
               reason
        WHERE  sr_reason_sk = r_reason_sk
               AND r_reason_desc = 'reason 67') t
GROUP  BY ss_customer_sk
ORDER  BY sumsales,
          ss_customer_sk;

-- query29.tpl
SELECT i_item_id,
       i_item_desc,
       s_store_id,
       s_store_name,
       SUM(ss_quantity)        AS store_sales_quantity,
       SUM(sr_return_quantity) AS store_returns_quantity,
       SUM(cs_quantity)        AS catalog_sales_quantity
FROM   store_sales,
       store_returns,
       catalog_sales,
       date_dim d1,
       date_dim d2,
       date_dim d3,
       store,
       item
WHERE  d1.d_moy = 4
       AND d1.d_year = 1998
       AND d1.d_date_sk = ss_sold_date_sk
       AND i_item_sk = ss_item_sk
       AND s_store_sk = ss_store_sk
       AND ss_customer_sk = sr_customer_sk
       AND ss_item_sk = sr_item_sk
       AND ss_ticket_number = sr_ticket_number
       AND sr_returned_date_sk = d2.d_date_sk
       AND d2.d_moy BETWEEN 4 AND 4 + 3
       AND d2.d_year = 1998
       AND sr_customer_sk = cs_bill_customer_sk
       AND sr_item_sk = cs_item_sk
       AND cs_sold_date_sk = d3.d_date_sk
       AND d3.d_year IN ( 1998, 1998 + 1, 1998 + 2 )
GROUP  BY i_item_id,
          i_item_desc,
          s_store_id,
          s_store_name
ORDER  BY i_item_id,
          i_item_desc,
          s_store_id,
          s_store_name;

-- query38.tpl
SELECT COUNT(*)
FROM   (SELECT DISTINCT c_last_name,
                        c_first_name,
                        d_date
        FROM   store_sales,
               date_dim,
               customer
        WHERE  store_sales.ss_sold_date_sk = date_dim.d_date_sk
               AND store_sales.ss_customer_sk = customer.c_customer_sk
               AND d_year = 1999
        INTERSECT
        SELECT DISTINCT c_last_name,
                        c_first_name,
                        d_date
        FROM   catalog_sales,
               date_dim,
               customer
        WHERE  catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
               AND catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
               AND d_year = 1999
        INTERSECT
        SELECT DISTINCT c_last_name,
                        c_first_name,
                        d_date
        FROM   web_sales,
               date_dim,
               customer
        WHERE  web_sales.ws_sold_date_sk = date_dim.d_date_sk
               AND web_sales.ws_bill_customer_sk = customer.c_customer_sk
               AND d_year = 1999) hot_cust;

-- query22.tpl
SELECT i_product_name,
       i_brand,
       i_class,
       i_category,
       AVG(inv_quantity_on_hand) qoh
FROM   inventory,
       date_dim,
       item,
       warehouse
WHERE  inv_date_sk = d_date_sk
       AND inv_item_sk = i_item_sk
       AND inv_warehouse_sk = w_warehouse_sk
       AND d_year = 1998
GROUP  BY ROLLUP( i_product_name, i_brand, i_class, i_category )
ORDER  BY qoh,
          i_product_name,
          i_brand,
          i_class,
          i_category;

-- query89.tpl
SELECT *
FROM  (SELECT i_category,
              i_class,
              i_brand,
              s_store_name,
              s_company_name,
              d_moy,
              SUM(ss_sales_price)                                                                            sum_sales,
              AVG(SUM(ss_sales_price)) OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name)
              avg_monthly_sales
       FROM   item,
              store_sales,
              date_dim,
              store
       WHERE  ss_item_sk = i_item_sk
              AND ss_sold_date_sk = d_date_sk
              AND ss_store_sk = s_store_sk
              AND d_year IN ( 1999 )
              AND ( ( i_category IN ( 'Electronics', 'Sports', 'Shoes' )
                      AND i_class IN ( 'dvd/vcr players', 'guns', 'kids' ) )
                     OR ( i_category IN ( 'Music', 'Women', 'Men' )
                          AND i_class IN ( 'pop', 'maternity', 'shirts' ) ) )
       GROUP  BY i_category,
                 i_class,
                 i_brand,
                 s_store_name,
                 s_company_name,
                 d_moy) tmp1
WHERE  CASE
         WHEN ( avg_monthly_sales <> 0 ) THEN ( ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales )
         ELSE NULL
       END > 0.1
ORDER  BY sum_sales - avg_monthly_sales,
          s_store_name;

-- query15.tpl
SELECT ca_zip,
       SUM(cs_sales_price)
FROM   catalog_sales,
       customer,
       customer_address,
       date_dim
WHERE  cs_bill_customer_sk = c_customer_sk
       AND c_current_addr_sk = ca_address_sk
       AND ( SUBSTR(ca_zip, 1, 5) IN ( '85669', '86197', '88274', '83405',
                                       '86475', '85392', '85460', '80348', '81792' )
              OR ca_state IN ( 'CA', 'WA', 'GA' )
              OR cs_sales_price > 500 )
       AND cs_sold_date_sk = d_date_sk
       AND d_qoy = 1
       AND d_year = 1998
GROUP  BY ca_zip
ORDER  BY ca_zip;

-- query6.tpl
SELECT a.ca_state state,
       COUNT(*)   cnt
FROM   customer_address a,
       customer c,
       store_sales s,
       date_dim d,
       item i
WHERE  a.ca_address_sk = c.c_current_addr_sk
       AND c.c_customer_sk = s.ss_customer_sk
       AND s.ss_sold_date_sk = d.d_date_sk
       AND s.ss_item_sk = i.i_item_sk
       AND d.d_month_seq = (SELECT DISTINCT ( d_month_seq )
                            FROM   date_dim
                            WHERE  d_year = 2000
                                   AND d_moy = 2)
       AND i.i_current_price > 1.2 * (SELECT AVG(j.i_current_price)
                                      FROM   item j
                                      WHERE  j.i_category = i.i_category)
GROUP  BY a.ca_state
HAVING COUNT(*) >= 10
ORDER  BY cnt;

-- query52.tpl
SELECT dt.d_year,
       item.i_brand_id         brand_id,
       item.i_brand            brand,
       SUM(ss_ext_sales_price) ext_price
FROM   date_dim dt,
       store_sales,
       item
WHERE  dt.d_date_sk = store_sales.ss_sold_date_sk
       AND store_sales.ss_item_sk = item.i_item_sk
       AND item.i_manager_id = 1
       AND dt.d_moy = 11
       AND dt.d_year = 1998
GROUP  BY dt.d_year,
          item.i_brand,
          item.i_brand_id
ORDER  BY dt.d_year,
          ext_price DESC,
          brand_id;

-- query50.tpl
SELECT s_store_name,
       s_company_id,
       s_street_number,
       s_street_name,
       s_street_type,
       s_suite_number,
       s_city,
       s_county,
       s_state,
       s_zip,
       SUM(CASE
             WHEN ( sr_returned_date_sk - ss_sold_date_sk <= 30 ) THEN 1
             ELSE 0
           END) AS "30 days",
       SUM(CASE
             WHEN ( sr_returned_date_sk - ss_sold_date_sk > 30 )
                  AND ( sr_returned_date_sk - ss_sold_date_sk <= 60 ) THEN 1
             ELSE 0
           END) AS "31-60 days",
       SUM(CASE
             WHEN ( sr_returned_date_sk - ss_sold_date_sk > 60 )
                  AND ( sr_returned_date_sk - ss_sold_date_sk <= 90 ) THEN 1
             ELSE 0
           END) AS "61-90 days",
       SUM(CASE
             WHEN ( sr_returned_date_sk - ss_sold_date_sk > 90 )
                  AND ( sr_returned_date_sk - ss_sold_date_sk <= 120 ) THEN 1
             ELSE 0
           END) AS "91-120 days",
       SUM(CASE
             WHEN ( sr_returned_date_sk - ss_sold_date_sk > 120 ) THEN 1
             ELSE 0
           END) AS ">120 days"
FROM   store_sales,
       store_returns,
       store,
       date_dim d1,
       date_dim d2
WHERE  d2.d_year = 1998
       AND d2.d_moy = 8
       AND ss_ticket_number = sr_ticket_number
       AND ss_item_sk = sr_item_sk
       AND ss_sold_date_sk = d1.d_date_sk
       AND sr_returned_date_sk = d2.d_date_sk
       AND ss_customer_sk = sr_customer_sk
       AND ss_store_sk = s_store_sk
GROUP  BY s_store_name,
          s_company_id,
          s_street_number,
          s_street_name,
          s_street_type,
          s_suite_number,
          s_city,
          s_county,
          s_state,
          s_zip
ORDER  BY s_store_name,
          s_company_id,
          s_street_number,
          s_street_name,
          s_street_type,
          s_suite_number,
          s_city,
          s_county,
          s_state,
          s_zip;

-- query42.tpl
SELECT dt.d_year,
       item.i_category_id,
       item.i_category,
       SUM(ss_ext_sales_price)
FROM   date_dim dt,
       store_sales,
       item
WHERE  dt.d_date_sk = store_sales.ss_sold_date_sk
       AND store_sales.ss_item_sk = item.i_item_sk
       AND item.i_manager_id = 1
       AND dt.d_moy = 12
       AND dt.d_year = 1998
GROUP  BY dt.d_year,
          item.i_category_id,
          item.i_category
ORDER  BY SUM(ss_ext_sales_price) DESC,
          dt.d_year,
          item.i_category_id,
          item.i_category;

-- query41.tpl
SELECT DISTINCT( i_product_name )
FROM   item i1
WHERE  i_manufact_id BETWEEN 844 AND 844 + 40
       AND (SELECT COUNT(*) AS item_cnt
            FROM   item
            WHERE  ( i_manufact = i1.i_manufact
                     AND ( ( i_category = 'Women'
                             AND ( i_color = 'dark'
                                    OR i_color = 'burnished' )
                             AND ( i_units = 'Oz'
                                    OR i_units = 'Pallet' )
                             AND ( i_size = 'small'
                                    OR i_size = 'N/A' ) )
                            OR ( i_category = 'Women'
                                 AND ( i_color = 'blush'
                                        OR i_color = 'white' )
                                 AND ( i_units = 'Box'
                                        OR i_units = 'Bundle' )
                                 AND ( i_size = 'petite'
                                        OR i_size = 'medium' ) )
                            OR ( i_category = 'Men'
                                 AND ( i_color = 'pink'
                                        OR i_color = 'rose' )
                                 AND ( i_units = 'Bunch'
                                        OR i_units = 'Pound' )
                                 AND ( i_size = 'extra large'
                                        OR i_size = 'large' ) )
                            OR ( i_category = 'Men'
                                 AND ( i_color = 'orchid'
                                        OR i_color = 'chocolate' )
                                 AND ( i_units = 'Unknown'
                                        OR i_units = 'Carton' )
                                 AND ( i_size = 'small'
                                        OR i_size = 'N/A' ) ) ) )
                    OR ( i_manufact = i1.i_manufact
                         AND ( ( i_category = 'Women'
                                 AND ( i_color = 'papaya'
                                        OR i_color = 'cornsilk' )
                                 AND ( i_units = 'Cup'
                                        OR i_units = 'Case' )
                                 AND ( i_size = 'small'
                                        OR i_size = 'N/A' ) )
                                OR ( i_category = 'Women'
                                     AND ( i_color = 'red'
                                            OR i_color = 'antique' )
                                     AND ( i_units = 'Lb'
                                            OR i_units = 'Dram' )
                                     AND ( i_size = 'petite'
                                            OR i_size = 'medium' ) )
                                OR ( i_category = 'Men'
                                     AND ( i_color = 'beige'
                                            OR i_color = 'metallic' )
                                     AND ( i_units = 'N/A'
                                            OR i_units = 'Tbl' )
                                     AND ( i_size = 'extra large'
                                            OR i_size = 'large' ) )
                                OR ( i_category = 'Men'
                                     AND ( i_color = 'slate'
                                            OR i_color = 'dim' )
                                     AND ( i_units = 'Dozen'
                                            OR i_units = 'Ton' )
                                     AND ( i_size = 'small'
                                            OR i_size = 'N/A' ) ) ) )) > 0
ORDER  BY i_product_name;

-- query8.tpl
SELECT s_store_name,
       SUM(ss_net_profit)
FROM   store_sales,
       date_dim,
       store,
       (SELECT ca_zip
        FROM   ((SELECT SUBSTR(ca_zip, 1, 5) ca_zip
                 FROM   customer_address
                 WHERE  SUBSTR(ca_zip, 1, 5) IN ( '28770', '22406', '84990', '37601',
                                                  '12148', '16006', '48732', '14368',
                                                  '15447', '12262', '70483', '37463',
                                                  '15631', '17727', '10225', '59176',
                                                  '37668', '65776', '96083', '90946',
                                                  '18674', '73506', '13509', '32137',
                                                  '33329', '50534', '38862', '42996',
                                                  '90592', '99213', '71363', '72890',
                                                  '33471', '14265', '61953', '81674',
                                                  '23523', '13291', '76248', '13250',
                                                  '83607', '58749', '27117', '73867',
                                                  '88501', '13213', '36285', '16498',
                                                  '75897', '85038', '79615', '29044',
                                                  '46745', '85381', '97906', '25235',
                                                  '41927', '13652', '20504', '22254',
                                                  '28616', '75431', '98243', '96508',
                                                  '43266', '27218', '84385', '30353',
                                                  '24834', '93747', '64144', '92994',
                                                  '27098', '93158', '56997', '87436',
                                                  '27931', '27487', '65275', '55960',
                                                  '68863', '66290', '29308', '11111',
                                                  '89041', '40351', '83542', '96502',
                                                  '73055', '76810', '53337', '89572',
                                                  '37471', '11257', '90071', '42520',
                                                  '15877', '75323', '42374', '36082',
                                                  '15023', '42900', '70892', '18404',
                                                  '46638', '94480', '17423', '31700',
                                                  '52142', '54785', '86220', '14887',
                                                  '50307', '67866', '49674', '70436',
                                                  '45055', '84774', '32385', '72038',
                                                  '60421', '23462', '11446', '89945',
                                                  '98404', '65478', '20749', '11845',
                                                  '89409', '59193', '57687', '31236',
                                                  '33530', '67003', '89889', '33164',
                                                  '12292', '60881', '32203', '71270',
                                                  '14911', '18844', '18397', '26767',
                                                  '39437', '92108', '17246', '53357',
                                                  '74612', '53120', '68632', '92018',
                                                  '14345', '98111', '65622', '54706',
                                                  '44130', '10562', '78330', '69194',
                                                  '46593', '15627', '14535', '44655',
                                                  '78184', '88106', '57470', '98878',
                                                  '16415', '64353', '57172', '40824',
                                                  '12324', '52012', '74349', '80323',
                                                  '98734', '70474', '59159', '51115',
                                                  '15003', '19908', '18966', '57066',
                                                  '55696', '85300', '40581', '39259',
                                                  '15304', '54812', '40369', '78396',
                                                  '58585', '80079', '68927', '69080',
                                                  '45340', '30410', '23271', '23904',
                                                  '63204', '45908', '54573', '90070',
                                                  '68085', '34418', '73265', '70679',
                                                  '33661', '45403', '62985', '12823',
                                                  '33928', '97817', '63093', '37708',
                                                  '21009', '40023', '17744', '45052',
                                                  '47249', '92933', '59027', '45762',
                                                  '23371', '88295', '35445', '77562',
                                                  '19072', '18946', '14501', '69144',
                                                  '10053', '60091', '10314', '77319',
                                                  '15961', '19445', '83317', '64805',
                                                  '41996', '76825', '13170', '15264',
                                                  '83225', '87248', '27749', '39882',
                                                  '92688', '81484', '44811', '54208',
                                                  '55926', '46237', '55279', '98696',
                                                  '49830', '60510', '24261', '90107',
                                                  '94192', '41609', '77937', '58737',
                                                  '28892', '38216', '22861', '92818',
                                                  '95022', '59181', '51971', '42538',
                                                  '20625', '61787', '43567', '20148',
                                                  '79376', '20804', '62246', '49326',
                                                  '34819', '66972', '15524', '27116',
                                                  '57341', '47760', '33953', '37850',
                                                  '57830', '18743', '14182', '12789',
                                                  '18557', '59696', '96434', '11101',
                                                  '76340', '12011', '46097', '27759',
                                                  '24084', '57428', '30971', '14719',
                                                  '82718', '78582', '45030', '91113',
                                                  '33945', '71368', '56369', '15129',
                                                  '96700', '49947', '11505', '82544',
                                                  '69959', '75495', '90786', '49227',
                                                  '80358', '79028', '27643', '16330',
                                                  '47496', '16775', '96139', '39035',
                                                  '77772', '93128', '36485', '95729',
                                                  '10519', '54218', '43962', '42289',
                                                  '35895', '34201', '54832', '30179',
                                                  '55376', '46077', '87339', '43883',
                                                  '30289', '36111', '14359', '89298',
                                                  '20746', '64670', '83780', '36901',
                                                  '11968', '44193', '87950', '61889',
                                                  '41788', '35049', '34862', '10379',
                                                  '36987', '20434', '26946', '87414',
                                                  '29553', '96117', '90604', '33734',
                                                  '11414', '16206', '15583', '45042',
                                                  '48182', '74138', '10101', '58855',
                                                  '17133', '79043', '37181', '36630',
                                                  '20262', '55210', '62061', '71395',
                                                  '32984', '64643', '43620', '92523',
                                                  '54314', '98650', '75682', '67356',
                                                  '59497', '58949', '62109', '88703',
                                                  '57638', '92292', '20159', '38548' ))
                INTERSECT
                (SELECT ca_zip
                 FROM   (SELECT SUBSTR(ca_zip, 1, 5) ca_zip,
                                COUNT(*)             cnt
                         FROM   customer_address,
                                customer
                         WHERE  ca_address_sk = c_current_addr_sk
                                AND c_preferred_cust_flag = 'Y'
                         GROUP  BY ca_zip
                         HAVING COUNT(*) > 10)a1))a2) v1
WHERE  ss_store_sk = s_store_sk
       AND ss_sold_date_sk = d_date_sk
       AND d_qoy = 1
       AND d_year = 2001
       AND ( SUBSTR(s_zip, 1, 2) = SUBSTR(v1.ca_zip, 1, 2) )
GROUP  BY s_store_name
ORDER  BY s_store_name;

-- query12.tpl
SELECT i_item_desc,
       i_category,
       i_class,
       i_current_price,
       SUM(ws_ext_sales_price)                                                                  AS itemrevenue,
       SUM(ws_ext_sales_price) * 100 / SUM(SUM(ws_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio
FROM   web_sales,
       item,
       date_dim
WHERE  ws_item_sk = i_item_sk
       AND i_category IN ( 'Men', 'Women', 'Jewelry' )
       AND ws_sold_date_sk = d_date_sk
       AND d_date BETWEEN CAST('1999-04-06' AS DATE) AND ( CAST('1999-04-06' AS DATE) + 30 DAYS )
GROUP  BY i_item_id,
          i_item_desc,
          i_category,
          i_class,
          i_current_price
ORDER  BY i_category,
          i_class,
          i_item_id,
          i_item_desc,
          revenueratio;

-- query20.tpl
SELECT i_item_desc,
       i_category,
       i_class,
       i_current_price,
       SUM(cs_ext_sales_price)                                                                  AS itemrevenue,
       SUM(cs_ext_sales_price) * 100 / SUM(SUM(cs_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio
FROM   catalog_sales,
       item,
       date_dim
WHERE  cs_item_sk = i_item_sk
       AND i_category IN ( 'Books', 'Jewelry', 'Children' )
       AND cs_sold_date_sk = d_date_sk
       AND d_date BETWEEN CAST('1998-03-26' AS DATE) AND ( CAST('1998-03-26' AS DATE) + 30 DAYS )
GROUP  BY i_item_id,
          i_item_desc,
          i_category,
          i_class,
          i_current_price
ORDER  BY i_category,
          i_class,
          i_item_id,
          i_item_desc,
          revenueratio;

-- query88.tpl
SELECT *
FROM   (SELECT COUNT(*) h8_30_to_9
        FROM   store_sales,
               household_demographics,
               time_dim,
               store
        WHERE  ss_sold_time_sk = time_dim.t_time_sk
               AND ss_hdemo_sk = household_demographics.hd_demo_sk
               AND ss_store_sk = s_store_sk
               AND time_dim.t_hour = 8
               AND time_dim.t_minute >= 30
               AND ( ( household_demographics.hd_dep_count = 3
                       AND household_demographics.hd_vehicle_count <= 3 + 2 )
                      OR ( household_demographics.hd_dep_count = 2
                           AND household_demographics.hd_vehicle_count <= 2 + 2 )
                      OR ( household_demographics.hd_dep_count = 4
                           AND household_demographics.hd_vehicle_count <= 4 + 2 ) )
               AND store.s_store_name = 'ese') s1,
       (SELECT COUNT(*) h9_to_9_30
        FROM   store_sales,
               household_demographics,
               time_dim,
               store
        WHERE  ss_sold_time_sk = time_dim.t_time_sk
               AND ss_hdemo_sk = household_demographics.hd_demo_sk
               AND ss_store_sk = s_store_sk
               AND time_dim.t_hour = 9
               AND time_dim.t_minute < 30
               AND ( ( household_demographics.hd_dep_count = 3
                       AND household_demographics.hd_vehicle_count <= 3 + 2 )
                      OR ( household_demographics.hd_dep_count = 2
                           AND household_demographics.hd_vehicle_count <= 2 + 2 )
                      OR ( household_demographics.hd_dep_count = 4
                           AND household_demographics.hd_vehicle_count <= 4 + 2 ) )
               AND store.s_store_name = 'ese') s2,
       (SELECT COUNT(*) h9_30_to_10
        FROM   store_sales,
               household_demographics,
               time_dim,
               store
        WHERE  ss_sold_time_sk = time_dim.t_time_sk
               AND ss_hdemo_sk = household_demographics.hd_demo_sk
               AND ss_store_sk = s_store_sk
               AND time_dim.t_hour = 9
               AND time_dim.t_minute >= 30
               AND ( ( household_demographics.hd_dep_count = 3
                       AND household_demographics.hd_vehicle_count <= 3 + 2 )
                      OR ( household_demographics.hd_dep_count = 2
                           AND household_demographics.hd_vehicle_count <= 2 + 2 )
                      OR ( household_demographics.hd_dep_count = 4
                           AND household_demographics.hd_vehicle_count <= 4 + 2 ) )
               AND store.s_store_name = 'ese') s3,
       (SELECT COUNT(*) h10_to_10_30
        FROM   store_sales,
               household_demographics,
               time_dim,
               store
        WHERE  ss_sold_time_sk = time_dim.t_time_sk
               AND ss_hdemo_sk = household_demographics.hd_demo_sk
               AND ss_store_sk = s_store_sk
               AND time_dim.t_hour = 10
               AND time_dim.t_minute < 30
               AND ( ( household_demographics.hd_dep_count = 3
                       AND household_demographics.hd_vehicle_count <= 3 + 2 )
                      OR ( household_demographics.hd_dep_count = 2
                           AND household_demographics.hd_vehicle_count <= 2 + 2 )
                      OR ( household_demographics.hd_dep_count = 4
                           AND household_demographics.hd_vehicle_count <= 4 + 2 ) )
               AND store.s_store_name = 'ese') s4,
       (SELECT COUNT(*) h10_30_to_11
        FROM   store_sales,
               household_demographics,
               time_dim,
               store
        WHERE  ss_sold_time_sk = time_dim.t_time_sk
               AND ss_hdemo_sk = household_demographics.hd_demo_sk
               AND ss_store_sk = s_store_sk
               AND time_dim.t_hour = 10
               AND time_dim.t_minute >= 30
               AND ( ( household_demographics.hd_dep_count = 3
                       AND household_demographics.hd_vehicle_count <= 3 + 2 )
                      OR ( household_demographics.hd_dep_count = 2
                           AND household_demographics.hd_vehicle_count <= 2 + 2 )
                      OR ( household_demographics.hd_dep_count = 4
                           AND household_demographics.hd_vehicle_count <= 4 + 2 ) )
               AND store.s_store_name = 'ese') s5,
       (SELECT COUNT(*) h11_to_11_30
        FROM   store_sales,
               household_demographics,
               time_dim,
               store
        WHERE  ss_sold_time_sk = time_dim.t_time_sk
               AND ss_hdemo_sk = household_demographics.hd_demo_sk
               AND ss_store_sk = s_store_sk
               AND time_dim.t_hour = 11
               AND time_dim.t_minute < 30
               AND ( ( household_demographics.hd_dep_count = 3
                       AND household_demographics.hd_vehicle_count <= 3 + 2 )
                      OR ( household_demographics.hd_dep_count = 2
                           AND household_demographics.hd_vehicle_count <= 2 + 2 )
                      OR ( household_demographics.hd_dep_count = 4
                           AND household_demographics.hd_vehicle_count <= 4 + 2 ) )
               AND store.s_store_name = 'ese') s6,
       (SELECT COUNT(*) h11_30_to_12
        FROM   store_sales,
               household_demographics,
               time_dim,
               store
        WHERE  ss_sold_time_sk = time_dim.t_time_sk
               AND ss_hdemo_sk = household_demographics.hd_demo_sk
               AND ss_store_sk = s_store_sk
               AND time_dim.t_hour = 11
               AND time_dim.t_minute >= 30
               AND ( ( household_demographics.hd_dep_count = 3
                       AND household_demographics.hd_vehicle_count <= 3 + 2 )
                      OR ( household_demographics.hd_dep_count = 2
                           AND household_demographics.hd_vehicle_count <= 2 + 2 )
                      OR ( household_demographics.hd_dep_count = 4
                           AND household_demographics.hd_vehicle_count <= 4 + 2 ) )
               AND store.s_store_name = 'ese') s7,
       (SELECT COUNT(*) h12_to_12_30
        FROM   store_sales,
               household_demographics,
               time_dim,
               store
        WHERE  ss_sold_time_sk = time_dim.t_time_sk
               AND ss_hdemo_sk = household_demographics.hd_demo_sk
               AND ss_store_sk = s_store_sk
               AND time_dim.t_hour = 12
               AND time_dim.t_minute < 30
               AND ( ( household_demographics.hd_dep_count = 3
                       AND household_demographics.hd_vehicle_count <= 3 + 2 )
                      OR ( household_demographics.hd_dep_count = 2
                           AND household_demographics.hd_vehicle_count <= 2 + 2 )
                      OR ( household_demographics.hd_dep_count = 4
                           AND household_demographics.hd_vehicle_count <= 4 + 2 ) )
               AND store.s_store_name = 'ese') s8;

-- query82.tpl
SELECT i_item_id,
       i_item_desc,
       i_current_price
FROM   item,
       inventory,
       date_dim,
       store_sales
WHERE  i_current_price BETWEEN 31 AND 31 + 30
       AND inv_item_sk = i_item_sk
       AND d_date_sk = inv_date_sk
       AND d_date BETWEEN CAST('2002-05-18' AS DATE) AND ( CAST('2002-05-18' AS DATE) + 60 DAYS )
       AND i_manufact_id IN ( 867, 107, 602, 451 )
       AND inv_quantity_on_hand BETWEEN 100 AND 500
       AND ss_item_sk = i_item_sk
GROUP  BY i_item_id,
          i_item_desc,
          i_current_price
ORDER  BY i_item_id;

-- query23.tpl
WITH frequent_ss_items
     AS (SELECT SUBSTR(i_item_desc, 1, 30) itemdesc,
                i_item_sk                  item_sk,
                d_date                     solddate,
                COUNT(*)                   cnt
         FROM   store_sales,
                date_dim,
                item
         WHERE  ss_sold_date_sk = d_date_sk
                AND ss_item_sk = i_item_sk
                AND d_year IN ( 1999, 1999 + 1, 1999 + 2, 1999 + 3 )
         GROUP  BY SUBSTR(i_item_desc, 1, 30),
                   i_item_sk,
                   d_date
         HAVING COUNT(*) > 4),
     max_store_sales
     AS (SELECT MAX(csales) tpcds_cmax
         FROM   (SELECT c_customer_sk,
                        SUM(ss_quantity * ss_sales_price) csales
                 FROM   store_sales,
                        customer,
                        date_dim
                 WHERE  ss_customer_sk = c_customer_sk
                        AND ss_sold_date_sk = d_date_sk
                        AND d_year IN ( 1999, 1999 + 1, 1999 + 2, 1999 + 3 )
                 GROUP  BY c_customer_sk) x),
     best_ss_customer
     AS (SELECT c_customer_sk,
                SUM(ss_quantity * ss_sales_price) ssales
         FROM   store_sales,
                customer
         WHERE  ss_customer_sk = c_customer_sk
         GROUP  BY c_customer_sk
         HAVING SUM(ss_quantity * ss_sales_price) > ( 95 / 100.0 ) * (SELECT *
                                                                      FROM   max_store_sales))
SELECT SUM(sales)
FROM   ((SELECT cs_quantity * cs_list_price sales
         FROM   catalog_sales,
                date_dim
         WHERE  d_year = 1999
                AND d_moy = 5
                AND cs_sold_date_sk = d_date_sk
                AND cs_item_sk IN (SELECT item_sk
                                   FROM   frequent_ss_items)
                AND cs_bill_customer_sk IN (SELECT c_customer_sk
                                            FROM   best_ss_customer))
        UNION ALL
        (SELECT ws_quantity * ws_list_price sales
         FROM   web_sales,
                date_dim
         WHERE  d_year = 1999
                AND d_moy = 5
                AND ws_sold_date_sk = d_date_sk
                AND ws_item_sk IN (SELECT item_sk
                                   FROM   frequent_ss_items)
                AND ws_bill_customer_sk IN (SELECT c_customer_sk
                                            FROM   best_ss_customer))) y;

-- query14.tpl
WITH cross_items
     AS (SELECT i_item_sk ss_item_sk
         FROM   item,
                (SELECT iss.i_brand_id    brand_id,
                        iss.i_class_id    class_id,
                        iss.i_category_id category_id
                 FROM   store_sales,
                        item iss,
                        date_dim d1
                 WHERE  ss_item_sk = iss.i_item_sk
                        AND ss_sold_date_sk = d1.d_date_sk
                        AND d1.d_year BETWEEN 1999 AND 1999 + 2
                 INTERSECT
                 SELECT ics.i_brand_id,
                        ics.i_class_id,
                        ics.i_category_id
                 FROM   catalog_sales,
                        item ics,
                        date_dim d2
                 WHERE  cs_item_sk = ics.i_item_sk
                        AND cs_sold_date_sk = d2.d_date_sk
                        AND d2.d_year BETWEEN 1999 AND 1999 + 2
                 INTERSECT
                 SELECT iws.i_brand_id,
                        iws.i_class_id,
                        iws.i_category_id
                 FROM   web_sales,
                        item iws,
                        date_dim d3
                 WHERE  ws_item_sk = iws.i_item_sk
                        AND ws_sold_date_sk = d3.d_date_sk
                        AND d3.d_year BETWEEN 1999 AND 1999 + 2) x
         WHERE  i_brand_id = brand_id
                AND i_class_id = class_id
                AND i_category_id = category_id),
     avg_sales
     AS (SELECT AVG(quantity * list_price) average_sales
         FROM   (SELECT ss_quantity   quantity,
                        ss_list_price list_price
                 FROM   store_sales,
                        date_dim
                 WHERE  ss_sold_date_sk = d_date_sk
                        AND d_year BETWEEN 1999 AND 2001
                 UNION ALL
                 SELECT cs_quantity   quantity,
                        cs_list_price list_price
                 FROM   catalog_sales,
                        date_dim
                 WHERE  cs_sold_date_sk = d_date_sk
                        AND d_year BETWEEN 1998 AND 1998 + 2
                 UNION ALL
                 SELECT ws_quantity   quantity,
                        ws_list_price list_price
                 FROM   web_sales,
                        date_dim
                 WHERE  ws_sold_date_sk = d_date_sk
                        AND d_year BETWEEN 1998 AND 1998 + 2) x)
SELECT channel,
       i_brand_id,
       i_class_id,
       i_category_id,
       SUM(sales),
       SUM(number_sales)
FROM  (SELECT 'store'                          channel,
              i_brand_id,
              i_class_id,
              i_category_id,
              SUM(ss_quantity * ss_list_price) sales,
              COUNT(*)                         number_sales
       FROM   store_sales,
              item,
              date_dim
       WHERE  ss_item_sk IN (SELECT ss_item_sk
                             FROM   cross_items)
              AND ss_item_sk = i_item_sk
              AND ss_sold_date_sk = d_date_sk
              AND d_year = 1998 + 2
              AND d_moy = 11
       GROUP  BY i_brand_id,
                 i_class_id,
                 i_category_id
       HAVING SUM(ss_quantity * ss_list_price) > (SELECT average_sales
                                                  FROM   avg_sales)
       UNION ALL
       SELECT 'catalog'                        channel,
              i_brand_id,
              i_class_id,
              i_category_id,
              SUM(cs_quantity * cs_list_price) sales,
              COUNT(*)                         number_sales
       FROM   catalog_sales,
              item,
              date_dim
       WHERE  cs_item_sk IN (SELECT ss_item_sk
                             FROM   cross_items)
              AND cs_item_sk = i_item_sk
              AND cs_sold_date_sk = d_date_sk
              AND d_year = 1998 + 2
              AND d_moy = 11
       GROUP  BY i_brand_id,
                 i_class_id,
                 i_category_id
       HAVING SUM(cs_quantity * cs_list_price) > (SELECT average_sales
                                                  FROM   avg_sales)
       UNION ALL
       SELECT 'web'                            channel,
              i_brand_id,
              i_class_id,
              i_category_id,
              SUM(ws_quantity * ws_list_price) sales,
              COUNT(*)                         number_sales
       FROM   web_sales,
              item,
              date_dim
       WHERE  ws_item_sk IN (SELECT ss_item_sk
                             FROM   cross_items)
              AND ws_item_sk = i_item_sk
              AND ws_sold_date_sk = d_date_sk
              AND d_year = 1998 + 2
              AND d_moy = 11
       GROUP  BY i_brand_id,
                 i_class_id,
                 i_category_id
       HAVING SUM(ws_quantity * ws_list_price) > (SELECT average_sales
                                                  FROM   avg_sales)) y
GROUP  BY ROLLUP ( channel, i_brand_id, i_class_id, i_category_id )
ORDER  BY channel,
          i_brand_id,
          i_class_id,
          i_category_id;

-- query57.tpl
WITH v1
     AS (SELECT i_category,
                i_brand,
                cc_name,
                d_year,
                d_moy,
                SUM(cs_sales_price)                                                               sum_sales,
                AVG(SUM(cs_sales_price)) OVER (PARTITION BY i_category, i_brand, cc_name, d_year) avg_monthly_sales,
                RANK() OVER (PARTITION BY i_category, i_brand, cc_name ORDER BY d_year, d_moy)    rn
         FROM   item,
                catalog_sales,
                date_dim,
                call_center
         WHERE  cs_item_sk = i_item_sk
                AND cs_sold_date_sk = d_date_sk
                AND cc_call_center_sk = cs_call_center_sk
                AND ( d_year = 1999
                       OR ( d_year = 1999 - 1
                            AND d_moy = 12 )
                       OR ( d_year = 1999 + 1
                            AND d_moy = 1 ) )
         GROUP  BY i_category,
                   i_brand,
                   cc_name,
                   d_year,
                   d_moy),
     v2
     AS (SELECT v1.i_category,
                v1.i_brand,
                v1.cc_name,
                v1.d_year,
                v1.d_moy,
                v1.avg_monthly_sales,
                v1.sum_sales,
                v1_lag.sum_sales  psum,
                v1_lead.sum_sales nsum
         FROM   v1,
                v1 v1_lag,
                v1 v1_lead
         WHERE  v1.i_category = v1_lag.i_category
                AND v1.i_category = v1_lead.i_category
                AND v1.i_brand = v1_lag.i_brand
                AND v1.i_brand = v1_lead.i_brand
                AND v1. cc_name = v1_lag. cc_name
                AND v1. cc_name = v1_lead. cc_name
                AND v1.rn = v1_lag.rn + 1
                AND v1.rn = v1_lead.rn - 1)
SELECT *
FROM   v2
WHERE  d_year = 1999
       AND avg_monthly_sales > 0
       AND CASE
             WHEN avg_monthly_sales > 0 THEN ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales
             ELSE NULL
           END > 0.1
ORDER  BY sum_sales - avg_monthly_sales,
          cc_name;

-- query65.tpl
SELECT s_store_name,
       i_item_desc,
       sc.revenue
FROM   store,
       item,
       (SELECT ss_store_sk,
               AVG(revenue) AS ave
        FROM   (SELECT ss_store_sk,
                       ss_item_sk,
                       SUM(ss_sales_price) AS revenue
                FROM   store_sales,
                       date_dim
                WHERE  ss_sold_date_sk = d_date_sk
                       AND d_year = 1999
                GROUP  BY ss_store_sk,
                          ss_item_sk) sa
        GROUP  BY ss_store_sk) sb,
       (SELECT ss_store_sk,
               ss_item_sk,
               SUM(ss_sales_price) AS revenue
        FROM   store_sales,
               date_dim
        WHERE  ss_sold_date_sk = d_date_sk
               AND d_year = 1999
        GROUP  BY ss_store_sk,
                  ss_item_sk) sc
WHERE  sb.ss_store_sk = sc.ss_store_sk
       AND sc.revenue <= 0.1 * sb.ave
       AND s_store_sk = sc.ss_store_sk
       AND i_item_sk = sc.ss_item_sk
ORDER  BY s_store_name,
          i_item_desc;

-- query71.tpl
SELECT i_brand_id     brand_id,
       i_brand        brand,
       t_hour,
       t_minute,
       SUM(ext_price) ext_price
FROM   item,
       (SELECT ws_ext_sales_price AS ext_price,
               ws_sold_date_sk    AS sold_date_sk,
               ws_item_sk         AS sold_item_sk,
               ws_sold_time_sk    AS time_sk
        FROM   web_sales,
               date_dim
        WHERE  d_date_sk = ws_sold_date_sk
               AND d_moy = 12
               AND d_year = 2002
        UNION ALL
        SELECT cs_ext_sales_price AS ext_price,
               cs_sold_date_sk    AS sold_date_sk,
               cs_item_sk         AS sold_item_sk,
               cs_sold_time_sk    AS time_sk
        FROM   catalog_sales,
               date_dim
        WHERE  d_date_sk = cs_sold_date_sk
               AND d_moy = 12
               AND d_year = 2002
        UNION ALL
        SELECT ss_ext_sales_price AS ext_price,
               ss_sold_date_sk    AS sold_date_sk,
               ss_item_sk         AS sold_item_sk,
               ss_sold_time_sk    AS time_sk
        FROM   store_sales,
               date_dim
        WHERE  d_date_sk = ss_sold_date_sk
               AND d_moy = 12
               AND d_year = 2002) AS tmp,
       time_dim
WHERE  sold_item_sk = i_item_sk
       AND i_manager_id = 1
       AND time_sk = t_time_sk
       AND ( t_meal_time = 'breakfast'
              OR t_meal_time = 'dinner' )
GROUP  BY i_brand,
          i_brand_id,
          t_hour,
          t_minute
ORDER  BY ext_price DESC,
          i_brand_id;

-- query34.tpl
SELECT c_last_name,
       c_first_name,
       c_salutation,
       c_preferred_cust_flag,
       ss_ticket_number,
       cnt
FROM   (SELECT ss_ticket_number,
               ss_customer_sk,
               COUNT(*) cnt
        FROM   store_sales,
               date_dim,
               store,
               household_demographics
        WHERE  store_sales.ss_sold_date_sk = date_dim.d_date_sk
               AND store_sales.ss_store_sk = store.s_store_sk
               AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
               AND ( date_dim.d_dom BETWEEN 1 AND 3
                      OR date_dim.d_dom BETWEEN 25 AND 28 )
               AND ( household_demographics.hd_buy_potential = '501-1000'
                      OR household_demographics.hd_buy_potential = '5001-10000' )
               AND household_demographics.hd_vehicle_count > 0
               AND ( CASE
                       WHEN household_demographics.hd_vehicle_count > 0 THEN household_demographics.hd_dep_count /
                                                                             household_demographics.hd_vehicle_count
                       ELSE NULL
                     END ) > 1.2
               AND date_dim.d_year IN ( 1998, 1998 + 1, 1998 + 2 )
               AND store.s_county IN ( 'Ziebach County', 'Williamson County', 'Walker County', 'Ziebach County',
                                       'Ziebach County', 'Ziebach County', 'Williamson County', 'Ziebach County' )
        GROUP  BY ss_ticket_number,
                  ss_customer_sk) dn,
       customer
WHERE  ss_customer_sk = c_customer_sk
       AND cnt BETWEEN 15 AND 20
ORDER  BY c_last_name,
          c_first_name,
          c_salutation,
          c_preferred_cust_flag DESC;

-- query48.tpl
SELECT SUM (ss_quantity)
FROM   store_sales,
       store,
       customer_demographics,
       customer_address,
       date_dim
WHERE  s_store_sk = ss_store_sk
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

-- query30.tpl
WITH customer_total_return
     AS (SELECT wr_returning_customer_sk AS ctr_customer_sk,
                ca_state                 AS ctr_state,
                SUM(wr_return_amt)       AS ctr_total_return
         FROM   web_returns,
                date_dim,
                customer_address
         WHERE  wr_returned_date_sk = d_date_sk
                AND d_year = 1999
                AND wr_returning_addr_sk = ca_address_sk
         GROUP  BY wr_returning_customer_sk,
                   ca_state)
SELECT c_customer_id,
       c_salutation,
       c_first_name,
       c_last_name,
       c_preferred_cust_flag,
       c_birth_day,
       c_birth_month,
       c_birth_year,
       c_birth_country,
       c_login,
       c_email_address,
       c_last_review_date,
       ctr_total_return
FROM   customer_total_return ctr1,
       customer_address,
       customer
WHERE  ctr1.ctr_total_return > (SELECT AVG(ctr_total_return) * 1.2
                                FROM   customer_total_return ctr2
                                WHERE  ctr1.ctr_state = ctr2.ctr_state)
       AND ca_address_sk = c_current_addr_sk
       AND ca_state = 'CO'
       AND ctr1.ctr_customer_sk = c_customer_sk
ORDER  BY c_customer_id,
          c_salutation,
          c_first_name,
          c_last_name,
          c_preferred_cust_flag,
          c_birth_day,
          c_birth_month,
          c_birth_year,
          c_birth_country,
          c_login,
          c_email_address,
          c_last_review_date,
          ctr_total_return;

-- query74.tpl
WITH year_total
     AS (SELECT c_customer_id    customer_id,
                c_first_name     customer_first_name,
                c_last_name      customer_last_name,
                d_year           AS YEAR,
                SUM(ss_net_paid) year_total,
                's'              sale_type
         FROM   customer,
                store_sales,
                date_dim
         WHERE  c_customer_sk = ss_customer_sk
                AND ss_sold_date_sk = d_date_sk
                AND d_year IN ( 2001, 2001 + 1 )
         GROUP  BY c_customer_id,
                   c_first_name,
                   c_last_name,
                   d_year
         UNION ALL
         SELECT c_customer_id    customer_id,
                c_first_name     customer_first_name,
                c_last_name      customer_last_name,
                d_year           AS YEAR,
                SUM(ws_net_paid) year_total,
                'w'              sale_type
         FROM   customer,
                web_sales,
                date_dim
         WHERE  c_customer_sk = ws_bill_customer_sk
                AND ws_sold_date_sk = d_date_sk
                AND d_year IN ( 2001, 2001 + 1 )
         GROUP  BY c_customer_id,
                   c_first_name,
                   c_last_name,
                   d_year)
SELECT t_s_secyear.customer_id,
       t_s_secyear.customer_first_name,
       t_s_secyear.customer_last_name
FROM   year_total t_s_firstyear,
       year_total t_s_secyear,
       year_total t_w_firstyear,
       year_total t_w_secyear
WHERE  t_s_secyear.customer_id = t_s_firstyear.customer_id
       AND t_s_firstyear.customer_id = t_w_secyear.customer_id
       AND t_s_firstyear.customer_id = t_w_firstyear.customer_id
       AND t_s_firstyear.sale_type = 's'
       AND t_w_firstyear.sale_type = 'w'
       AND t_s_secyear.sale_type = 's'
       AND t_w_secyear.sale_type = 'w'
       AND t_s_firstyear.year = 2001
       AND t_s_secyear.year = 2001 + 1
       AND t_w_firstyear.year = 2001
       AND t_w_secyear.year = 2001 + 1
       AND t_s_firstyear.year_total > 0
       AND t_w_firstyear.year_total > 0
       AND CASE
             WHEN t_w_firstyear.year_total > 0 THEN t_w_secyear.year_total / t_w_firstyear.year_total
             ELSE NULL
           END > CASE
                   WHEN t_s_firstyear.year_total > 0 THEN t_s_secyear.year_total / t_s_firstyear.year_total
                   ELSE NULL
                 END
ORDER  BY 1;

-- query87.tpl
SELECT COUNT(*)
FROM   ((SELECT DISTINCT c_last_name,
                         c_first_name,
                         d_date
         FROM   store_sales,
                date_dim,
                customer
         WHERE  store_sales.ss_sold_date_sk = date_dim.d_date_sk
                AND store_sales.ss_customer_sk = customer.c_customer_sk
                AND d_year = 1998)
        EXCEPT
        (SELECT DISTINCT c_last_name,
                         c_first_name,
                         d_date
         FROM   catalog_sales,
                date_dim,
                customer
         WHERE  catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
                AND catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
                AND d_year = 1998)
        EXCEPT
        (SELECT DISTINCT c_last_name,
                         c_first_name,
                         d_date
         FROM   web_sales,
                date_dim,
                customer
         WHERE  web_sales.ws_sold_date_sk = date_dim.d_date_sk
                AND web_sales.ws_bill_customer_sk = customer.c_customer_sk
                AND d_year = 1998)) cool_cust;

-- query77.tpl
WITH ss
     AS (SELECT s_store_sk,
                SUM(ss_ext_sales_price) AS sales,
                SUM(ss_net_profit)      AS profit
         FROM   store_sales,
                date_dim,
                store
         WHERE  ss_sold_date_sk = d_date_sk
                AND d_date BETWEEN CAST('1998-08-09' AS DATE) AND ( CAST('1998-08-09' AS DATE) + 30 DAYS )
                AND ss_store_sk = s_store_sk
         GROUP  BY s_store_sk),
     sr
     AS (SELECT s_store_sk,
                SUM(sr_return_amt) AS RETURNS,
                SUM(sr_net_loss)   AS profit_loss
         FROM   store_returns,
                date_dim,
                store
         WHERE  sr_returned_date_sk = d_date_sk
                AND d_date BETWEEN CAST('1998-08-09' AS DATE) AND ( CAST('1998-08-09' AS DATE) + 30 DAYS )
                AND sr_store_sk = s_store_sk
         GROUP  BY s_store_sk),
     cs
     AS (SELECT cs_call_center_sk,
                SUM(cs_ext_sales_price) AS sales,
                SUM(cs_net_profit)      AS profit
         FROM   catalog_sales,
                date_dim
         WHERE  cs_sold_date_sk = d_date_sk
                AND d_date BETWEEN CAST('1998-08-09' AS DATE) AND ( CAST('1998-08-09' AS DATE) + 30 DAYS )
         GROUP  BY cs_call_center_sk),
     cr
     AS (SELECT SUM(cr_return_amount) AS RETURNS,
                SUM(cr_net_loss)      AS profit_loss
         FROM   catalog_returns,
                date_dim
         WHERE  cr_returned_date_sk = d_date_sk
                AND d_date BETWEEN CAST('1998-08-09' AS DATE) AND ( CAST('1998-08-09' AS DATE) + 30 DAYS )),
     ws
     AS (SELECT wp_web_page_sk,
                SUM(ws_ext_sales_price) AS sales,
                SUM(ws_net_profit)      AS profit
         FROM   web_sales,
                date_dim,
                web_page
         WHERE  ws_sold_date_sk = d_date_sk
                AND d_date BETWEEN CAST('1998-08-09' AS DATE) AND ( CAST('1998-08-09' AS DATE) + 30 DAYS )
                AND ws_web_page_sk = wp_web_page_sk
         GROUP  BY wp_web_page_sk),
     wr
     AS (SELECT wp_web_page_sk,
                SUM(wr_return_amt) AS RETURNS,
                SUM(wr_net_loss)   AS profit_loss
         FROM   web_returns,
                date_dim,
                web_page
         WHERE  wr_returned_date_sk = d_date_sk
                AND d_date BETWEEN CAST('1998-08-09' AS DATE) AND ( CAST('1998-08-09' AS DATE) + 30 DAYS )
                AND wr_web_page_sk = wp_web_page_sk
         GROUP  BY wp_web_page_sk)
SELECT channel,
       id,
       SUM(sales)   AS sales,
       SUM(RETURNS) AS RETURNS,
       SUM(profit)  AS profit
FROM   (SELECT 'store channel'                       AS channel,
               ss.s_store_sk                         AS id,
               sales,
               COALESCE(RETURNS, 0)                  AS RETURNS,
               ( profit - COALESCE(profit_loss, 0) ) AS profit
        FROM   ss
               LEFT JOIN sr
                 ON ss.s_store_sk = sr.s_store_sk
        UNION ALL
        SELECT 'catalog channel'        AS channel,
               cs_call_center_sk        AS id,
               sales,
               RETURNS,
               ( profit - profit_loss ) AS profit
        FROM   cs,
               cr
        UNION ALL
        SELECT 'web channel'                         AS channel,
               ws.wp_web_page_sk                     AS id,
               sales,
               COALESCE(RETURNS, 0)                  RETURNS,
               ( profit - COALESCE(profit_loss, 0) ) AS profit
        FROM   ws
               LEFT JOIN wr
                 ON ws.wp_web_page_sk = wr.wp_web_page_sk) x
GROUP  BY ROLLUP ( channel, id )
ORDER  BY channel,
          id;

-- query73.tpl
SELECT c_last_name,
       c_first_name,
       c_salutation,
       c_preferred_cust_flag,
       ss_ticket_number,
       cnt
FROM   (SELECT ss_ticket_number,
               ss_customer_sk,
               COUNT(*) cnt
        FROM   store_sales,
               date_dim,
               store,
               household_demographics
        WHERE  store_sales.ss_sold_date_sk = date_dim.d_date_sk
               AND store_sales.ss_store_sk = store.s_store_sk
               AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
               AND date_dim.d_dom BETWEEN 1 AND 2
               AND ( household_demographics.hd_buy_potential = '1001-5000'
                      OR household_demographics.hd_buy_potential = '0-500' )
               AND household_demographics.hd_vehicle_count > 0
               AND CASE
                     WHEN household_demographics.hd_vehicle_count > 0 THEN household_demographics.hd_dep_count /
                                                                           household_demographics.hd_vehicle_count
                     ELSE NULL
                   END > 1
               AND date_dim.d_year IN ( 1999, 1999 + 1, 1999 + 2 )
               AND store.s_county IN ( 'Williamson County', 'Ziebach County', 'Walker County', 'Ziebach County' )
        GROUP  BY ss_ticket_number,
                  ss_customer_sk) dj,
       customer
WHERE  ss_customer_sk = c_customer_sk
       AND cnt BETWEEN 1 AND 5
ORDER  BY cnt DESC;

-- query84.tpl
SELECT c_customer_id    AS customer_id,
       c_last_name
        || ', '
        || c_first_name AS customername
FROM   customer,
       customer_address,
       customer_demographics,
       household_demographics,
       income_band,
       store_returns
WHERE  ca_city = 'Salem'
       AND c_current_addr_sk = ca_address_sk
       AND ib_lower_bound >= 38258
       AND ib_upper_bound <= 38258 + 50000
       AND ib_income_band_sk = hd_income_band_sk
       AND cd_demo_sk = c_current_cdemo_sk
       AND hd_demo_sk = c_current_hdemo_sk
       AND sr_cdemo_sk = cd_demo_sk
ORDER  BY c_customer_id;

-- query54.tpl
WITH my_customers
     AS (SELECT DISTINCT c_customer_sk,
                         c_current_addr_sk
         FROM   (SELECT cs_sold_date_sk     sold_date_sk,
                        cs_bill_customer_sk customer_sk,
                        cs_item_sk          item_sk
                 FROM   catalog_sales
                 UNION ALL
                 SELECT ws_sold_date_sk     sold_date_sk,
                        ws_bill_customer_sk customer_sk,
                        ws_item_sk          item_sk
                 FROM   web_sales) cs_or_ws_sales,
                item,
                date_dim,
                customer
         WHERE  sold_date_sk = d_date_sk
                AND item_sk = i_item_sk
                AND i_category = 'Women'
                AND i_class = 'fragrances'
                AND c_customer_sk = cs_or_ws_sales.customer_sk
                AND d_moy = 6
                AND d_year = 2001),
     my_revenue
     AS (SELECT c_customer_sk,
                SUM(ss_ext_sales_price) AS revenue
         FROM   my_customers,
                store_sales,
                customer_address,
                store,
                date_dim
         WHERE  c_current_addr_sk = ca_address_sk
                AND ca_county = s_county
                AND ca_state = s_state
                AND ss_sold_date_sk = d_date_sk
                AND c_customer_sk = ss_customer_sk
                AND d_month_seq BETWEEN (SELECT DISTINCT d_month_seq + 1
                                         FROM   date_dim
                                         WHERE  d_year = 2001
                                                AND d_moy = 6) AND (SELECT DISTINCT d_month_seq + 3
                                                                    FROM   date_dim
                                                                    WHERE  d_year = 2001
                                                                           AND d_moy = 6)
         GROUP  BY c_customer_sk),
     segments
     AS (SELECT CAST(( revenue / 50 ) AS INT) AS segment
         FROM   my_revenue)
SELECT segment,
       COUNT(*)     AS num_customers,
       segment * 50 AS segment_base
FROM   segments
GROUP  BY segment
ORDER  BY segment,
          num_customers;

-- query55.tpl
SELECT i_brand_id              brand_id,
       i_brand                 brand,
       SUM(ss_ext_sales_price) ext_price
FROM   date_dim,
       store_sales,
       item
WHERE  d_date_sk = ss_sold_date_sk
       AND ss_item_sk = i_item_sk
       AND i_manager_id = 53
       AND d_moy = 12
       AND d_year = 2001
GROUP  BY i_brand,
          i_brand_id
ORDER  BY ext_price DESC,
          i_brand_id;

-- query56.tpl
WITH ss
     AS (SELECT i_item_id,
                SUM(ss_ext_sales_price) total_sales
         FROM   store_sales,
                date_dim,
                customer_address,
                item
         WHERE  i_item_id IN (SELECT i_item_id
                              FROM   item
                              WHERE  i_color IN ( 'mint', 'orange', 'olive' ))
                AND ss_item_sk = i_item_sk
                AND ss_sold_date_sk = d_date_sk
                AND d_year = 1999
                AND d_moy = 7
                AND ss_addr_sk = ca_address_sk
                AND ca_gmt_offset = -6
         GROUP  BY i_item_id),
     cs
     AS (SELECT i_item_id,
                SUM(cs_ext_sales_price) total_sales
         FROM   catalog_sales,
                date_dim,
                customer_address,
                item
         WHERE  i_item_id IN (SELECT i_item_id
                              FROM   item
                              WHERE  i_color IN ( 'mint', 'orange', 'olive' ))
                AND cs_item_sk = i_item_sk
                AND cs_sold_date_sk = d_date_sk
                AND d_year = 1999
                AND d_moy = 7
                AND cs_bill_addr_sk = ca_address_sk
                AND ca_gmt_offset = -6
         GROUP  BY i_item_id),
     ws
     AS (SELECT i_item_id,
                SUM(ws_ext_sales_price) total_sales
         FROM   web_sales,
                date_dim,
                customer_address,
                item
         WHERE  i_item_id IN (SELECT i_item_id
                              FROM   item
                              WHERE  i_color IN ( 'mint', 'orange', 'olive' ))
                AND ws_item_sk = i_item_sk
                AND ws_sold_date_sk = d_date_sk
                AND d_year = 1999
                AND d_moy = 7
                AND ws_bill_addr_sk = ca_address_sk
                AND ca_gmt_offset = -6
         GROUP  BY i_item_id)
SELECT i_item_id,
       SUM(total_sales) total_sales
FROM   (SELECT *
        FROM   ss
        UNION ALL
        SELECT *
        FROM   cs
        UNION ALL
        SELECT *
        FROM   ws) tmp1
GROUP  BY i_item_id
ORDER  BY total_sales;

-- query2.tpl
WITH wscs
     AS (SELECT sold_date_sk,
                sales_price
         FROM   (SELECT ws_sold_date_sk    sold_date_sk,
                        ws_ext_sales_price sales_price
                 FROM   web_sales) x
         UNION ALL
         (SELECT cs_sold_date_sk    sold_date_sk,
                 cs_ext_sales_price sales_price
          FROM   catalog_sales)),
     wswscs
     AS (SELECT d_week_seq,
                SUM(CASE
                      WHEN ( d_day_name = 'Sunday' ) THEN sales_price
                      ELSE NULL
                    END) sun_sales,
                SUM(CASE
                      WHEN ( d_day_name = 'Monday' ) THEN sales_price
                      ELSE NULL
                    END) mon_sales,
                SUM(CASE
                      WHEN ( d_day_name = 'Tuesday' ) THEN sales_price
                      ELSE NULL
                    END) tue_sales,
                SUM(CASE
                      WHEN ( d_day_name = 'Wednesday' ) THEN sales_price
                      ELSE NULL
                    END) wed_sales,
                SUM(CASE
                      WHEN ( d_day_name = 'Thursday' ) THEN sales_price
                      ELSE NULL
                    END) thu_sales,
                SUM(CASE
                      WHEN ( d_day_name = 'Friday' ) THEN sales_price
                      ELSE NULL
                    END) fri_sales,
                SUM(CASE
                      WHEN ( d_day_name = 'Saturday' ) THEN sales_price
                      ELSE NULL
                    END) sat_sales
         FROM   wscs,
                date_dim
         WHERE  d_date_sk = sold_date_sk
         GROUP  BY d_week_seq)
SELECT d_week_seq1,
       ROUND(sun_sales1 / sun_sales2, 2),
       ROUND(mon_sales1 / mon_sales2, 2),
       ROUND(tue_sales1 / tue_sales2, 2),
       ROUND(wed_sales1 / wed_sales2, 2),
       ROUND(thu_sales1 / thu_sales2, 2),
       ROUND(fri_sales1 / fri_sales2, 2),
       ROUND(sat_sales1 / sat_sales2, 2)
FROM   (SELECT wswscs.d_week_seq d_week_seq1,
               sun_sales         sun_sales1,
               mon_sales         mon_sales1,
               tue_sales         tue_sales1,
               wed_sales         wed_sales1,
               thu_sales         thu_sales1,
               fri_sales         fri_sales1,
               sat_sales         sat_sales1
        FROM   wswscs,
               date_dim
        WHERE  date_dim.d_week_seq = wswscs.d_week_seq
               AND d_year = 2001) y,
       (SELECT wswscs.d_week_seq d_week_seq2,
               sun_sales         sun_sales2,
               mon_sales         mon_sales2,
               tue_sales         tue_sales2,
               wed_sales         wed_sales2,
               thu_sales         thu_sales2,
               fri_sales         fri_sales2,
               sat_sales         sat_sales2
        FROM   wswscs,
               date_dim
        WHERE  date_dim.d_week_seq = wswscs.d_week_seq
               AND d_year = 2001 + 1) z
WHERE  d_week_seq1 = d_week_seq2 - 53
ORDER  BY d_week_seq1;

-- query26.tpl
SELECT i_item_id,
       AVG(cs_quantity)    agg1,
       AVG(cs_list_price)  agg2,
       AVG(cs_coupon_amt)  agg3,
       AVG(cs_sales_price) agg4
FROM   catalog_sales,
       customer_demographics,
       date_dim,
       item,
       promotion
WHERE  cs_sold_date_sk = d_date_sk
       AND cs_item_sk = i_item_sk
       AND cs_bill_cdemo_sk = cd_demo_sk
       AND cs_promo_sk = p_promo_sk
       AND cd_gender = 'F'
       AND cd_marital_status = 'D'
       AND cd_education_status = 'Primary'
       AND ( p_channel_email = 'N'
              OR p_channel_event = 'N' )
       AND d_year = 1998
GROUP  BY i_item_id
ORDER  BY i_item_id;

-- query40.tpl
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
FROM   catalog_sales
       LEFT OUTER JOIN catalog_returns
         ON ( cs_order_number = cr_order_number
              AND cs_item_sk = cr_item_sk ),
       warehouse,
       item,
       date_dim
WHERE  i_current_price BETWEEN 0.99 AND 1.49
       AND i_item_sk = cs_item_sk
       AND cs_warehouse_sk = w_warehouse_sk
       AND cs_sold_date_sk = d_date_sk
       AND d_date BETWEEN ( CAST ('2001-05-21' AS DATE) - 30 DAYS ) AND ( CAST ('2001-05-21' AS DATE) + 30 DAYS )
GROUP  BY w_state,
          i_item_id
ORDER  BY w_state,
          i_item_id;

-- query72.tpl
SELECT i_item_desc,
       w_warehouse_name,
       d1.d_week_seq,
       COUNT(CASE
               WHEN p_promo_sk IS NULL THEN 1
               ELSE 0
             END) no_promo,
       COUNT(CASE
               WHEN p_promo_sk IS NOT NULL THEN 1
               ELSE 0
             END) promo,
       COUNT(*)   total_cnt
FROM   catalog_sales
       JOIN inventory
         ON ( cs_item_sk = inv_item_sk )
       JOIN warehouse
         ON ( w_warehouse_sk = inv_warehouse_sk )
       JOIN item
         ON ( i_item_sk = cs_item_sk )
       JOIN customer_demographics
         ON ( cs_bill_cdemo_sk = cd_demo_sk )
       JOIN household_demographics
         ON ( cs_bill_hdemo_sk = hd_demo_sk )
       JOIN date_dim d1
         ON ( cs_sold_date_sk = d1.d_date_sk )
       JOIN date_dim d2
         ON ( inv_date_sk = d2.d_date_sk )
       JOIN date_dim d3
         ON ( cs_ship_date_sk = d3.d_date_sk )
       LEFT OUTER JOIN promotion
         ON ( cs_promo_sk = p_promo_sk )
       LEFT OUTER JOIN catalog_returns
         ON ( cr_item_sk = cs_item_sk
              AND cr_order_number = cs_order_number )
WHERE  d1.d_week_seq = d2.d_week_seq
       AND inv_quantity_on_hand < cs_quantity
       AND d3.d_date > d1.d_date + 5
       AND hd_buy_potential = '>10000'
       AND d1.d_year = 1999
       AND hd_buy_potential = '>10000'
       AND cd_marital_status = 'M'
       AND d1.d_year = 1999
GROUP  BY i_item_desc,
          w_warehouse_name,
          d1.d_week_seq
ORDER  BY total_cnt DESC,
          i_item_desc,
          w_warehouse_name,
          d_week_seq;

-- query53.tpl
SELECT *
FROM   (SELECT i_manufact_id,
               SUM(ss_sales_price)                                        sum_sales,
               AVG(SUM(ss_sales_price)) OVER (PARTITION BY i_manufact_id) avg_quarterly_sales
        FROM   item,
               store_sales,
               date_dim,
               store
        WHERE  ss_item_sk = i_item_sk
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
        GROUP  BY i_manufact_id,
                  d_qoy) tmp1
WHERE  CASE
         WHEN avg_quarterly_sales > 0 THEN ABS (sum_sales - avg_quarterly_sales) / avg_quarterly_sales
         ELSE NULL
       END > 0.1
ORDER  BY avg_quarterly_sales,
          sum_sales,
          i_manufact_id;

-- query79.tpl
SELECT c_last_name,
       c_first_name,
       SUBSTR(s_city, 1, 30),
       ss_ticket_number,
       amt,
       profit
FROM   (SELECT ss_ticket_number,
               ss_customer_sk,
               store.s_city,
               SUM(ss_coupon_amt) amt,
               SUM(ss_net_profit) profit
        FROM   store_sales,
               date_dim,
               store,
               household_demographics
        WHERE  store_sales.ss_sold_date_sk = date_dim.d_date_sk
               AND store_sales.ss_store_sk = store.s_store_sk
               AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
               AND ( household_demographics.hd_dep_count = 1
                      OR household_demographics.hd_vehicle_count > 0 )
               AND date_dim.d_dow = 1
               AND date_dim.d_year IN ( 1999, 1999 + 1, 1999 + 2 )
               AND store.s_number_employees BETWEEN 200 AND 295
        GROUP  BY ss_ticket_number,
                  ss_customer_sk,
                  ss_addr_sk,
                  store.s_city) ms,
       customer
WHERE  ss_customer_sk = c_customer_sk
ORDER  BY c_last_name,
          c_first_name,
          SUBSTR(s_city, 1, 30),
          profit;

-- query18.tpl
SELECT i_item_id,
       ca_country,
       ca_state,
       ca_county,
       AVG(CAST(cs_quantity AS NUMERIC(12, 2)))      agg1,
       AVG(CAST(cs_list_price AS NUMERIC(12, 2)))    agg2,
       AVG(CAST(cs_coupon_amt AS NUMERIC(12, 2)))    agg3,
       AVG(CAST(cs_sales_price AS NUMERIC(12, 2)))   agg4,
       AVG(CAST(cs_net_profit AS NUMERIC(12, 2)))    agg5,
       AVG(CAST(c_birth_year AS NUMERIC(12, 2)))     agg6,
       AVG(CAST(cd1.cd_dep_count AS NUMERIC(12, 2))) agg7
FROM   catalog_sales,
       customer_demographics cd1,
       customer_demographics cd2,
       customer,
       customer_address,
       date_dim,
       item
WHERE  cs_sold_date_sk = d_date_sk
       AND cs_item_sk = i_item_sk
       AND cs_bill_cdemo_sk = cd1.cd_demo_sk
       AND cs_bill_customer_sk = c_customer_sk
       AND cd1.cd_gender = 'M'
       AND cd1.cd_education_status = 'Advanced Degree'
       AND c_current_cdemo_sk = cd2.cd_demo_sk
       AND c_current_addr_sk = ca_address_sk
       AND c_birth_month IN ( 10, 4, 3, 5,
                              9, 7 )
       AND d_year = 1998
       AND ca_state IN ( 'VA', 'GA', 'VA', 'WV',
                         'VA', 'KS', 'AZ' )
GROUP  BY ROLLUP ( i_item_id, ca_country, ca_state, ca_county )
ORDER  BY ca_country,
          ca_state,
          ca_county,
          i_item_id;

-- query13.tpl
SELECT AVG(ss_quantity),
       AVG(ss_ext_sales_price),
       AVG(ss_ext_wholesale_cost),
       SUM(ss_ext_wholesale_cost)
FROM   store_sales,
       store,
       customer_demographics,
       household_demographics,
       customer_address,
       date_dim
WHERE  s_store_sk = ss_store_sk
       AND ss_sold_date_sk = d_date_sk
       AND d_year = 2001
       AND ( ( ss_hdemo_sk = hd_demo_sk
               AND cd_demo_sk = ss_cdemo_sk
               AND cd_marital_status = 'D'
               AND cd_education_status = 'College'
               AND ss_sales_price BETWEEN 100.00 AND 150.00
               AND hd_dep_count = 3 )
              OR ( ss_hdemo_sk = hd_demo_sk
                   AND cd_demo_sk = ss_cdemo_sk
                   AND cd_marital_status = 'U'
                   AND cd_education_status = '2 yr Degree'
                   AND ss_sales_price BETWEEN 50.00 AND 100.00
                   AND hd_dep_count = 1 )
              OR ( ss_hdemo_sk = hd_demo_sk
                   AND cd_demo_sk = ss_cdemo_sk
                   AND cd_marital_status = 'W'
                   AND cd_education_status = '4 yr Degree'
                   AND ss_sales_price BETWEEN 150.00 AND 200.00
                   AND hd_dep_count = 1 ) )
       AND ( ( ss_addr_sk = ca_address_sk
               AND ca_country = 'United States'
               AND ca_state IN ( 'MO', 'KS', 'IA' )
               AND ss_net_profit BETWEEN 100 AND 200 )
              OR ( ss_addr_sk = ca_address_sk
                   AND ca_country = 'United States'
                   AND ca_state IN ( 'WV', 'KS', 'NM' )
                   AND ss_net_profit BETWEEN 150 AND 300 )
              OR ( ss_addr_sk = ca_address_sk
                   AND ca_country = 'United States'
                   AND ca_state IN ( 'MA', 'MO', 'TX' )
                   AND ss_net_profit BETWEEN 50 AND 250 ) );

-- query24.tpl
WITH ssales
     AS (SELECT c_last_name,
                c_first_name,
                s_store_name,
                ca_state,
                s_state,
                i_color,
                i_current_price,
                i_manager_id,
                i_units,
                i_size,
                SUM(ss_sales_price) netpaid
         FROM   store_sales,
                store_returns,
                store,
                item,
                customer,
                customer_address
         WHERE  ss_ticket_number = sr_ticket_number
                AND ss_item_sk = sr_item_sk
                AND ss_customer_sk = c_customer_sk
                AND ss_item_sk = i_item_sk
                AND ss_store_sk = s_store_sk
                AND c_birth_country = UPPER(ca_country)
                AND s_zip = ca_zip
                AND s_market_id = 8
         GROUP  BY c_last_name,
                   c_first_name,
                   s_store_name,
                   ca_state,
                   s_state,
                   i_color,
                   i_current_price,
                   i_manager_id,
                   i_units,
                   i_size)
SELECT c_last_name,
       c_first_name,
       s_store_name,
       SUM(netpaid) paid
FROM   ssales
WHERE  i_color = 'dodger'
GROUP  BY c_last_name,
          c_first_name,
          s_store_name
HAVING SUM(netpaid) > (SELECT 0.05 * AVG(netpaid)
                       FROM   ssales);

WITH ssales
     AS (SELECT c_last_name,
                c_first_name,
                s_store_name,
                ca_state,
                s_state,
                i_color,
                i_current_price,
                i_manager_id,
                i_units,
                i_size,
                SUM(ss_sales_price) netpaid
         FROM   store_sales,
                store_returns,
                store,
                item,
                customer,
                customer_address
         WHERE  ss_ticket_number = sr_ticket_number
                AND ss_item_sk = sr_item_sk
                AND ss_customer_sk = c_customer_sk
                AND ss_item_sk = i_item_sk
                AND ss_store_sk = s_store_sk
                AND c_birth_country = UPPER(ca_country)
                AND s_zip = ca_zip
                AND s_market_id = 8
         GROUP  BY c_last_name,
                   c_first_name,
                   s_store_name,
                   ca_state,
                   s_state,
                   i_color,
                   i_current_price,
                   i_manager_id,
                   i_units,
                   i_size)
SELECT c_last_name,
       c_first_name,
       s_store_name,
       SUM(netpaid) paid
FROM   ssales
WHERE  i_color = 'chartreuse'
GROUP  BY c_last_name,
          c_first_name,
          s_store_name
HAVING SUM(netpaid) > (SELECT 0.05 * AVG(netpaid)
                       FROM   ssales);

-- query4.tpl
WITH year_total
     AS (SELECT c_customer_id
                customer_id
                   ,
                c_first_name
                    customer_first_name,
                c_last_name
                    customer_last_name,
                c_preferred_cust_flag,
                c_birth_country,
                c_login,
                c_email_address,
                d_year
                dyear,
                SUM(( ( ss_ext_list_price - ss_ext_wholesale_cost - ss_ext_discount_amt ) + ss_ext_sales_price ) / 2)
                year_total
                   ,
                's'
                   sale_type
         FROM   customer,
                store_sales,
                date_dim
         WHERE  c_customer_sk = ss_customer_sk
                AND ss_sold_date_sk = d_date_sk
         GROUP  BY c_customer_id,
                   c_first_name,
                   c_last_name,
                   c_preferred_cust_flag,
                   c_birth_country,
                   c_login,
                   c_email_address,
                   d_year
         UNION ALL
         SELECT c_customer_id
                customer_id,
                c_first_name
                customer_first_name,
                c_last_name
                customer_last_name,
                c_preferred_cust_flag,
                c_birth_country,
                c_login,
                c_email_address,
                d_year
                dyear,
                SUM(( ( ( cs_ext_list_price - cs_ext_wholesale_cost - cs_ext_discount_amt ) + cs_ext_sales_price ) / 2 )
                )
                year_total,
                'c'
                sale_type
         FROM   customer,
                catalog_sales,
                date_dim
         WHERE  c_customer_sk = cs_bill_customer_sk
                AND cs_sold_date_sk = d_date_sk
         GROUP  BY c_customer_id,
                   c_first_name,
                   c_last_name,
                   c_preferred_cust_flag,
                   c_birth_country,
                   c_login,
                   c_email_address,
                   d_year)
SELECT t_s_secyear.customer_id,
       t_s_secyear.customer_first_name,
       t_s_secyear.customer_last_name,
       t_s_secyear.c_preferred_cust_flag,
       t_s_secyear.c_birth_country,
       t_s_secyear.c_login
FROM   year_total t_s_firstyear,
       year_total t_s_secyear,
       year_total t_c_firstyear,
       year_total t_c_secyear
WHERE  t_s_secyear.customer_id = t_s_firstyear.customer_id
       AND t_s_firstyear.customer_id = t_c_secyear.customer_id
       AND t_s_firstyear.customer_id = t_c_firstyear.customer_id
       AND t_s_firstyear.sale_type = 's'
       AND t_c_firstyear.sale_type = 'c'
       AND t_s_secyear.sale_type = 's'
       AND t_c_secyear.sale_type = 'c'
       AND t_s_firstyear.dyear = 1998
       AND t_s_secyear.dyear = 1998 + 1
       AND t_c_firstyear.dyear = 1998
       AND t_c_secyear.dyear = 1998 + 1
       AND t_s_firstyear.year_total > 0
       AND t_c_firstyear.year_total > 0
       AND CASE
             WHEN t_c_firstyear.year_total > 0 THEN t_c_secyear.year_total / t_c_firstyear.year_total
             ELSE NULL
           END > CASE
                   WHEN t_s_firstyear.year_total > 0 THEN t_s_secyear.year_total / t_s_firstyear.year_total
                   ELSE NULL
                 END
ORDER  BY t_s_secyear.customer_id,
          t_s_secyear.customer_first_name,
          t_s_secyear.customer_last_name,
          t_s_secyear.c_preferred_cust_flag,
          t_s_secyear.c_birth_country,
          t_s_secyear.c_login;

-- query99.tpl
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
FROM   catalog_sales,
       warehouse,
       ship_mode,
       call_center,
       date_dim
WHERE  EXTRACT (YEAR FROM d_date) = 1998
       AND cs_ship_date_sk = d_date_sk
       AND cs_warehouse_sk = w_warehouse_sk
       AND cs_ship_mode_sk = sm_ship_mode_sk
       AND cs_call_center_sk = cc_call_center_sk
GROUP  BY SUBSTR(w_warehouse_name, 1, 20),
          sm_type,
          cc_name
ORDER  BY SUBSTR(w_warehouse_name, 1, 20),
          sm_type,
          cc_name;

-- query68.tpl
SELECT c_last_name,
       c_first_name,
       ca_city,
       bought_city,
       ss_ticket_number,
       extended_price,
       extended_tax,
       list_price
FROM   (SELECT ss_ticket_number,
               ss_customer_sk,
               ca_city                 bought_city,
               SUM(ss_ext_sales_price) extended_price,
               SUM(ss_ext_list_price)  list_price,
               SUM(ss_ext_tax)         extended_tax
        FROM   store_sales,
               date_dim,
               store,
               household_demographics,
               customer_address
        WHERE  store_sales.ss_sold_date_sk = date_dim.d_date_sk
               AND store_sales.ss_store_sk = store.s_store_sk
               AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
               AND store_sales.ss_addr_sk = customer_address.ca_address_sk
               AND date_dim.d_dom BETWEEN 1 AND 2
               AND ( household_demographics.hd_dep_count = 7
                      OR household_demographics.hd_vehicle_count = 2 )
               AND date_dim.d_year IN ( 1999, 1999 + 1, 1999 + 2 )
               AND store.s_city IN ( 'Riverside', 'Midway' )
        GROUP  BY ss_ticket_number,
                  ss_customer_sk,
                  ss_addr_sk,
                  ca_city) dn,
       customer,
       customer_address current_addr
WHERE  ss_customer_sk = c_customer_sk
       AND customer.c_current_addr_sk = current_addr.ca_address_sk
       AND current_addr.ca_city <> bought_city
ORDER  BY c_last_name,
          ss_ticket_number;

-- query83.tpl
WITH sr_items
     AS (SELECT i_item_id               item_id,
                SUM(sr_return_quantity) sr_item_qty
         FROM   store_returns,
                item,
                date_dim
         WHERE  sr_item_sk = i_item_sk
                AND d_date IN (SELECT d_date
                               FROM   date_dim
                               WHERE  d_week_seq IN (SELECT d_week_seq
                                                     FROM   date_dim
                                                     WHERE  d_date IN ( '1998-03-21', '1998-09-26', '1998-11-15' )))
                AND sr_returned_date_sk = d_date_sk
         GROUP  BY i_item_id),
     cr_items
     AS (SELECT i_item_id               item_id,
                SUM(cr_return_quantity) cr_item_qty
         FROM   catalog_returns,
                item,
                date_dim
         WHERE  cr_item_sk = i_item_sk
                AND d_date IN (SELECT d_date
                               FROM   date_dim
                               WHERE  d_week_seq IN (SELECT d_week_seq
                                                     FROM   date_dim
                                                     WHERE  d_date IN ( '1998-03-21', '1998-09-26', '1998-11-15' )))
                AND cr_returned_date_sk = d_date_sk
         GROUP  BY i_item_id),
     wr_items
     AS (SELECT i_item_id               item_id,
                SUM(wr_return_quantity) wr_item_qty
         FROM   web_returns,
                item,
                date_dim
         WHERE  wr_item_sk = i_item_sk
                AND d_date IN (SELECT d_date
                               FROM   date_dim
                               WHERE  d_week_seq IN (SELECT d_week_seq
                                                     FROM   date_dim
                                                     WHERE  d_date IN ( '1998-03-21', '1998-09-26', '1998-11-15' )))
                AND wr_returned_date_sk = d_date_sk
         GROUP  BY i_item_id)
SELECT sr_items.item_id,
       sr_item_qty,
       sr_item_qty / ( sr_item_qty + cr_item_qty + wr_item_qty ) / 3.0 * 100 sr_dev,
       cr_item_qty,
       cr_item_qty / ( sr_item_qty + cr_item_qty + wr_item_qty ) / 3.0 * 100 cr_dev,
       wr_item_qty,
       wr_item_qty / ( sr_item_qty + cr_item_qty + wr_item_qty ) / 3.0 * 100 wr_dev,
       ( sr_item_qty + cr_item_qty + wr_item_qty ) / 3.0                     average
FROM   sr_items,
       cr_items,
       wr_items
WHERE  sr_items.item_id = cr_items.item_id
       AND sr_items.item_id = wr_items.item_id
ORDER  BY sr_items.item_id,
          sr_item_qty;

-- query61.tpl
SELECT promotions,
       total,
       CAST(promotions AS DECIMAL(15, 4)) / CAST(total AS DECIMAL(15, 4)) * 100
FROM   (SELECT SUM(ss_ext_sales_price) promotions
        FROM   store_sales,
               store,
               promotion,
               date_dim,
               customer,
               customer_address,
               item
        WHERE  ss_sold_date_sk = d_date_sk
               AND ss_store_sk = s_store_sk
               AND ss_promo_sk = p_promo_sk
               AND ss_customer_sk = c_customer_sk
               AND ca_address_sk = c_current_addr_sk
               AND ss_item_sk = i_item_sk
               AND ca_gmt_offset = -6
               AND i_category = 'Books'
               AND ( p_channel_dmail = 'Y'
                      OR p_channel_email = 'Y'
                      OR p_channel_tv = 'Y' )
               AND s_gmt_offset = -6
               AND d_year = 1999
               AND d_moy = 12) promotional_sales,
       (SELECT SUM(ss_ext_sales_price) total
        FROM   store_sales,
               store,
               date_dim,
               customer,
               customer_address,
               item
        WHERE  ss_sold_date_sk = d_date_sk
               AND ss_store_sk = s_store_sk
               AND ss_customer_sk = c_customer_sk
               AND ca_address_sk = c_current_addr_sk
               AND ss_item_sk = i_item_sk
               AND ca_gmt_offset = -6
               AND i_category = 'Books'
               AND s_gmt_offset = -6
               AND d_year = 1999
               AND d_moy = 12) all_sales
ORDER  BY promotions,
          total;

-- query5.tpl
WITH ssr
     AS (SELECT s_store_id,
                SUM(sales_price) AS sales,
                SUM(profit)      AS profit,
                SUM(return_amt)  AS RETURNS,
                SUM(net_loss)    AS profit_loss
         FROM   (SELECT ss_store_sk              AS store_sk,
                        ss_sold_date_sk          AS date_sk,
                        ss_ext_sales_price       AS sales_price,
                        ss_net_profit            AS profit,
                        CAST(0 AS DECIMAL(7, 2)) AS return_amt,
                        CAST(0 AS DECIMAL(7, 2)) AS net_loss
                 FROM   store_sales
                 UNION ALL
                 SELECT sr_store_sk              AS store_sk,
                        sr_returned_date_sk      AS date_sk,
                        CAST(0 AS DECIMAL(7, 2)) AS sales_price,
                        CAST(0 AS DECIMAL(7, 2)) AS profit,
                        sr_return_amt            AS return_amt,
                        sr_net_loss              AS net_loss
                 FROM   store_returns) salesreturns,
                date_dim,
                store
         WHERE  date_sk = d_date_sk
                AND d_date BETWEEN CAST('1998-08-16' AS DATE) AND ( CAST('1998-08-16' AS DATE) + 14 DAYS )
                AND store_sk = s_store_sk
         GROUP  BY s_store_id),
     csr
     AS (SELECT cp_catalog_page_id,
                SUM(sales_price) AS sales,
                SUM(profit)      AS profit,
                SUM(return_amt)  AS RETURNS,
                SUM(net_loss)    AS profit_loss
         FROM   (SELECT cs_catalog_page_sk       AS page_sk,
                        cs_sold_date_sk          AS date_sk,
                        cs_ext_sales_price       AS sales_price,
                        cs_net_profit            AS profit,
                        CAST(0 AS DECIMAL(7, 2)) AS return_amt,
                        CAST(0 AS DECIMAL(7, 2)) AS net_loss
                 FROM   catalog_sales
                 UNION ALL
                 SELECT cr_catalog_page_sk       AS page_sk,
                        cr_returned_date_sk      AS date_sk,
                        CAST(0 AS DECIMAL(7, 2)) AS sales_price,
                        CAST(0 AS DECIMAL(7, 2)) AS profit,
                        cr_return_amount         AS return_amt,
                        cr_net_loss              AS net_loss
                 FROM   catalog_returns) salesreturns,
                date_dim,
                catalog_page
         WHERE  date_sk = d_date_sk
                AND d_date BETWEEN CAST('1998-08-16' AS DATE) AND ( CAST('1998-08-16' AS DATE) + 14 DAYS )
                AND page_sk = cp_catalog_page_sk
         GROUP  BY cp_catalog_page_id),
     wsr
     AS (SELECT web_site_id,
                SUM(sales_price) AS sales,
                SUM(profit)      AS profit,
                SUM(return_amt)  AS RETURNS,
                SUM(net_loss)    AS profit_loss
         FROM   (SELECT ws_web_site_sk           AS wsr_web_site_sk,
                        ws_sold_date_sk          AS date_sk,
                        ws_ext_sales_price       AS sales_price,
                        ws_net_profit            AS profit,
                        CAST(0 AS DECIMAL(7, 2)) AS return_amt,
                        CAST(0 AS DECIMAL(7, 2)) AS net_loss
                 FROM   web_sales
                 UNION ALL
                 SELECT ws_web_site_sk           AS wsr_web_site_sk,
                        wr_returned_date_sk      AS date_sk,
                        CAST(0 AS DECIMAL(7, 2)) AS sales_price,
                        CAST(0 AS DECIMAL(7, 2)) AS profit,
                        wr_return_amt            AS return_amt,
                        wr_net_loss              AS net_loss
                 FROM   web_returns
                        LEFT OUTER JOIN web_sales
                          ON ( wr_item_sk = ws_item_sk
                               AND wr_order_number = ws_order_number )) salesreturns,
                date_dim,
                web_site
         WHERE  date_sk = d_date_sk
                AND d_date BETWEEN CAST('1998-08-16' AS DATE) AND ( CAST('1998-08-16' AS DATE) + 14 DAYS )
                AND wsr_web_site_sk = web_site_sk
         GROUP  BY web_site_id)
SELECT channel,
       id,
       SUM(sales)   AS sales,
       SUM(RETURNS) AS RETURNS,
       SUM(profit)  AS profit
FROM   (SELECT 'store channel'          AS channel,
               'store'
                || s_store_id           AS id,
               sales,
               RETURNS,
               ( profit - profit_loss ) AS profit
        FROM   ssr
        UNION ALL
        SELECT 'catalog channel'        AS channel,
               'catalog_page'
                || cp_catalog_page_id   AS id,
               sales,
               RETURNS,
               ( profit - profit_loss ) AS profit
        FROM   csr
        UNION ALL
        SELECT 'web channel'            AS channel,
               'web_site'
                || web_site_id          AS id,
               sales,
               RETURNS,
               ( profit - profit_loss ) AS profit
        FROM   wsr) x
GROUP  BY ROLLUP ( channel, id )
ORDER  BY channel,
          id;

-- query76.tpl
SELECT channel,
       col_name,
       d_year,
       d_qoy,
       i_category,
       COUNT(*)             sales_cnt,
       SUM(ext_sales_price) sales_amt
FROM   (SELECT 'store'            AS channel,
               'ss_addr_sk'       col_name,
               d_year,
               d_qoy,
               i_category,
               ss_ext_sales_price ext_sales_price
        FROM   store_sales,
               item,
               date_dim
        WHERE  ss_addr_sk IS NULL
               AND ss_sold_date_sk = d_date_sk
               AND ss_item_sk = i_item_sk
        UNION ALL
        SELECT 'web'              AS channel,
               'ws_web_site_sk'   col_name,
               d_year,
               d_qoy,
               i_category,
               ws_ext_sales_price ext_sales_price
        FROM   web_sales,
               item,
               date_dim
        WHERE  ws_web_site_sk IS NULL
               AND ws_sold_date_sk = d_date_sk
               AND ws_item_sk = i_item_sk
        UNION ALL
        SELECT 'catalog'             AS channel,
               'cs_ship_customer_sk' col_name,
               d_year,
               d_qoy,
               i_category,
               cs_ext_sales_price    ext_sales_price
        FROM   catalog_sales,
               item,
               date_dim
        WHERE  cs_ship_customer_sk IS NULL
               AND cs_sold_date_sk = d_date_sk
               AND cs_item_sk = i_item_sk) foo
GROUP  BY channel,
          col_name,
          d_year,
          d_qoy,
          i_category
ORDER  BY channel,
          col_name,
          d_year,
          d_qoy,
          i_category; 
