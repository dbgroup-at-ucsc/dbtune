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
