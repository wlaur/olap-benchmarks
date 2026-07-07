select
         w_warehouse_name
  ,w_warehouse_sq_ft
  ,w_city
  ,w_county
  ,w_state
  ,w_country
        ,ship_carriers
        ,year_
  ,sum(x_jan_sales) as jan_sales
  ,sum(x_feb_sales) as feb_sales
  ,sum(x_mar_sales) as mar_sales
  ,sum(x_apr_sales) as apr_sales
  ,sum(x_may_sales) as may_sales
  ,sum(x_jun_sales) as jun_sales
  ,sum(x_jul_sales) as jul_sales
  ,sum(x_aug_sales) as aug_sales
  ,sum(x_sep_sales) as sep_sales
  ,sum(x_oct_sales) as oct_sales
  ,sum(x_nov_sales) as nov_sales
  ,sum(x_dec_sales) as dec_sales
  ,sum(x_jan_sales/w_warehouse_sq_ft) as jan_sales_per_sq_foot
  ,sum(x_feb_sales/w_warehouse_sq_ft) as feb_sales_per_sq_foot
  ,sum(x_mar_sales/w_warehouse_sq_ft) as mar_sales_per_sq_foot
  ,sum(x_apr_sales/w_warehouse_sq_ft) as apr_sales_per_sq_foot
  ,sum(x_may_sales/w_warehouse_sq_ft) as may_sales_per_sq_foot
  ,sum(x_jun_sales/w_warehouse_sq_ft) as jun_sales_per_sq_foot
  ,sum(x_jul_sales/w_warehouse_sq_ft) as jul_sales_per_sq_foot
  ,sum(x_aug_sales/w_warehouse_sq_ft) as aug_sales_per_sq_foot
  ,sum(x_sep_sales/w_warehouse_sq_ft) as sep_sales_per_sq_foot
  ,sum(x_oct_sales/w_warehouse_sq_ft) as oct_sales_per_sq_foot
  ,sum(x_nov_sales/w_warehouse_sq_ft) as nov_sales_per_sq_foot
  ,sum(x_dec_sales/w_warehouse_sq_ft) as dec_sales_per_sq_foot
  ,sum(x_jan_net) as jan_net
  ,sum(x_feb_net) as feb_net
  ,sum(x_mar_net) as mar_net
  ,sum(x_apr_net) as apr_net
  ,sum(x_may_net) as may_net
  ,sum(x_jun_net) as jun_net
  ,sum(x_jul_net) as jul_net
  ,sum(x_aug_net) as aug_net
  ,sum(x_sep_net) as sep_net
  ,sum(x_oct_net) as oct_net
  ,sum(x_nov_net) as nov_net
  ,sum(x_dec_net) as dec_net
 from (
     select
  w_warehouse_name
  ,w_warehouse_sq_ft
  ,w_city
  ,w_county
  ,w_state
  ,w_country
  ,'DHL,BARIAN' as ship_carriers
       ,d_year as year_
  ,sum(case when d_moy = 1
    then ws_ext_sales_price* ws_quantity else 0 end) as x_jan_sales
  ,sum(case when d_moy = 2
    then ws_ext_sales_price* ws_quantity else 0 end) as x_feb_sales
  ,sum(case when d_moy = 3
    then ws_ext_sales_price* ws_quantity else 0 end) as x_mar_sales
  ,sum(case when d_moy = 4
    then ws_ext_sales_price* ws_quantity else 0 end) as x_apr_sales
  ,sum(case when d_moy = 5
    then ws_ext_sales_price* ws_quantity else 0 end) as x_may_sales
  ,sum(case when d_moy = 6
    then ws_ext_sales_price* ws_quantity else 0 end) as x_jun_sales
  ,sum(case when d_moy = 7
    then ws_ext_sales_price* ws_quantity else 0 end) as x_jul_sales
  ,sum(case when d_moy = 8
    then ws_ext_sales_price* ws_quantity else 0 end) as x_aug_sales
  ,sum(case when d_moy = 9
    then ws_ext_sales_price* ws_quantity else 0 end) as x_sep_sales
  ,sum(case when d_moy = 10
    then ws_ext_sales_price* ws_quantity else 0 end) as x_oct_sales
  ,sum(case when d_moy = 11
    then ws_ext_sales_price* ws_quantity else 0 end) as x_nov_sales
  ,sum(case when d_moy = 12
    then ws_ext_sales_price* ws_quantity else 0 end) as x_dec_sales
  ,sum(case when d_moy = 1
    then ws_net_paid * ws_quantity else 0 end) as x_jan_net
  ,sum(case when d_moy = 2
    then ws_net_paid * ws_quantity else 0 end) as x_feb_net
  ,sum(case when d_moy = 3
    then ws_net_paid * ws_quantity else 0 end) as x_mar_net
  ,sum(case when d_moy = 4
    then ws_net_paid * ws_quantity else 0 end) as x_apr_net
  ,sum(case when d_moy = 5
    then ws_net_paid * ws_quantity else 0 end) as x_may_net
  ,sum(case when d_moy = 6
    then ws_net_paid * ws_quantity else 0 end) as x_jun_net
  ,sum(case when d_moy = 7
    then ws_net_paid * ws_quantity else 0 end) as x_jul_net
  ,sum(case when d_moy = 8
    then ws_net_paid * ws_quantity else 0 end) as x_aug_net
  ,sum(case when d_moy = 9
    then ws_net_paid * ws_quantity else 0 end) as x_sep_net
  ,sum(case when d_moy = 10
    then ws_net_paid * ws_quantity else 0 end) as x_oct_net
  ,sum(case when d_moy = 11
    then ws_net_paid * ws_quantity else 0 end) as x_nov_net
  ,sum(case when d_moy = 12
    then ws_net_paid * ws_quantity else 0 end) as x_dec_net
     from
          web_sales
         ,warehouse
         ,date_dim
         ,time_dim
    ,ship_mode
     where
            ws_warehouse_sk =  w_warehouse_sk
        and ws_sold_date_sk = d_date_sk
        and ws_sold_time_sk = t_time_sk
  and ws_ship_mode_sk = sm_ship_mode_sk
        and d_year = 2001
  and t_time between 30838 and 30838+28800
  and sm_carrier in ('DHL','BARIAN')
     group by
        w_warehouse_name
  ,w_warehouse_sq_ft
  ,w_city
  ,w_county
  ,w_state
  ,w_country
       ,d_year
 union all
     select
  w_warehouse_name
  ,w_warehouse_sq_ft
  ,w_city
  ,w_county
  ,w_state
  ,w_country
  ,'DHL,BARIAN' as ship_carriers
       ,d_year as year_
  ,sum(case when d_moy = 1
    then cs_sales_price* cs_quantity else 0 end) as x_jan_sales
  ,sum(case when d_moy = 2
    then cs_sales_price* cs_quantity else 0 end) as x_feb_sales
  ,sum(case when d_moy = 3
    then cs_sales_price* cs_quantity else 0 end) as x_mar_sales
  ,sum(case when d_moy = 4
    then cs_sales_price* cs_quantity else 0 end) as x_apr_sales
  ,sum(case when d_moy = 5
    then cs_sales_price* cs_quantity else 0 end) as x_may_sales
  ,sum(case when d_moy = 6
    then cs_sales_price* cs_quantity else 0 end) as x_jun_sales
  ,sum(case when d_moy = 7
    then cs_sales_price* cs_quantity else 0 end) as x_jul_sales
  ,sum(case when d_moy = 8
    then cs_sales_price* cs_quantity else 0 end) as x_aug_sales
  ,sum(case when d_moy = 9
    then cs_sales_price* cs_quantity else 0 end) as x_sep_sales
  ,sum(case when d_moy = 10
    then cs_sales_price* cs_quantity else 0 end) as x_oct_sales
  ,sum(case when d_moy = 11
    then cs_sales_price* cs_quantity else 0 end) as x_nov_sales
  ,sum(case when d_moy = 12
    then cs_sales_price* cs_quantity else 0 end) as x_dec_sales
  ,sum(case when d_moy = 1
    then cs_net_paid_inc_tax * cs_quantity else 0 end) as x_jan_net
  ,sum(case when d_moy = 2
    then cs_net_paid_inc_tax * cs_quantity else 0 end) as x_feb_net
  ,sum(case when d_moy = 3
    then cs_net_paid_inc_tax * cs_quantity else 0 end) as x_mar_net
  ,sum(case when d_moy = 4
    then cs_net_paid_inc_tax * cs_quantity else 0 end) as x_apr_net
  ,sum(case when d_moy = 5
    then cs_net_paid_inc_tax * cs_quantity else 0 end) as x_may_net
  ,sum(case when d_moy = 6
    then cs_net_paid_inc_tax * cs_quantity else 0 end) as x_jun_net
  ,sum(case when d_moy = 7
    then cs_net_paid_inc_tax * cs_quantity else 0 end) as x_jul_net
  ,sum(case when d_moy = 8
    then cs_net_paid_inc_tax * cs_quantity else 0 end) as x_aug_net
  ,sum(case when d_moy = 9
    then cs_net_paid_inc_tax * cs_quantity else 0 end) as x_sep_net
  ,sum(case when d_moy = 10
    then cs_net_paid_inc_tax * cs_quantity else 0 end) as x_oct_net
  ,sum(case when d_moy = 11
    then cs_net_paid_inc_tax * cs_quantity else 0 end) as x_nov_net
  ,sum(case when d_moy = 12
    then cs_net_paid_inc_tax * cs_quantity else 0 end) as x_dec_net
     from
          catalog_sales
         ,warehouse
         ,date_dim
         ,time_dim
   ,ship_mode
     where
            cs_warehouse_sk =  w_warehouse_sk
        and cs_sold_date_sk = d_date_sk
        and cs_sold_time_sk = t_time_sk
  and cs_ship_mode_sk = sm_ship_mode_sk
        and d_year = 2001
  and t_time between 30838 AND 30838+28800
  and sm_carrier in ('DHL','BARIAN')
     group by
        w_warehouse_name
  ,w_warehouse_sq_ft
  ,w_city
  ,w_county
  ,w_state
  ,w_country
       ,d_year
 ) x
 group by
        w_warehouse_name
  ,w_warehouse_sq_ft
  ,w_city
  ,w_county
  ,w_state
  ,w_country
  ,ship_carriers
       ,year_
 order by w_warehouse_name NULLS FIRST
LIMIT 100;
