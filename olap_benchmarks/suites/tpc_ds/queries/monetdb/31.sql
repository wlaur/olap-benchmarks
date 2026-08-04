WITH ss AS
  (SELECT ca_county,
          d_qoy,
          d_year,
          sum(ss_ext_sales_price) AS store_sales
   FROM store_sales,
        date_dim,
        customer_address
   WHERE ss_sold_date_sk = d_date_sk
     AND ss_addr_sk=ca_address_sk
   GROUP BY ca_county,
            d_qoy,
            d_year),
     ws AS
  (SELECT ca_county,
          d_qoy,
          d_year,
          sum(ws_ext_sales_price) AS web_sales
   FROM web_sales,
        date_dim,
        customer_address
   WHERE ws_sold_date_sk = d_date_sk
     AND ws_bill_addr_sk=ca_address_sk
   GROUP BY ca_county,
            d_qoy,
            d_year)
SELECT ss1.ca_county ,
       ss1.d_year ,
       cast(ws2.web_sales as double)/ws1.web_sales web_q1_q2_increase ,
       cast(ss2.store_sales as double)/ss1.store_sales store_q1_q2_increase ,
       cast(ws3.web_sales as double)/ws2.web_sales web_q2_q3_increase ,
       cast(ss3.store_sales as double)/ss2.store_sales store_q2_q3_increase
FROM ss ss1 ,
     ss ss2 ,
     ss ss3 ,
     ws ws1 ,
     ws ws2 ,
     ws ws3
WHERE ss1.d_qoy = 1
  AND ss1.d_year = 2000
  AND ss1.ca_county = ss2.ca_county
  AND ss2.d_qoy = 2
  AND ss2.d_year = 2000
  AND ss2.ca_county = ss3.ca_county
  AND ss3.d_qoy = 3
  AND ss3.d_year = 2000
  AND ss1.ca_county = ws1.ca_county
  AND ws1.d_qoy = 1
  AND ws1.d_year = 2000
  AND ws1.ca_county = ws2.ca_county
  AND ws2.d_qoy = 2
  AND ws2.d_year = 2000
  AND ws1.ca_county = ws3.ca_county
  AND ws3.d_qoy = 3
  AND ws3.d_year = 2000
  AND CASE
          WHEN ws1.web_sales > 0 THEN cast(ws2.web_sales as double)/ws1.web_sales
          ELSE NULL
      END > CASE
                WHEN ss1.store_sales > 0 THEN cast(ss2.store_sales as double)/ss1.store_sales
                ELSE NULL
            END
  AND CASE
          WHEN ws2.web_sales > 0 THEN cast(ws3.web_sales as double)/ws2.web_sales
          ELSE NULL
      END > CASE
                WHEN ss2.store_sales > 0 THEN cast(ss3.store_sales as double)/ss2.store_sales
                ELSE NULL
            END
ORDER BY ss1.ca_county;
