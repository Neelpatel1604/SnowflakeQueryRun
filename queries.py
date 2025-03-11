queries = [
    ("simple_select", """
    SELECT * 
    FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER"
    LIMIT 10
    """),
    
    ("another_query", """
    SELECT * 
    FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_SALES"
    LIMIT 5
    """),

    # ("full_join_rank", """
    # SELECT c.c_customer_sk, 
    #        ss.ss_item_sk,
    #        RANK() OVER (PARTITION BY c.c_birth_year ORDER BY ss.ss_sales_price DESC) AS rank_value
    # FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER" c
    # FULL JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_SALES" ss 
    #     ON c.c_customer_sk = ss.ss_customer_sk
    # WHERE ss.ss_quantity BETWEEN 10 AND 100
    # ORDER BY rank_value;
    # """),

    # ("lead_having", """
    # SELECT c.c_customer_sk,
    #        LEAD(ss.ss_sales_price) OVER (PARTITION BY c.c_birth_year ORDER BY ss.ss_sold_date_sk) AS next_sale,
    #        SUM(ss.ss_sales_price) AS total
    # FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER" c
    # INNER JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_SALES" ss 
    #     ON c.c_customer_sk = ss.ss_customer_sk
    # GROUP BY c.c_customer_sk, ss.ss_sales_price, c.c_birth_year, ss.ss_sold_date_sk
    # HAVING SUM(ss.ss_sales_price) > 500
    # ORDER BY total DESC;
    # """),

    # ("lag_case_group", """
    # SELECT c.c_customer_sk,
    #        LAG(ss.ss_sales_price) OVER (PARTITION BY c.c_birth_country ORDER BY ss.ss_sold_date_sk) AS prev_sale,
    #        CASE 
    #            WHEN ss.ss_quantity > 50 THEN 'High'
    #            ELSE 'Low'
    #        END AS qty_category
    # FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER" c
    # INNER JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_SALES" ss 
    #     ON c.c_customer_sk = ss.ss_customer_sk
    # GROUP BY c.c_customer_sk, ss.ss_sales_price, c.c_birth_country, ss.ss_sold_date_sk, ss.ss_quantity
    # ORDER BY c.c_customer_sk;
    # """),

    # ("dense_rank_coalesce", """
    # SELECT c.c_customer_sk,
    #        COALESCE(ss.ss_store_sk, 0) AS store_id,
    #        DENSE_RANK() OVER (PARTITION BY c.c_birth_year ORDER BY ss.ss_sales_price) AS dense_rank_value
    # FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER" c
    # LEFT JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_SALES" ss 
    #     ON c.c_customer_sk = ss.ss_customer_sk
    # WHERE c.c_birth_month IS NOT NULL
    # ORDER BY dense_rank_value;
    # """),

    # ("row_number_exists", """
    # SELECT c.c_customer_sk,
    #        ROW_NUMBER() OVER (PARTITION BY c.c_birth_country ORDER BY ss.ss_sold_date_sk) AS row_num
    # FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER" c
    # INNER JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_SALES" ss 
    #     ON c.c_customer_sk = ss.ss_customer_sk
    # WHERE EXISTS (
    #     SELECT 1
    #     FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE" s
    #     WHERE s.s_store_sk = ss.ss_store_sk
    #     AND s.s_store_sk > 1000
    # )
    # ORDER BY row_num;
    # """),

    # ("not_exists_sum_over", """
    # SELECT c.c_customer_sk,
    #        SUM(ss.ss_sales_price) OVER (PARTITION BY c.c_birth_year ORDER BY ss.ss_sold_date_sk) AS running_total
    # FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER" c
    # INNER JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_SALES" ss 
    #     ON c.c_customer_sk = ss.ss_customer_sk
    # WHERE NOT EXISTS (
    #     SELECT 1
    #     FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_RETURNS" sr
    #     WHERE sr.sr_customer_sk = c.c_customer_sk
    # )
    # GROUP BY c.c_customer_sk, ss.ss_sales_price, c.c_birth_year, ss.ss_sold_date_sk
    # ORDER BY running_total DESC;
    # """),

    # ("string_agg_full_join", """
    # SELECT c.c_customer_sk,
    #        STRING_AGG(ss.ss_item_sk::STRING, ', ') AS item_list
    # FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER" c
    # FULL JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_SALES" ss 
    #     ON c.c_customer_sk = ss.ss_customer_sk
    # WHERE c.c_last_name LIKE '%son'
    # GROUP BY c.c_customer_sk
    # HAVING COUNT(*) > 10
    # ORDER BY c.c_customer_sk;
    # """),

    # ("date_trunc_window", """
    # SELECT DATE_TRUNC('month', d.d_date) AS month_start,
    #        SUM(ss.ss_sales_price) OVER (PARTITION BY DATE_TRUNC('month', d.d_date)) AS monthly_total
    # FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_SALES" ss
    # INNER JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."DATE_DIM" d 
    #     ON ss.ss_sold_date_sk = d.d_date_sk
    # WHERE ss.ss_quantity IN (10, 20, 30)
    # GROUP BY d.d_date, ss.ss_sales_price, ss.ss_quantity
    # ORDER BY month_start;
    # """),

    # ("extract_rank_join", """
    # SELECT c.c_customer_sk,
    #        EXTRACT(YEAR FROM d.d_date) AS sale_year,
    #        RANK() OVER (PARTITION BY c.c_birth_country ORDER BY ss.ss_sales_price DESC) AS rank_value
    # FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER" c
    # INNER JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_SALES" ss 
    #     ON c.c_customer_sk = ss.ss_customer_sk
    # INNER JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."DATE_DIM" d 
    #     ON ss.ss_sold_date_sk = d.d_date_sk
    # WHERE ss.ss_quantity > 0
    # ORDER BY sale_year;
    # """),

    # ("cte_lead_having", """
    # WITH cte AS (
    #     SELECT c.c_customer_sk,
    #            LEAD(ss.ss_sales_price) OVER (PARTITION BY c.c_birth_year ORDER BY ss.ss_sold_date_sk) AS next_sale,
    #            SUM(ss.ss_sales_price) AS total
    #     FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER" c
    #     INNER JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_SALES" ss 
    #         ON c.c_customer_sk = ss.ss_customer_sk
    #     GROUP BY c.c_customer_sk, ss.ss_sales_price, c.c_birth_year, ss.ss_sold_date_sk
    # )
    # SELECT c_customer_sk, next_sale, total
    # FROM cte
    # WHERE total BETWEEN 100 AND 1000
    # HAVING MAX(next_sale) > 50
    # ORDER BY total DESC;
    # """),

    # ("full_join_dense_rank", """
    # SELECT c.c_customer_sk,
    #        ss.ss_item_sk,
    #        DENSE_RANK() OVER (PARTITION BY c.c_birth_year ORDER BY ss.ss_quantity) AS dense_rank_value
    # FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER" c
    # FULL JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_SALES" ss 
    #     ON c.c_customer_sk = ss.ss_customer_sk
    # WHERE ss.ss_sales_price IS NOT NULL
    # GROUP BY c.c_customer_sk, ss.ss_item_sk, c.c_birth_year, ss.ss_quantity
    # ORDER BY dense_rank_value;
    # """),

    # ("case_lag_agg", """
    # SELECT c.c_customer_sk,
    #        LAG(ss.ss_sales_price) OVER (PARTITION BY c.c_birth_country ORDER BY ss.ss_sold_date_sk) AS prev_sale,
    #        CASE 
    #            WHEN SUM(ss.ss_quantity) > 100 THEN 'High'
    #            ELSE 'Low'
    #        END AS qty_category
    # FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER" c
    # INNER JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_SALES" ss 
    #     ON c.c_customer_sk = ss.ss_customer_sk
    # GROUP BY c.c_customer_sk, ss.ss_sales_price, c.c_birth_country, ss.ss_sold_date_sk
    # HAVING COUNT(*) > 5
    # ORDER BY c.c_customer_sk;
    # """),

    # ("coalesce_row_number", """
    # SELECT COALESCE(c.c_first_name, 'Unknown') AS first_name,
    #        ROW_NUMBER() OVER (PARTITION BY c.c_birth_year ORDER BY ss.ss_sold_date_sk) AS row_num
    # FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER" c
    # INNER JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_SALES" ss 
    #     ON c.c_customer_sk = ss.ss_customer_sk
    # WHERE c.c_last_name LIKE '%er'
    # GROUP BY c.c_first_name, c.c_birth_year, ss.ss_sold_date_sk
    # ORDER BY row_num;
    # """),

    # ("union_window", """
    # SELECT c.c_customer_sk,
    #        SUM(ss.ss_sales_price) OVER (PARTITION BY c.c_birth_year) AS total_sales
    # FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER" c
    # INNER JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_SALES" ss 
    #     ON c.c_customer_sk = ss.ss_customer_sk
    # WHERE ss.ss_quantity > 50
    # UNION
    # SELECT c.c_customer_sk,
    #        SUM(ss.ss_sales_price) OVER (PARTITION BY c.c_birth_year) AS total_sales
    # FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER" c
    # INNER JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_SALES" ss 
    #     ON c.c_customer_sk = ss.ss_customer_sk
    # WHERE ss.ss_quantity < 20
    # ORDER BY total_sales DESC;
    # """),

    # ("union_all_rank", """
    # SELECT c.c_customer_sk,
    #        RANK() OVER (PARTITION BY c.c_birth_country ORDER BY ss.ss_sales_price DESC) AS rank_value
    # FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER" c
    # INNER JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_SALES" ss 
    #     ON c.c_customer_sk = ss.ss_customer_sk
    # UNION ALL
    # SELECT c.c_customer_sk,
    #        RANK() OVER (PARTITION BY c.c_birth_country ORDER BY ss.ss_sales_price DESC) AS rank_value
    # FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER" c
    # INNER JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_SALES" ss 
    #     ON c.c_customer_sk = ss.ss_customer_sk
    # ORDER BY rank_value;
    # """),

    # ("left_join_lead", """
    # SELECT c.c_customer_sk,
    #        ss.ss_item_sk,
    #        LEAD(ss.ss_sales_price) OVER (PARTITION BY c.c_birth_year ORDER BY ss.ss_sold_date_sk) AS next_sale
    # FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER" c
    # LEFT JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_SALES" ss 
    #     ON c.c_customer_sk = ss.ss_customer_sk
    # WHERE c.c_birth_month IN (1, 2, 3)
    # GROUP BY c.c_customer_sk, ss.ss_item_sk, ss.ss_sales_price, c.c_birth_year, ss.ss_sold_date_sk
    # ORDER BY next_sale;
    # """),

    # ("string_agg_exists", """
    # SELECT c.c_customer_sk,
    #        STRING_AGG(ss.ss_item_sk::STRING, '; ') AS item_list
    # FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER" c
    # INNER JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_SALES" ss 
    #     ON c.c_customer_sk = ss.ss_customer_sk
    # WHERE EXISTS (
    #     SELECT 1
    #     FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."ITEM" i
    #     WHERE i.i_item_sk = ss.ss_item_sk
    # )
    # GROUP BY c.c_customer_sk
    # HAVING COUNT(*) > 5
    # ORDER BY c.c_customer_sk;
    # """),

    # ("date_trunc_dense_rank", """
    # SELECT DATE_TRUNC('month', d.d_date) AS month_start,
    #        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('month', d.d_date) ORDER BY ss.ss_sales_price) AS dense_rank_value
    # FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_SALES" ss
    # INNER JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."DATE_DIM" d 
    #     ON ss.ss_sold_date_sk = d.d_date_sk
    # WHERE ss.ss_quantity BETWEEN 10 AND 100
    # GROUP BY d.d_date, ss.ss_sales_price, ss.ss_quantity
    # ORDER BY month_start;
    # """),

    # ("extract_lag_join", """
    # SELECT c.c_customer_sk,
    #        EXTRACT(YEAR FROM d.d_date) AS sale_year,
    #        LAG(ss.ss_sales_price) OVER (PARTITION BY c.c_birth_country ORDER BY ss.ss_sold_date_sk) AS prev_sale
    # FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER" c
    # INNER JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_SALES" ss 
    #     ON c.c_customer_sk = ss.ss_customer_sk
    # INNER JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."DATE_DIM" d 
    #     ON ss.ss_sold_date_sk = d.d_date_sk
    # WHERE ss.ss_quantity > 0
    # ORDER BY sale_year;
    # """),

    # ("case_full_join_sum_over", """
    # SELECT c.c_customer_sk,
    #        CASE 
    #            WHEN ss.ss_item_sk IS NULL THEN 'No Sale'
    #            ELSE 'Sold'
    #        END AS sale_status,
    #        SUM(ss.ss_sales_price) OVER (PARTITION BY c.c_birth_year) AS running_total
    # FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER" c
    # FULL JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_SALES" ss 
    #     ON c.c_customer_sk = ss.ss_customer_sk
    # GROUP BY c.c_customer_sk, ss.ss_item_sk, ss.ss_sales_price, c.c_birth_year
    # ORDER BY running_total DESC;
    # """),

    # ("coalesce_row_number_having", """
    # SELECT COALESCE(c.c_last_name, 'Unknown') AS last_name,
    #        ROW_NUMBER() OVER (PARTITION BY c.c_birth_year ORDER BY ss.ss_sold_date_sk) AS row_num,
    #        SUM(ss.ss_sales_price) AS total
    # FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER" c
    # INNER JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_SALES" ss 
    #     ON c.c_customer_sk = ss.ss_customer_sk
    # GROUP BY c.c_last_name, c.c_birth_year, ss.ss_sold_date_sk, ss.ss_sales_price
    # HAVING SUM(ss.ss_sales_price) > 1000
    # ORDER BY row_num;
    # """),

    # ("lead_string_agg", """
    # SELECT c.c_customer_sk,
    #        LEAD(ss.ss_sales_price) OVER (PARTITION BY c.c_birth_country ORDER BY ss.ss_sold_date_sk) AS next_sale,
    #        STRING_AGG(ss.ss_item_sk::STRING, ', ') AS item_list
    # FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER" c
    # INNER JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_SALES" ss 
    #     ON c.c_customer_sk = ss.ss_customer_sk
    # WHERE c.c_first_name LIKE '%a%'
    # GROUP BY c.c_customer_sk, ss.ss_sales_price, c.c_birth_country, ss.ss_sold_date_sk, ss.ss_item_sk
    # ORDER BY next_sale;
    # """),

    # ("dense_rank_union", """
    # SELECT c.c_customer_sk,
    #        DENSE_RANK() OVER (PARTITION BY c.c_birth_year ORDER BY ss.ss_sales_price) AS dense_rank_value
    # FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER" c
    # INNER JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_SALES" ss 
    #     ON c.c_customer_sk = ss.ss_customer_sk
    # WHERE ss.ss_quantity > 50
    # UNION
    # SELECT c.c_customer_sk,
    #        DENSE_RANK() OVER (PARTITION BY c.c_birth_year ORDER BY ss.ss_sales_price) AS dense_rank_value
    # FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER" c
    # INNER JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_SALES" ss 
    #     ON c.c_customer_sk = ss.ss_customer_sk
    # WHERE ss.ss_quantity < 20
    # ORDER BY dense_rank_value;
    # """),

    # ("cte_not_exists_rank", """
    # WITH cte AS (
    #     SELECT c.c_customer_sk,
    #            RANK() OVER (PARTITION BY c.c_birth_country ORDER BY ss.ss_sales_price DESC) AS rank_value
    #     FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER" c
    #     INNER JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_SALES" ss 
    #         ON c.c_customer_sk = ss.ss_customer_sk
    #     WHERE NOT EXISTS (
    #         SELECT 1
    #         FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_RETURNS" sr
    #         WHERE sr.sr_customer_sk = c.c_customer_sk
    #     )
    # )
    # SELECT c_customer_sk, rank_value
    # FROM cte
    # GROUP BY c_customer_sk, rank_value
    # HAVING COUNT(*) > 2
    # ORDER BY rank_value;
    # """),

    # ("full_join_lead_case", """
    # SELECT c.c_customer_sk,
    #        ss.ss_store_sk,
    #        LEAD(ss.ss_sales_price) OVER (PARTITION BY c.c_birth_year ORDER BY ss.ss_sold_date_sk) AS next_sale,
    #        CASE 
    #            WHEN ss.ss_quantity > 100 THEN 'High'
    #            ELSE 'Low'
    #        END AS qty_category
    # FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER" c
    # FULL JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_SALES" ss 
    #     ON c.c_customer_sk = ss.ss_customer_sk
    # GROUP BY c.c_customer_sk, ss.ss_store_sk, ss.ss_sales_price, c.c_birth_year, ss.ss_sold_date_sk, ss.ss_quantity
    # ORDER BY next_sale;
    # """),

    # ("string_agg_date_trunc", """
    # SELECT DATE_TRUNC('month', d.d_date) AS month_start,
    #        STRING_AGG(ss.ss_item_sk::STRING, '; ') AS item_list,
    #        SUM(ss.ss_sales_price) AS total
    # FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_SALES" ss
    # INNER JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."DATE_DIM" d 
    #     ON ss.ss_sold_date_sk = d.d_date_sk
    # WHERE ss.ss_quantity IN (10, 20, 30)
    # GROUP BY d.d_date, ss.ss_item_sk, ss.ss_sales_price, ss.ss_quantity
    # HAVING SUM(ss.ss_sales_price) > 500
    # ORDER BY month_start;
    # """),

    # ("extract_row_number_join", """
    # SELECT c.c_customer_sk,
    #        EXTRACT(YEAR FROM d.d_date) AS sale_year,
    #        ROW_NUMBER() OVER (PARTITION BY c.c_birth_country ORDER BY ss.ss_sold_date_sk) AS row_num
    # FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER" c
    # LEFT JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_SALES" ss 
    #     ON c.c_customer_sk = ss.ss_customer_sk
    # INNER JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."DATE_DIM" d 
    #     ON ss.ss_sold_date_sk = d.d_date_sk
    # WHERE ss.ss_quantity BETWEEN 50 AND 150
    # GROUP BY c.c_customer_sk, d.d_date, c.c_birth_country, ss.ss_sold_date_sk
    # ORDER BY row_num;
    # """),

    # ("union_all_lag_coalesce", """
    # SELECT COALESCE(c.c_email_address, 'N/A') AS email,
    #        LAG(ss.ss_sales_price) OVER (PARTITION BY c.c_birth_year ORDER BY ss.ss_sold_date_sk) AS prev_sale
    # FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER" c
    # INNER JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_SALES" ss 
    #     ON c.c_customer_sk = ss.ss_customer_sk
    # WHERE ss.ss_quantity > 10
    # UNION ALL
    # SELECT COALESCE(c.c_email_address, 'N/A') AS email,
    #        LAG(ss.ss_sales_price) OVER (PARTITION BY c.c_birth_year ORDER BY ss.ss_sold_date_sk) AS prev_sale
    # FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER" c
    # INNER JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_SALES" ss 
    #     ON c.c_customer_sk = ss.ss_customer_sk
    # WHERE ss.ss_quantity < 50
    # ORDER BY prev_sale;
    # """),

    # ("complex_join_window_agg", """
    # SELECT c.c_customer_sk,
    #        ss.ss_store_sk,
    #        SUM(ss.ss_sales_price) OVER (PARTITION BY c.c_birth_country ORDER BY ss.ss_sold_date_sk) AS running_total,
    #        STRING_AGG(ss.ss_item_sk::STRING, ', ') AS item_list
    # FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER" c
    # INNER JOIN "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."STORE_SALES" ss 
    #     ON c.c_customer_sk = ss.ss_customer_sk
    # WHERE c.c_last_name LIKE '%son'
    # GROUP BY c.c_customer_sk, ss.ss_store_sk, ss.ss_sales_price, c.c_birth_country, ss.ss_sold_date_sk, ss.ss_item_sk
    # HAVING COUNT(*) > 3
    # ORDER BY running_total DESC;
    # """),
    # # # Add more queries as needed
]