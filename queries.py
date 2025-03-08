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
    # Add more queries as needed
]