import snowflake.connector
import time
import csv
import os
from dotenv import load_dotenv
from queries import queries

def check_env_variables():
    """Check if all required environment variables are set"""
    required_vars = [
        'USER_NAME',
        'USER_PASSWORD',
        'USER_ACCOUNT',
        'USER_WAREHOUSE',
        'USER_DATABASE',
        'USER_SCHEMA'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

# def list_schemas_and_tables(cursor):
#     """List available schemas and tables"""
#     # print("\nAvailable Schemas:")
#     # cursor.execute("SHOW SCHEMAS")
#     # schemas = cursor.fetchall()
#     # for schema in schemas:
#     #     print(f"- {schema[1]}")

#     # print(f"\nTables in {os.getenv('USER_SCHEMA')}:")
#     cursor.execute(f"SHOW TABLES IN SCHEMA {os.getenv('USER_DATABASE')}.{os.getenv('USER_SCHEMA')}")
#     # tables = cursor.fetchall()
#     # for table in tables:
#     #     print(f"- {table[1]}")

def run_query_and_save_metrics(query_name, query_sql, metrics_file, results_file):
    """
    Run a single query and save its results and performance metrics
    """
    conn = None
    cursor = None
    try:
        # Load environment variables
        load_dotenv()
        
        # Validate environment variables
        check_env_variables()
        
        # Connect to Snowflake
        print(f"\nExecuting query: {query_name}")
        print("Connecting to Snowflake...")
        
        # Print connection details (without password)
        # print(f"Account: {os.getenv('USER_ACCOUNT')}")
        # print(f"User: {os.getenv('USER_NAME')}")
        # print(f"Database: {os.getenv('USER_DATABASE')}")
        # print(f"Schema: {os.getenv('USER_SCHEMA')}")
        # print(f"Warehouse: {os.getenv('USER_WAREHOUSE')}")
        
        conn = snowflake.connector.connect(
            user=os.getenv('USER_NAME'),
            password=os.getenv('USER_PASSWORD'),
            account=os.getenv('USER_ACCOUNT'),
            warehouse=os.getenv('USER_WAREHOUSE'),
            database=os.getenv('USER_DATABASE'),
            schema=os.getenv('USER_SCHEMA')
        )

        cursor = conn.cursor()

        # List available schemas and tables
        # list_schemas_and_tables(cursor)

        # Record start time
        
        # # Disable query caching
        cursor.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE")

        # Set a unique query tag
        # cursor.execute(f"ALTER SESSION SET QUERY_TAG = 'RUN_{int(time.time())}'")
        # Execute query
        start_time = time.time()
        cursor.execute(query_sql)
        end_time = time.time()
        runtime_ms = (end_time - start_time) * 1000  # Time in milliseconds
        # 
        query_id = cursor.sfqid
        
        # Get column names
        # column_names = [desc[0] for desc in cursor.description]

        # Get query performance metrics
        cursor.execute(f"""
            SELECT 
                QUERY_ID,
                TOTAL_ELAPSED_TIME as ELAPSED_TIME_MS,  -- Keep in milliseconds
                BYTES_SCANNED/1024/1024 as MB_SCANNED,
                ROWS_PRODUCED,
                CREDITS_USED_CLOUD_SERVICES
            FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
            WHERE QUERY_ID = '{query_id}'
        """)
        metrics = cursor.fetchone()

        # Create output directory
        output_dir = 'query_results'
        os.makedirs(output_dir, exist_ok=True)



        # Prepare metrics data with times in milliseconds
        metrics_data = {
            'Query_Name': query_name,
            'Time_mesaured_by_us': runtime_ms,  # Store runtime in milliseconds
            'Snowflakes_Time': metrics[1] if metrics else None,  # Store elapsed time in milliseconds
            'MB_Scanned': metrics[2] if metrics else None,
            'Rows_Produced': metrics[3] if metrics else None,
            'Credits_Used': metrics[4] if metrics else None,
            'Query_ID': query_id,
            'Query': query_sql.replace('\n', ' ').strip()
        }

        # Append metrics to the common metrics file
        file_exists = os.path.isfile(metrics_file)
        with open(metrics_file, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=metrics_data.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(metrics_data)

        # Print summary with times in milliseconds
        print(f"\nQuery execution completed:")
        print(f"Results saved to: {results_file}")
        print(f"Metrics appended to: {metrics_file}")
        # print(f"\nPerformance Summary:")
        # print(f"Runtime: {runtime_ms:.2f} milliseconds")
        # print(f"Elapsed Time: {metrics[1]:.2f} milliseconds" if metrics else "Elapsed Time: N/A")
        # print(f"Rows produced: {metrics[3] if metrics else 'N/A'}")
        # print(f"Data scanned: {metrics[2]:.2f} MB" if metrics else "Data scanned: N/A")
        # print(f"Credits used: {metrics[4]}" if metrics else "Credits used: N/A")

    except ValueError as e:
        print(f"\nEnvironment Error for query {query_name}:")
        print(f"Error Message: {str(e)}")
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"\nSnowflake Error for query {query_name}:")
        print(f"Error Code: {e.errno}")
        print(f"Error Message: {str(e)}")
    except Exception as e:
        print(f"\nError executing query {query_name}:")
        print(f"Error Type: {type(e).__name__}")
        print(f"Error Message: {str(e)}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def main():
    # Create output directory
    output_dir = 'query_results'
    os.makedirs(output_dir, exist_ok=True)

    # Use fixed filename for metrics
    metrics_file = f'{output_dir}/query_metrics.csv'

    # Execute each query with a unique results file
    for query_name, query_sql in queries:
        results_file = f'{output_dir}/{query_name}_results.csv'
        run_query_and_save_metrics(query_name, query_sql, metrics_file, results_file)

    print(f"\nAll queries completed.")
    print(f"Metrics saved to: {metrics_file}")

if __name__ == "__main__":
    main()