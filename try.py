import snowflake.connector
import pandas as pd
import time
from datetime import datetime
import csv
import os
from dotenv import load_dotenv
from queries import QUERIES

def run_query_and_save_metrics(query_name, query_sql, metrics_file, results_file):
    """
    Run a single query and save its results and performance metrics
    """
    try:
        # Load environment variables
        load_dotenv()
        
        # Connect to Snowflake
        print(f"\nExecuting query: {query_name}")
        print("Connecting to Snowflake...")
        
        conn = snowflake.connector.connect(
            user=os.getenv('USER_NAME'),
            password=os.getenv('USER_PASSWORD'),
            account=os.getenv('USER_ACCOUNT'),
            warehouse=os.getenv('USER_WAREHOUSE'),
            database=os.getenv('USER_DATABASE'),
            schema=os.getenv('USER_SCHEMA')
        )

        cursor = conn.cursor()

        # Record start time
        start_time = time.time()
        start_timestamp = datetime.now()

        # Execute query
        cursor.execute(query_sql)
        results = cursor.fetchall()
        query_id = cursor.sfqid
        
        # Get column names
        column_names = [desc[0] for desc in cursor.description]

        # Record end time
        end_time = time.time()
        end_timestamp = datetime.now()
        runtime_seconds = end_time - start_time

        # Get query performance metrics
        cursor.execute(f"""
            SELECT 
                QUERY_ID,
                TOTAL_ELAPSED_TIME/1000 as ELAPSED_TIME_SECONDS,
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

        # Save query results
        results_df = pd.DataFrame(results, columns=column_names)
        
        # Add query metadata columns
        results_df['Query_Name'] = query_name
        results_df['Query_Timestamp'] = start_timestamp
        
        # Append or create results file
        if os.path.exists(results_file):
            results_df.to_csv(results_file, mode='a', header=False, index=False)
        else:
            results_df.to_csv(results_file, index=False)

        # Prepare metrics data
        metrics_data = {
            'Query_Name': query_name,
            'Query_ID': query_id,
            'Start_Time': start_timestamp,
            'End_Time': end_timestamp,
            'Runtime_Seconds': runtime_seconds,
            'Elapsed_Time_Seconds': metrics[1] if metrics else None,
            'MB_Scanned': metrics[2] if metrics else None,
            'Rows_Produced': metrics[3] if metrics else None,
            'Credits_Used': metrics[4] if metrics else None,
            'Query': query_sql.replace('\n', ' ').strip()
        }

        # Append metrics to the common metrics file
        file_exists = os.path.isfile(metrics_file)
        with open(metrics_file, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=metrics_data.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(metrics_data)

        # Print summary
        print(f"\nQuery execution completed:")
        print(f"Results appended to: {results_file}")
        print(f"Metrics appended to: {metrics_file}")
        print(f"\nPerformance Summary:")
        print(f"Runtime: {runtime_seconds:.2f} seconds")
        print(f"Rows produced: {metrics[3] if metrics else 'N/A'}")
        print(f"Data scanned: {metrics[2]:.2f} MB" if metrics else "Data scanned: N/A")
        print(f"Credits used: {metrics[4]}" if metrics else "Credits used: N/A")

    except Exception as e:
        print(f"\nError executing query {query_name}:")
        print(f"Error Type: {type(e).__name__}")
        print(f"Error Message: {str(e)}")

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

def main():
    # Create output directory
    output_dir = 'query_results'
    os.makedirs(output_dir, exist_ok=True)

    # Use fixed filenames for consistent storage
    metrics_file = f'{output_dir}/query_metrics.csv'
    results_file = f'{output_dir}/query_results.csv'

    # Execute each query from the queries file
    for query_name, query_sql in QUERIES.items():
        run_query_and_save_metrics(query_name, query_sql, metrics_file, results_file)

    print(f"\nAll queries completed.")
    print(f"Combined results saved to: {results_file}")
    print(f"Combined metrics saved to: {metrics_file}")

if __name__ == "__main__":
    main()