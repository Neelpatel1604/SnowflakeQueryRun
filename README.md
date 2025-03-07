# Snowflake Query Runner

A Python script to execute Snowflake queries and track their performance metrics.

## Features

- Execute multiple SQL queries against Snowflake
- Track query performance metrics (runtime, data scanned, credits used)
- Store results in consistent CSV files
- Maintain query history and metrics

## Setup

1. Clone the repository:
```bash
git clone <your-repo-url>
cd Snowflake
```

2. Install required packages:
```bash
pip install -r requirements.txt
```

3. Create a `.env` file with your Snowflake credentials:
```
USER_NAME=your_username
USER_PASSWORD=your_password
USER_ACCOUNT=your_account
USER_WAREHOUSE=your_warehouse
USER_DATABASE=your_database
USER_SCHEMA=your_schema
```

## Usage

1. Add your queries to `queries.py`:
```python
QUERIES = {
    "query_name": "YOUR SQL QUERY",
    # Add more queries as needed
}
```

2. Run the script:
```bash
python try.py
```

Results will be saved in:
- `query_results/query_results.csv`: Contains all query results
- `query_results/query_metrics.csv`: Contains performance metrics for all queries

## Output Files

### query_results.csv
Contains the actual query results with additional metadata:
- Original query columns
- Query_Name: Name of the executed query
- Query_Timestamp: When the query was executed

### query_metrics.csv
Contains performance metrics for each query:
- Query_Name: Name of the executed query
- Query_ID: Snowflake query ID
- Start_Time: Query start timestamp
- End_Time: Query end timestamp
- Runtime_Seconds: Total runtime
- Elapsed_Time_Seconds: Snowflake elapsed time
- MB_Scanned: Amount of data scanned
- Rows_Produced: Number of rows returned
- Credits_Used: Snowflake credits consumed
- Query: The executed SQL query 