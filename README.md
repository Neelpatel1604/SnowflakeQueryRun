# Snowflake Query Runner

A Python script to execute Snowflake queries and track their performance metrics.

## Features

- Execute multiple SQL queries against Snowflake
- Track query performance metrics (runtime, data scanned, credits used)
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
python main.py
```

Results will be saved in:
- `query_results/query_metrics.csv`: Contains performance metrics for all queries

## Output File
### query_metrics

This file contains performance metrics for each executed query.

## Columns:
- **Query_Name**: Name of the executed query.
- **Time_mesaured_by_us**: Total runtime in milliseconds.
- **Snowflskes_Time**: Snowflake elapsed time in milliseconds.
- **MB_Scanned**: Amount of data scanned.
- **Rows_Produced**: Number of rows returned.
- **Credits_Used**: Snowflake credits consumed.
- **Query_ID**: Unique identifier for the query.
- **Query**: The executed SQL query (formatted in a single line).