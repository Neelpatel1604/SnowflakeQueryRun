import pandas as pd
import os

# Define folder and file paths
folder_path = "query_results"
csv_file = os.path.join(folder_path, "query_metrics.csv")
excel_file = os.path.join(folder_path, "query_metrics.xlsx")

# Load CSV into a DataFrame
df = pd.read_csv(csv_file)

# Save DataFrame as an Excel file
df.to_excel(excel_file, index=False)
