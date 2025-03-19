import pandas as pd
import sqlite3
import pydoc_data.topics

# Step 1: Extract - Read data from a CSV file
def extract(file_path):
    df = pd.read_csv(file_path)
    return df

def ne_method():
    print("HI")

    
# Step 2: Transform - Clean and process the data
def transform(df):
    # Convert column names to lowercase
    df.columns = df.columns.str.lower()

    # Drop rows with missing values
    df.dropna(inplace=True)

    # Convert date column to datetime format
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])

    return df

# Step 3: Load - Store the data into SQLite database
def load(df, db_name, table_name):
    conn = sqlite3.connect(db_name)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()

# ETL Pipeline Execution
if __name__ == "__main__":
    file_path = "data.csv"  # Replace with your actual CSV file path
    db_name = "etl_database.db"
    table_name = "processed_data"

    print("Starting ETL process...")

    # Extract
    data = extract(file_path)
    print("Data extracted successfully.")

    # Transform
    cleaned_data = transform(data)
    print("Data transformed successfully.")

    # Load
    load(cleaned_data, db_name, table_name)
    print(f"Data loaded successfully into {table_name} table in {db_name}.")
