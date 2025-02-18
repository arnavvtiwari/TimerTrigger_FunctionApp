import pyodbc
import logging
import pandas as pd


def insert_to_db(conn, table_name):
    try:
        # Read CSV file
        df = pd.read_csv('out_of_stock.csv')

        # Insert data into SQL Server
        for index, row in df.iterrows():
            columns = ', '.join(df.columns)
            placeholders = ', '.join(['?' for _ in df.columns])
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            conn.cursor().execute(sql, tuple(row))

        # Commit changes
        conn.commit()
        print("Data inserted successfully!")

    except Exception as e:
        print(f"Error inserting data: {str(e)}")
