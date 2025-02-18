import pyodbc
import pandas as pd
from sqlalchemy import create_engine

def read_file():
    file_path = 'HttpTrigger2/scripts/out_of_stock_file.csv'
    file_path_op = 'HttpTrigger2/scripts/out_of_stock_file_filtered.csv'
    df = pd.read_csv(file_path)
    df["stock_status"].fillna("Out Of Stock", inplace=True)
    df.to_csv(file_path_op, index=False)
    print(df.iloc[2,7])

def insert_data():
    try:
        driver = "ODBC Driver 17 for SQL Server"
        server = "localhost"
        database = "newdb"
        username = "SA"
        password = "Iaaogbnos%401"

        connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver.replace(' ', '+')}&TrustServerCertificate=yes"

        engine = create_engine(connection_string)

        file_path = 'HttpTrigger2/scripts/out_of_stock_file_filtered.csv'
        df = pd.read_csv(file_path)

        df.to_sql('out_of_stock', engine, if_exists='append', index=False)
        print("Data inserted successfully!")
    except Exception as e:
        print("Error inserting data:", e)

