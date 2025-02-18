import logging
import pandas as pd
import io
from azure.storage.blob import ContainerClient
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
def db_connection():
    try:
        default_credential = DefaultAzureCredential()
        secret_client = SecretClient(
            vault_url='https://arnavskv.vault.azure.net/',
            credential=default_credential
        )
        blob_conn_string = secret_client.get_secret('arnavstorage').value
        container_client = ContainerClient.from_connection_string(
            conn_str=blob_conn_string,
            container_name='demo'
        )
        blob_name = "out_of_stock_file.csv"
        blob_client = container_client.get_blob_client(blob_name)
        csv_data = blob_client.download_blob().readall()
    except Exception as e:
        print(f'error: {e}')
    df = pd.read_csv(io.StringIO(csv_data.decode()))
    logging.info(df.head())
    return df

def remove_null(df):
    df["stock_status"].fillna("Out Of Stock", inplace=True)
    df["seller"].fillna("N/A", inplace = True)
    print(df.iloc[2, 7])
    return df

def push_to_storage(df):
    file_path = io.StringIO()
    df.to_csv(file_path, index = False) 
    try:
        default_credential = DefaultAzureCredential()
        secret_client = SecretClient(
            vault_url='https://arnavskv.vault.azure.net/',
            credential=default_credential
        )
        blob_conn_string = secret_client.get_secret('arnavstorage').value
        container_client = ContainerClient.from_connection_string(
            conn_str=blob_conn_string,
            container_name='demo'
        )
        blob_name = "out_of_stock_file_1.csv"
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(file_path.getvalue(), overwrite=True)
        print("File Pushed to BlobStorage")
    
    except Exception as e:
        print(f"Some error Occured {e}")