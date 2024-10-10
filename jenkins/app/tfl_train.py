import pandas as pd
import numpy as np
import mlflow
import time
import boto3
import os
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.pipeline import Pipeline

# Load data
def load_data(bucket_name, folder_name):
    """
    Load dataset from the given URL.

    Args:
        url (str): URL to the CSV file.

    Returns:
        pd.DataFrame: Loaded dataset.
    """
    s3_client = boto3.client("s3", region_name="eu-west-3")

    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)

    parquet_files = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('.parquet')]

    df_list = []
    for file in parquet_files:
        file_path = f"s3://{bucket_name}/{file}"
        df = pd.read_parquet(file_path, engine='pyarrow', storage_options={"key": os.environ["AWS_ACCESS_KEY_ID"], "secret": os.environ["AWS_SECRET_ACCESS_KEY"]})
        df_list.append(df)

    return df_list[0]

if __name__ == "__main__":
    experiment_name = "tfl-cycle-assertion"
    bucket_name = "tfl-cycle"
    folder_name = "silver"
    load_data(bucket_name, folder_name)
