import pandas as pd
import numpy as np
import mlflow
import time
import boto3
import os
# Load data
def load_data(bucket_name):
    """
    Load dataset from the given URL.

    Args:
        url (str): URL to the CSV file.

    Returns:
        pd.DataFrame: Loaded dataset.
    """

    session = boto3.Session(
    aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name="eu-west-3"
)

    ressources = session.resource('s3')
    bucket = ressources.Bucket(bucket_name)

    parquet_files = [obj.key for obj in bucket.objects.all()]

    df_list = []
    for file in parquet_files:
        file_path = f"s3://{bucket_name}/{file}"
        df = pd.read_parquet(file_path, engine='pyarrow')
        df_list.append(df)

    return df_list[0]

if __name__ == "__main__":
    experiment_name = "tfl-cycle-assertion"
    bucket_name = "tfl-cycle"
    folder_name = "silver"
    load_data(bucket_name)
