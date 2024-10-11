import pandas as pd
import numpy as np
import mlflow
import time
import boto3
import os
# Load data
def load_data(bucket_name, folder_name):
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

    s3_client = session.client('s3')

    return s3_client

if __name__ == "__main__":
    experiment_name = "tfl-cycle-assertion"
    bucket_name = "tfl-cycle"
    folder_name = "silver"
    load_data(bucket_name, folder_name)
