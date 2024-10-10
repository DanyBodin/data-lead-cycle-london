import pytest
import pandas as pd
import os
import boto3
from app.train import load_data

# Test data loading
@pytest.fixture
def sample_dataset():
    bucket_name = "tfl-cycle"
    folder_name = "silver"
    s3_client = boto3.client("s3", region_name="eu-west-3")
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
    parquet_files = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('.parquet')]

    df_list = []
    for file in parquet_files:
        file_path = f"s3://{bucket_name}/{file}"
        df = pd.read_parquet(file_path, engine='pyarrow', storage_options={"key": os.environ["AWS_ACCESS_KEY_ID"], "secret": os.environ["AWS_SECRET_ACCESS_KEY"]})
        df_list.append(df)

    return df_list[0]

def test_load_data(sample_dataset):
    assert not sample_dataset.empty, "Dataframe is empty"
