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
    df = load_data(bucket_name, folder_name)
    return df

def test_load_data(sample_dataset):
    assert not sample_dataset.empty, "Dataframe is empty"
