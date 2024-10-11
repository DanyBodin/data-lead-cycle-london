import pytest
import pandas as pd
import os
import boto3
from app.train import load_data

# Test data loading
@pytest.fixture
def sample_dataset():
    df = load_data()
    return df

def test_load_data(sample_dataset):
    assert not sample_dataset.empty, "Dataframe is empty"
