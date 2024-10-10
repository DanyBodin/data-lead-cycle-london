import pytest
from unittest import mock
from app.tfl_train import load_data

# Test data loading
def test_load_data():
    bucket_name = "tfl-cycle"
    folder_name = "silver"
    df = load_data(bucket_name, folder_name)
    assert not df.empty, "Dataframe is empty"
