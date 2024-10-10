import pytest
from unittest import mock
from app.tfl-train import load_data

# Test data loading
def test_load_data():
    url = "https://julie-2-next-resources.s3.eu-west-3.amazonaws.com/full-stack-full-time/linear-regression-ft/californian-housing-market-ft/california_housing_market.csv"
    df = load_data(url)
    assert not df.empty, "Dataframe is empty"
