import pandas as pd
import numpy as np
import mlflow
import time
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.pipeline import Pipeline

# Load data
def load_data(url):
    """
    Load dataset from the given URL.

    Args:
        url (str): URL to the CSV file.

    Returns:
        pd.DataFrame: Loaded dataset.
    """
    return pd.read_parquet(url, engine='pyarrow')
