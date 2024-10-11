import pandas as pd
import numpy as np
import mlflow
import time
import boto3
import os
# Load data
def load_data():
    """
    Load dataset from the given URL.

    Args:
        url (str): URL to the CSV file.

    Returns:
        pd.DataFrame: Loaded dataset.
    """

    file_path = "https://tfl-cycle.s3.eu-west-3.amazonaws.com/silver/20150104.parquet?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEC0aCWV1LXdlc3QtMyJHMEUCIQCx9fPW4ESbHqO0gh0ZCKssR6V%2F9QVSL9pRiPhaBHCv3AIgVdldl9pVVCQ91K7uBkiy2JtLpN2n5zstmDnEdYWqowkq7QIIhv%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FARADGgw2MDEzNjQyNjc5NTgiDKJ1Gsboflw2HNv6jCrBAm%2F2vMexmsOyvRDYZgWJn1Kp570HSw1HgGP0P1%2B5o4ukeZaN23jS0CzgBlS5fbfrGf5knJaxEA5kGkXk1esCrQzLiDAtxCdH9GWnow8Ejeca4iuuH2M5au77aJBbZRv9NDaCZIJeBIus9fSaagcNFrdyjkvEyJG6eVtBt3frMggHR9Q8naNJ9MHN6MFgCzVBFb5%2Blpq0KWckHTiNtzqxyDAtESDEcc4l2grhyfaSrkqTH1qBe6ykRRvvXICgFkd9K5VAiybKxEj78LjFSweBaLquUik0argDnpbcPupWj%2BXMIYUeXcW4mkEw2ZkQy%2BBY9cC%2B4PQP%2FCM02aOs1HeVLPn7PFXVHWdzwLY8PUUpbfhzR0NJyGtrEIse%2F4RUPSMv6QQApClhY5VyXafcUDxYmuquxZh3mn3Quy762RIcTIwTezCY2KK4BjqzAvizTWbkDoyXzDfbovCNhxBis7oOwFAkCKPPL93B3fzGfMXeTI1DlLrGu4qRL10CwNOVhnB2ujmwmz2RdGYvYWRI4Q9DTDuw2pGM9%2BPY20vlRI9bY2BNnVEo0LgGA4feOzE1J7v3uE3YaIwcs2JEixdf00RjF2JPdKE21e5v%2FljkWBRo9MoBBRPI9U%2FK3sYT36f%2BCS7nMQdnmogc14PsYS%2BQD7db9LO0TYRhTel9bkdboVMsizqNdBhnUsMLCgwhW04uJ26mZPgUPnxM4BIT7oNaU5Yx9YQB8AHlHbUJjYUIU4IXpQGH5RjFp3CUboFsuw2J60pT94bW4%2BrDB0IyRQUPHEl1myNXRBrbFjX7k0U05LGbwE%2FC8oDouDJD7jKUwHiqEMr1T%2BLo5JfSjKvtWciNfFk%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20241011T101654Z&X-Amz-SignedHeaders=host&X-Amz-Expires=21600&X-Amz-Credential=ASIAYYBA2QO3GYOE4FPB%2F20241011%2Feu-west-3%2Fs3%2Faws4_request&X-Amz-Signature=744d8110e656dcd7ddc1b0f5cc331cdcb5e04fadad052e901a1c3653ed9fc054"
    df = pd.read_parquet(file_path, engine='pyarrow')

    return df

if __name__ == "__main__":
    experiment_name = "tfl-cycle-assertion"
    bucket_name = "tfl-cycle"
    folder_name = "silver"
    load_data()
