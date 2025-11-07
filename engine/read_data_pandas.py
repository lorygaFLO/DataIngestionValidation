import pandas as pd
import polars as pl
import os
from config.settings import *


class DataReader:
    def __init__(self, folder_path=None, use_polars=None, delimiter=None):
        
        self.S = get_settings()
        self.folder_path = folder_path or self.S.PATH_INPUT
        self.delimiter = self.S.CSV_DELIMITER if delimiter is None else delimiter


    def read_csv_pandas(self, file_path):
        df = pd.read_csv(file_path, delimiter=self.delimiter)
        return df

    def read_parquet_pandas(self, file_path):
        df = pd.read_parquet(file_path)
        return df

    def read_csv_polars(self, file_path):
        df = pl.read_csv(file_path, separator=self.delimiter)
        return df

    def read_parquet_polars(self, file_path):
        df = pl.read_parquet(file_path)
        return df

    def read_all_files(self):
        file_paths = []
        for root, _, files in os.walk(self.folder_path):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                file_paths.append(file_path)
        return file_paths


