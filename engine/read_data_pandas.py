from utils.import_configs import get_config
import pandas as pd
import polars as pl
import os


class DataReader:
    def __init__(self, folder_path=None, use_polars=None, delimiter=None):
        if folder_path is None:
            folder_path = get_config('input_folder')
            if not os.path.isabs(folder_path):
                folder_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), folder_path)
        if use_polars is None:
            use_polars = get_config('use_polars')
        if delimiter is None:
            delimiter = get_config('csv_delimiter')
        self.folder_path = folder_path
        self.use_polars = use_polars
        self.delimiter = delimiter


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


