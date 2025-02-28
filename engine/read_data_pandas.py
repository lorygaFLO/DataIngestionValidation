from utils.import_configs import yaml_config, project_config
import pandas as pd
import polars as pl
import os


class DataReader:
    def __init__(self, folder_path=None, use_polars=None, delimiter=None):
        if folder_path is None:
            folder_path = project_config.some_config_attribute
        if use_polars is None:
            use_polars = project_config.use_polars
        if delimiter is None:
            delimiter = project_config.csv_delimiter
        self.folder_path = folder_path
        self.use_polars = use_polars
        self.delimiter = delimiter

    def read_csv_pandas(self, file_name):
        file_path = os.path.join(self.folder_path, file_name)
        df = pd.read_csv(file_path, delimiter=self.delimiter)
        return df

    def read_parquet_pandas(self, file_name):
        file_path = os.path.join(self.folder_path, file_name)
        df = pd.read_parquet(file_path)
        return df

    def read_csv_polars(self, file_name):
        file_path = os.path.join(self.folder_path, file_name)
        df = pl.read_csv(file_path, separator=self.delimiter)
        return df

    def read_parquet_polars(self, file_name):
        file_path = os.path.join(self.folder_path, file_name)
        df = pl.read_parquet(file_path)
        return df

    def read_all_files(self):
        dataframes = {}
        for file_name in os.listdir(self.folder_path):
            file_path = os.path.join(self.folder_path, file_name)
            if file_name.endswith('.csv'):
                if self.use_polars:
                    dataframes[file_path] = self.read_csv_polars(file_name)
                else:
                    dataframes[file_path] = self.read_csv_pandas(file_name)
            elif file_name.endswith('.parquet'):
                if self.use_polars:
                    dataframes[file_path] = self.read_parquet_polars(file_name)
                else:
                    dataframes[file_path] = self.read_parquet_pandas(file_name)
        return dataframes

# Example usage
data_reader = DataReader()

# Read all files
all_data_frames = data_reader.read_all_files()
print(all_data_frames)
