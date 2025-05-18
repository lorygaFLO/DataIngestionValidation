"""
DataHandler Class:
Base class for handling data files. Provides common functionality for:
- File discovery and reading
- Pattern matching against registry
- Basic file operations
"""

import os
import fnmatch
import pandas as pd
from typing import Dict, List, Tuple, Optional
from utils.import_configs import get_registry, get_config

class DataHandler:
    def __init__(self, registry_path: str, input_folder_path: str, output_folder_path: str):
        """
        Initialize the DataHandler.

        Args:
            registry_path (str): Path to the registry file
            input_folder_path (str): Path to input folder
            output_folder_path (str): Path to output folder

        Raises:
            ValueError: If required paths are not provided
        """
        if registry_path is None:
            raise ValueError("registry_path must be provided")
        
        if input_folder_path is None:
            raise ValueError("input_folder_path must be provided")
        
        if output_folder_path is None:
            raise ValueError("output_folder_path must be provided")
            
        self.registry_path = registry_path
        self.registry = get_registry(registry_path)
        self.input_folder_path = input_folder_path
        self.output_folder_path = output_folder_path
        self.delimiter = get_config('csv_delimiter')

    def match_file(self, file_path: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Match a file path to the patterns specified in the registry.

        Args:
            file_path (str): Path to the file to match.

        Returns:
            Tuple[Optional[str], Optional[str]]: Tuple of (matched pattern, error message if any)
                                               If no match found, returns (None, error_message)
        """
        matches = [pattern for pattern in self.registry.keys() 
                  if fnmatch.fnmatch(os.path.basename(file_path), pattern)]
        
        if len(matches) > 1:
            return None, f"Multiple matches found for {file_path}: {matches}. Cannot uniquely identify processing routine."
        elif matches:
            return matches[0], None
        else:
            return None, f"No matching pattern found in registry for: {file_path}. Impossible to identify a work routine."

    def read_file(self, file_path: str) -> Tuple[Optional[pd.DataFrame], List[str]]:
        """
        Read a file based on its extension.

        Args:
            file_path (str): Path to the file to read

        Returns:
            Tuple[Optional[pd.DataFrame], List[str]]: Tuple of (DataFrame or None, error messages)
        """
        messages = []
        try:
            if file_path.lower().endswith('.csv'):
                data = pd.read_csv(file_path, delimiter=self.delimiter)
                return data, messages
            elif file_path.lower().endswith('.parquet'):
                data = pd.read_parquet(file_path)
                return data, messages
            else:
                return None, [f"Unsupported file format for {file_path}. Only CSV and Parquet files are supported."]
        except Exception as e:
            return None, [f"Error reading file {file_path}: {str(e)}"]

    def get_input_files(self) -> List[str]:
        """
        Get all files from the input folder.

        Returns:
            List[str]: List of file paths
        """
        file_paths = []
        for root, _, files in os.walk(self.input_folder_path):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                file_paths.append(file_path)
        if not file_paths:
            raise FileNotFoundError(f"The input folder '{self.input_folder_path}' is empty.")
        return file_paths

    def save_to_output(self, file_path: str, data: pd.DataFrame):
        """
        Save the processed DataFrame to the output folder, preserving the original filename.

        Args:
            file_path (str): Original input file path
            data (pd.DataFrame): DataFrame to save
        """

        os.makedirs(self.output_folder_path, exist_ok=True)

        base_name = os.path.basename(file_path)
        output_path = os.path.join(self.output_folder_path, base_name)
        if base_name.lower().endswith('.csv'):
            data.to_csv(output_path, index=False, sep=self.delimiter)
        elif base_name.lower().endswith('.parquet'):
            data.to_parquet(output_path, index=False)
        else:
            raise ValueError(f"Unsupported file extension for output: {base_name}. Only CSV and Parquet files are supported.")

    def to_process_files(self, file_paths=None) -> Tuple[Dict[str, Tuple[pd.DataFrame, str]], Dict[str, List[str]]]:
        """
        Process files from input folder or specified list.
        Handles file reading and pattern matching.

        Args:
            file_paths (List[str], optional): Specific files to process. 
                                            If None, processes all files in input folder.

        Returns:
            Tuple[Dict[str, Tuple[pd.DataFrame, str]], Dict[str, List[str]]]: 
                First dict: {file_path: (dataframe, matched_pattern)} for successfully processed files
                Second dict: {file_path: [error_messages]} for files with errors
        """
        if file_paths is None:
            file_paths = self.get_input_files()

        to_process_files = {}
        error_files = {}
        
        for file_path in file_paths:
            # Try to read the file
            data, read_messages = self.read_file(file_path)
            if data is None:
                error_files[file_path] = read_messages
                continue

            # Match the file to a pattern
            matched_pattern, match_error = self.match_file(file_path)
            if matched_pattern:
                # DO NOT save to output here!
                # self.save_to_output(file_path, data)
                to_process_files[file_path] = (data, matched_pattern)
            else:
                error_files[file_path] = [match_error] if match_error else []

        return to_process_files, error_files