"""
Transformer Class:
- Responsible for transforming datasets according to rules in the transform registry
- Uses DataHandler for file operations and pattern matching
- Applies transformations in specified order
- Generates transformation reports
- Saves transformed files in specified format (parquet or csv)
"""

import os
from typing import Dict, Any, Literal
from utils.transformers import TRANSFORMERS_DICT
from engine.data_handler import DataHandler
from engine.reporter import Reporter
from config.settings import *

class DataTransformer:
    def __init__(self, name: str, registry_path: str, report_path: str, input_folder_path: str, output_folder_path: str = None):
        """
        Initialize the DataTransformer.

        Args:
            registry_path (str): Path to the transform registry file
            report_path (str): Path to store transformation reports
            input_folder_path (str): Path to input folder
            output_folder_path (str): Path to output folder
        """
        self.S = get_settings()

        if name == None:
            raise ValueError("Step name must be provided. In this way you can identify the step in the logs")
        
        self.name = name
        self.output_folder_path = output_folder_path or name  # Default to step name

        self.handler = DataHandler(registry_path, input_folder_path, output_folder_path)
        self.output_folder_path = os.path.join(self.S.PATH_STAGING_RUN, output_folder_path)
        self.reporter = Reporter(report_path)

    def save_transformed_file(self, data: Any, original_path: str) -> str:
        """
        Save transformed data to a file in the specified format.

        Args:
            data: The transformed dataset to save
            original_path (str): Original file path (used to determine output filename)
            output_format (str): Format to save the file in ('csv' or 'parquet')

        Returns:
            str: Path where the file was saved

        Raises:
            ValueError: If output_format is not 'csv' or 'parquet'
        """

        # Create output filename with new extension
        original_filename = os.path.splitext(os.path.basename(original_path))[0]
        output_filename = f"{original_filename}.{self.S.OUTPUT_FORMAT}"
        output_path = os.path.join(self.output_folder_path, output_filename)

        # Create output directory if it doesn't exist
        os.makedirs(self.output_folder_path, exist_ok=True)

        # Save file in specified format
        try:
            if self.S.OUTPUT_FORMAT == 'csv':
                data.to_csv(output_path, index=False)
            else:  # parquet
                data.to_parquet(output_path, index=False)
            return output_path
        except Exception as e:
            raise RuntimeError(f"Error saving transformed file: {str(e)}")

    def execute(self, file_paths=None, output_format: Literal['csv', 'parquet'] = 'parquet') -> Dict[str, Dict[str, Any]]:
        """
        Transform files according to the rules in the registry.

        Args:
            file_paths (list, optional): List of file paths to transform. 
                                       If None, uses all files in input folder.
            output_format (str): Format to save transformed files ('csv' or 'parquet')

        Returns:
            Dict[str, Dict[str, Any]]: Dictionary of transformation results
        """
        to_process_files, error_files = self.handler.to_process_files(file_paths)
        transformation_results = {}

        # Report any files that couldn't be processed
        for file_path, error_messages in error_files.items():
            self.reporter.write_report(file_path, error_messages)

        # Transform each processed file
        for file_path, (data, pattern) in to_process_files.items():
            print(f"Transforming {file_path}")
            messages = []
            
            transforms = self.handler.registry[pattern]["transforms"]
            sorted_transforms = sorted(transforms, key=lambda x: x["order"])
            
            modified_data = data.copy()
            transform_log = []

            for transform in sorted_transforms:
                try:
                    transformer_func = TRANSFORMERS_DICT.get(transform["function"])
                    
                    if transformer_func:
                        modified_data = transformer_func(
                            modified_data, 
                            messages=messages,
                            **transform["params"]
                        )
                        status_msg = f"Transform '{transform['name']}' completed successfully"
                        transform_log.append({
                            "transform": transform["name"],
                            "status": "success"
                        })
                        messages.append(status_msg)
                    else:
                        error_msg = f"Transformer function '{transform['function']}' not found"
                        transform_log.append({
                            "transform": transform["name"],
                            "status": "failed",
                            "error": error_msg
                        })
                        messages.append(error_msg)
                
                except Exception as e:
                    error_msg = f"Error in transform '{transform['name']}': {str(e)}"
                    transform_log.append({
                        "transform": transform["name"],
                        "status": "failed",
                        "error": str(e)
                    })
                    messages.append(error_msg)

            # Save transformed file if all transformations were successful
            if not any(log["status"] == "failed" for log in transform_log):
                try:
                    output_path = self.save_transformed_file(modified_data, file_path)
                    messages.append(f"\nTransformed file saved to: {output_path}")
                except Exception as e:
                    error_msg = f"Error saving transformed file: {str(e)}"
                    transform_log.append({
                        "transform": "save_file",
                        "status": "failed",
                        "error": error_msg
                    })
                    messages.append(error_msg)

            transformation_results[file_path] = {
                "data": modified_data,
                "log": transform_log
            }

            # Write transformation report
            if messages:
                messages.insert(0, f"\n------ TRANSFORMATION RESULTS for {file_path} -------\n")
                self.reporter.write_report(file_path, messages)

        return transformation_results