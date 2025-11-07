"""
Validator Class:
- Uses DataHandler for file operations and pattern matching
- Validates datasets against predefined rules specified in the registry
- Generates validation reports and saves compliant files
"""

import os
import shutil
from utils.validators import VALIDATORS_DICT
from engine.data_handler import DataHandler
from engine.reporter import Reporter
from typing import Dict, Any, Optional, List
from config.settings import *


class Validator:
    def __init__(self, name:str, registry_path: str, report_path: str, input_folder_path: str, output_folder_path: str = None):
        """
        Initialize the Validator class.

        Args:
            registry_path (str): Path to the registry file
            report_path (str): Path to store validation reports
            input_folder_path (str): Path to input folder
            output_folder_path (str): Path to output folder

        Raises:
            ValueError: If required paths are not provided
        """
        self.S = get_settings()
        
        if name == None:
            raise ValueError("Step name must be provided. In this way you can identify the step in the logs")
        
        self.name = name

        self.output_folder_path = output_folder_path or name  # Default to step name
            
        self.handler = DataHandler(registry_path, input_folder_path, output_folder_path)
        self.reporter = Reporter(report_path)

    def save_valid_files(self, path: str, dataset) -> None:
        """
        Save valid files to the output directory.

        Args:
            path (str): Path to the input file
            dataset: The dataset to save
        """
        base_name = os.path.basename(path)
        output_file_path = os.path.join(self.handler.output_folder_path, base_name)
        os.makedirs(self.handler.output_folder_path, exist_ok=True)
        shutil.copyfile(path, output_file_path)
        print(f"File compliant saved to {output_file_path}")

    def _execute_validator(self, validator_func, dataset: Any, messages: List[str], params: Optional[Dict] = None) -> bool:
        """
        Execute a validator function with appropriate parameters.

        Args:
            validator_func: The validator function to execute
            dataset: The dataset to validate
            messages: List to store validation messages
            params: Optional parameters for the validator

        Returns:
            bool: Validation result
        """
        try:
            if params is None:
                result = validator_func(dataset, messages)
            else:
                result = validator_func(dataset, messages, params)

            return result[0] if isinstance(result, tuple) else result
        except Exception as e:
            messages.append(str(e))
            return False

    def execute(self, file_paths=None) -> Dict[str, bool]:
        """
        Validate the input files against the registry rules.

        Args:
            file_paths (list, optional): List of file paths to validate.
                                       If None, processes all files in input folder.

        Returns:
            dict: Dictionary of validation results
        """
        to_process_files, error_files = self.handler.to_process_files(file_paths)
        validation_results = {}
        
        for file_path, (dataset, pattern) in to_process_files.items():
            print(f"Validating {file_path}")
            messages = []
            
            file_validation_results = {}
            validators = self.handler.registry[pattern]['validators']
            
            for validator_name, params in validators.items():
                validator_func = VALIDATORS_DICT.get(validator_name)
                if not validator_func:
                    raise ValueError(f"Validator {validator_name} not found")

                result = self._execute_validator(validator_func, dataset, messages, params)
                if result is False:  # Validation failed
                    self.reporter.write_report(file_path, messages)
                    break
                
                file_validation_results[validator_name] = result
            else:  # All validations passed for this file
                messages.append("\n\n------ VALIDATION RESULTS -------\n")
                for validator_name, result in file_validation_results.items():
                    messages.append(f"{validator_name}: {'Passed' if result else 'Failed'}")
                
                validation_results.update(file_validation_results)
                    
                # Save to output ONLY if all validations passed
                if all(result for result in file_validation_results.values()):
                    self.save_valid_files(file_path, dataset)
                else:
                    self.reporter.write_report(file_path, messages)
                
        return validation_results

# Usage example
if __name__ == "__main__":
    validator = Validator(
        r'c:\Users\Lorenzo\Documents\GitHub\DataIngestion\config\registry.yaml',
        r'c:\Users\Lorenzo\Documents\GitHub\DataIngestion\reports\validation_reports.txt',
        r'c:\Users\Lorenzo\Documents\GitHub\DataIngestion\data',
        r'c:\Users\Lorenzo\Documents\GitHub\DataIngestion\output'
    )
    input_data_dict = {
        r'c:\Users\Lorenzo\Documents\GitHub\DataIngestion\data\test1.csv': 'dataset1',
        r'c:\Users\Lorenzo\Documents\GitHub\DataIngestion\data\test2.csv': 'dataset2'
    }
    validation_results = validator.validate_files(list(input_data_dict.keys()))
    print(validation_results)
