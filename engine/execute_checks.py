"""
Validator Class:
- The Validator class is responsible for validating datasets against a set of predefined rules specified in the registry.
- It uses a registry file (default: 'config/registry.yaml') to determine which validators to apply to each dataset.
- The output path for compliant files can be specified in 'config/configs.yaml' or defaults to 'output'.
- Validation results and error messages are logged using the Reporter class.
- Compliant files are saved to the output directory, and validation reports are generated for non-compliant files.
"""

import fnmatch
import os
import shutil
from utils.import_configs import get_registry, get_output_path
from engine.reporter import Reporter
from utils.validators import VALIDATORS_DICT

class Validator:
    def __init__(self, registry_path=None):
        """
        Initialize the Validator class.

        Args:
            registry_path (str, optional): Path to the registry file. Defaults to 'config/registry.yaml'.
        """
        self.registry_path = registry_path if registry_path else os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config', 'registry.yaml')
        self.registry_data = self.load_registry()
        self.reporter = Reporter()
        self.output_path = get_output_path()

    def load_registry(self):
        """
        Load the registry data from the specified registry file.

        Returns:
            dict: The registry data.
        """
        return get_registry(self.registry_path)

    def match_files(self, input_data_dict):
        """
        Match input files to the patterns specified in the registry.

        Args:
            input_data_dict (dict): Dictionary of file paths and datasets.

        Returns:
            dict: Dictionary of matched files and datasets.
        """
        matched_files = {}
        for path, dataset in input_data_dict.items():
            matches = [pattern for pattern in self.registry_data.keys() if fnmatch.fnmatch(path, pattern)]
            if len(matches) > 1:
                message = f"Multiple matches found for {path}: {matches}. Not possible to univocally identify the validation routine."
                self.reporter.write_report(path, [message])
                print(message)
                continue  # Skip further validation for this file
            elif matches:
                matched_files[path] = dataset
        return matched_files

    def save_valid_files(self, path, dataset):
        """
        Save valid files to the output directory.

        Args:
            path (str): Path to the input file.
            dataset: The dataset to save.
        """
        output_file_path = os.path.join(self.output_path, os.path.relpath(path, start=os.path.dirname(self.output_path)))
        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
        shutil.copyfile(path, output_file_path)  # Overwrite if the file exists
        print(f"File compliant saved to {output_file_path}")

    def validate_files(self, input_data_dict):
        """
        Validate the input files against the registry rules.

        Args:
            input_data_dict (dict): Dictionary of file paths and datasets.

        Returns:
            dict: Dictionary of validation results.
        """
        matched_files = self.match_files(input_data_dict)
        validation_results = {}
        for path, dataset in matched_files.items():
            print(f"Validating {path}")
            messages = []
            matched_pattern = [pattern for pattern in self.registry_data.keys() if fnmatch.fnmatch(path, pattern)]
            if matched_pattern:
                validators = self.registry_data[matched_pattern[0]]['validators']
                for validator_name, params in validators.items():
                    validator_func = VALIDATORS_DICT.get(validator_name)
                    if validator_func:
                        try:
                            result = validator_func(dataset, params, messages)
                            if isinstance(result, tuple):
                                result = result[0]
                            validation_results[validator_name] = result
                        except ValueError as e:
                            messages.append(str(e))
                            self.reporter.write_report(path, messages)
                            break  # Stop further validation for this file
                    else:
                        raise ValueError(f"Validator {validator_name} not found")
                else:
                    messages.append("\n\n------ VALIDATION RESULTS -------\n")
                    for validator_name, result in validation_results.items():
                        messages.append(f"{validator_name}: {'Passed' if result else 'Failed'}")
                    if any(not result for result in validation_results.values()):
                        self.reporter.write_report(path, messages)
                    else:
                        self.save_valid_files(path, dataset)
            else:
                messages.append(f"No matching pattern found for {path}")
        return validation_results

# Usage example
if __name__ == "__main__":
    validator = Validator()
    input_data_dict = {
        r'c:\Users\Lorenzo\Documents\GitHub\DataIngestion\data\test1.csv': 'dataset1',
        r'c:\Users\Lorenzo\Documents\GitHub\DataIngestion\data\test2.csv': 'dataset2'
    }
    validation_results = validator.validate_files(input_data_dict)
    print(validation_results)
