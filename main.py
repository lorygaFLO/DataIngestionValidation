"""
Main entry point for the data processing pipeline.
Handles both transformation and validation of data files.
"""

from engine.execute_checks import Validator
from engine.data_transformer import DataTransformer
from utils.import_configs import get_config

def main():
    print("Process started...")



    # Initialize components with relative paths
    transformer = DataTransformer(
        registry_path='config/transform_registry.yaml',
        report_path='reports/transform_reports',
        input_folder_path='input',
        output_folder_path='output/transformed'
    )

    validator = Validator(
        registry_path='config/validation_registry.yaml',
        report_path='reports/validation_reports',
        input_folder_path='output/transformed',  # Validate transformed files
        output_folder_path='output/validated'
    )

    # First transform all files
    print("\nStarting transformation phase...")
    transformation_results = transformer.transform_files(output_format='parquet')
    print("Transformation phase completed")

    # Then validate all transformed files
    print("\nStarting validation phase...")
    validation_results = validator.validate_files()
    print("Validation phase completed")

    print("\nProcess ended")

if __name__ == "__main__":
    main()
