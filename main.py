"""
Main entry point for the data processing pipeline.
Handles both transformation and validation of data files.
"""

from engine.execute_checks import Validator
from engine.data_transformer import DataTransformer
from config.settings import *


def main():
    S = get_settings()
    print("Process started with RUN_ID:", S.RUN_ID)


    # Initialize components with relative paths
    transformer = DataTransformer(
        registry_path='config/transform_registry.yaml',
        report_path='transform_reports',
        input_folder_path=S.INPUT,
        output_folder_path='transformed'
    )

    validator = Validator(
        registry_path='config/validation_registry.yaml',
        report_path='validation_reports',
        input_folder_path='transformed',  # Validate transformed files
        output_folder_path='delivery'
    )

    # First transform all files
    print("\nStarting transformation phase...")
    transformation_results = transformer.transform_files()
    print("Transformation phase completed")

    # Then validate all transformed files
    print("\nStarting validation phase...")
    validation_results = validator.validate_files()
    print("Validation phase completed")

    print("\nProcess ended")

if __name__ == "__main__":
    main()
