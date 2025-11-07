"""
Main entry point for the data processing pipeline.
Handles both transformation and validation of data files.
Every step must have an execute attribute
"""

from engine.data_validator import Validator
from engine.data_transformer import DataTransformer
from config.settings import *
import yaml


def load_pipeline_config(config_path):
    """Load pipeline configuration from a YAML file."""
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)


def step_factory(step_type, **kwargs):
    """Factory function to create step instances based on type."""
    if step_type == 'transform':
        return DataTransformer(
            name=kwargs['name'],
            registry_path=kwargs['registry_path'],
            report_path=kwargs['report_path'],
            input_folder_path=kwargs['input_folder_path'],
            output_folder_path=kwargs.get('output_folder_path') or kwargs['name']  # Default to step name
        )
    elif step_type == 'validate':
        return Validator(
            name=kwargs['name'],
            registry_path=kwargs['registry_path'],
            report_path=kwargs['report_path'],
            input_folder_path=kwargs['input_folder_path'],
            output_folder_path=kwargs.get('output_folder_path') or kwargs['name'] 
        )
    else:
        raise ValueError(f"Unsupported step type: {step_type}")


def run():
    S = get_settings()
    print("Process started with RUN_ID:", S.RUN_ID)

    # Load pipeline configuration
    pipeline_config = load_pipeline_config('config/pipeline_config.yaml')

    for step in pipeline_config['steps']:
        if step is None:  # Skip None steps
            print("Skipping a None step...")
            continue

        step_type = step['type']
        step_name = step['name']
        print(f"\nStarting {step_type} step: {step_name}...")

        try:
            step_instance = step_factory(step_type, **step)
            # Execute the step (assumes all step classes have a `run` method)
            step_instance.execute()
        except ValueError as e:
            print(f"Error: {e}")

        print(f"{step_type.capitalize()} step {step_name} completed")

    print("\nProcess ended")


if __name__ == "__main__":
    run()
