import yaml
import importlib.util
import os
import sys

def load_config(file_path='config/configs.yaml'):
    if not os.path.isabs(file_path):
        file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), file_path)
    
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
    
    return config

def get_config(key, file_path='config/configs.yaml'):
    config = load_config(file_path)
    if key in config:
        return config[key]
    else:
        raise KeyError(f"The key '{key}' does not exist in the configuration file. Please set the variable.")

def get_registry(file_path='config/registry.yaml'):
    if not os.path.isabs(file_path):
        file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), file_path)
    
    with open(file_path, 'r') as file:
        registry = yaml.safe_load(file)
    
    return registry

def get_output_path(file_path='config/configs.yaml'):
    """
    Retrieve the output path from the configuration file.

    Args:
        file_path (str): The path to the configuration file. Defaults to 'config/configs.yaml'.

    Returns:
        str: The absolute path to the output directory.
    """
    config = load_config(file_path)
    output_path = config.get('output_folder', 'output')
    if not os.path.isabs(output_path):
        output_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), output_path)
    return output_path
