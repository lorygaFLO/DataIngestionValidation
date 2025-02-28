import yaml
import importlib.util
import os
import sys


def get_main_dir():
    if getattr(sys, 'frozen', False):
        # The application is frozen (e.g., running as an executable)
        main_dir = os.path.dirname(sys.executable)
    else:
        # The application is not frozen (e.g., running as a script)
        main_dir = os.path.dirname(os.path.abspath(__file__))
    return main_dir


def load_config(config_filename):
    main_dir = get_main_dir()
    config_path = os.path.join(main_dir, config_filename)
    with open(config_path, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(f"Error loading YAML file: {exc}")


def import_project_config(config_module_filename):
    main_dir = get_main_dir()
    config_module_path = os.path.join(main_dir, config_module_filename)
    spec = importlib.util.spec_from_file_location("configs", config_module_path)
    config_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config_module)
    return config_module


# Load configurations at the start
yaml_config = load_config('config.yaml')
project_config = import_project_config('configs.py')