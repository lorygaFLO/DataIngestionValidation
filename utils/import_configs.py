import yaml
import os
from config.settings import *
S = get_settings()

#to move in settings and remove file
def get_registry(file_path='config/registry.yaml'):
    if not os.path.isabs(file_path):
        file_path = os.path.join(S.BASEPATH, file_path)
    
    with open(file_path, 'r') as file:
        registry = yaml.safe_load(file)
    
    return registry


