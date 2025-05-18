# Data Ingestion, Validation & Transformation

This repository provides a flexible and modular tool for data ingestion, validation, and transformation. The goal is to ensure that data complies with user-defined rules, can be transformed through configurable stages, and provides detailed reporting throughout the process.

This project is especially valuable for organizations that need to routinely validate and transform data from third-party sources. It automates the entire data processing pipeline, ensuring data quality and compliance with predefined standards.  
Additionally, it provides a structured template that simplifies the definition of data transformation and validation processes, making it easier to generate reports, trace operations, and standardize data processing pipelines.

## Project Overview

The tool has been updated to support greater modularity and configurability, allowing you to define multi-stage pipelines via YAML configuration files. You can now specify a sequence of validation and transformation stages, each with its own registry, directly in the configuration.

### Key Features

* **Configurable Multi-stage Pipeline**: Define a sequence of validation and transformation stages via YAML
* **Extensible Validation Rules**: Easily add new custom validators
* **Modular Transformations**: Implement custom transformations via registry and Python functions
* **Extended Format Support**: CSV and Parquet, with extensibility for more formats
* **Detailed Reporting**: Reports generated for each stage and processed file
* **Plug-in Architecture**: Easily add new validators/transformers
* **Non-destructive Processing**: Original files are never modified

## Project Architecture

### Directory Structure

```
/
├── config/                 # Configuration files
│   ├── configs.yaml        # General configuration
│   ├── constants.py        # System constants
│   ├── validation_registry.yaml      # Validation rules registry
│   └── transform_registry.yaml       # Transformation rules registry
├── engine/                 # Core processing modules
│   ├── execute_checks.py   # Validation execution
│   ├── execute_transforms.py # Transformation execution
│   ├── read_data_pandas.py # Data reading utilities
│   └── reporter.py         # Reporting functionality
└── utils/                  # Utility functions
    ├── import_configs.py   # Configuration management
    ├── validators.py       # Validation functions
    └── transformers.py     # Transformation functions
```

### Processing Flow

1. **Data Discovery**: The system scans the input directory for supported file formats
2. **Data Loading**: Files are loaded using the appropriate readers (Pandas for CSV/Parquet)
3. **Stage Processing**: Files go through the configured pipeline stages:
   * **Validation Stages**: Validation according to rules defined in the registry
   * **Transformation Stages**: Transformation according to rules defined in the registry
4. **Result Processing**:
   * Successfully processed files are copied to the output directory (structure replicated)
   * Detailed reports are generated for each stage and file

## Configuration Guide

### General Configuration (configs.yaml)

```yaml
use_polars: False
csv_delimiter: ","  
```

### Registry Configuration Examples

#### Validation Registry (validation_registry.yaml)
```yaml
"sales_*.csv":                
  validators:                  
    required_columns:          
      - "transaction_id"
      - "product_code"
      # ... other columns ...
    data_type:                
      transaction_id: "str"
      product_code: "str"
      # ... other validations ...
```

#### Transformation Registry (transform_registry.yaml)
```yaml
"sales_*.csv":
  transformers:
    column_rename:
      transaction_id: "id"
      product_code: "sku"
    date_format:
      transaction_date: "%Y-%m-%d"
    derived_columns:
      profit:
        formula: "total_amount - (quantity * unit_cost)"
```

## Implementation Guide

### Creating Custom Components

To add a custom validator or transformer, simply implement the function and register it in the respective dictionary (`VALIDATORS_DICT` or `TRANSFORMERS_DICT`).

#### Custom Validator
```python
def custom_validator(dataset, messages):
    """Custom validation function
    Args:
        dataset: pandas DataFrame to validate
        messages: list for validation messages
    """
    # custom logic
    return True

VALIDATORS_DICT['custom_validator'] = custom_validator
```

#### Custom Transformer
```python
def custom_transformer(dataset, config):
    """Custom transformation function
    Args:
        dataset: pandas DataFrame to transform
        config: transformation configuration
    """
    # custom logic
    return transformed_dataset

TRANSFORMERS_DICT['custom_transformer'] = custom_transformer
```

## Usage Examples

### Basic Usage
```python
from engine.execute_checks import Validator

# Initialize the validator
validator = Validator()

# Prepare input data
input_data = {
    'data/input/sample.csv': pd.read_csv('data/input/sample.csv')
}

# Run validation
results = validator.validate_files(input_data)
```


## Future Enhancements

* Support for additional file formats (JSON, XML, etc.)
* Integration with cloud storage services
* Database connectivity for validation/transformation against reference data
* Real-time monitoring and alerts
* REST API for remote processing requests
* Parallel processing of multiple files
* Conditional stage execution
* Dynamic transformation rules
* Enhanced error recovery and retry mechanisms

## Contributing

Contributions are welcome! Here are some areas where you can contribute:

* **New File Format Support**: Add support for additional data formats
* **Additional Validators/Transformers**: Implement new validation rules and transformation functions
* **Performance Optimization**: Improve processing speed for large datasets
* **Cloud Integration**: Add support for cloud storage services
* **Documentation**: Improve guides and examples
* **Error Handling**: Enhance error messages and reporting
* **Partial Processing**: Implement partial success/failure handling
* **Cross-file Operations**: Enable validation/transformation across multiple files
* **Pipeline Optimization**: Add support for parallel processing and conditional execution
