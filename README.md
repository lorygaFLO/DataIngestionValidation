# Data Ingestion & Validation

This repository provides a simple, practical, and flexible tool for data ingestion and validation. The goal is to ensure that data complies with user-defined rules, providing a detailed report in case of non-compliance.

This project is particularly valuable for individuals organizations that need to regularly validate data consistency from third-party providers. It automates the validation process, ensuring data quality and compliance with predefined standards.


## Project Overview

The Data Ingestion & Validation tool is designed to automate and streamline the process of validating data files against predefined rules. It supports CSV and Parquet files, with the flexibility to extend to other formats in the future.

### Key Features

* **Flexible Validation Rules**: Define custom validation rules through a simple registry configuration
* **Multiple File Format Support**: Currently handles CSV and Parquet files
* **Detailed Reporting**: Generates comprehensive validation reports for non-compliant files
* **Extensible Architecture**: Easy to add new validators and file format support
* **Non-Destructive Processing**: Original files remain unchanged during validation

## Project Architecture

### Directory Structure

```
/
├── config/                 # Configuration files
│   ├── configs.yaml       # General configuration
│   ├── constants.py       # System constants
│   └── registry.yaml      # Validation rules registry
├── engine/                # Core processing modules
│   ├── execute_checks.py  # Validation execution
│   ├── read_data_pandas.py# Data reading utilities
│   └── reporter.py        # Reporting functionality
└── utils/                 # Utility functions
    ├── import_configs.py  # Configuration management
    └── validators.py      # Validation functions
```

### Validation Process Flow

1. **Data Discovery**: The system scans the input directory for supported file formats
2. **Data Loading**: Files are loaded using appropriate readers (Pandas for CSV/Parquet)
3. **Rule Matching**: Files are matched against patterns in the registry
4. **Validation**: Each file undergoes validation against defined rules
5. **Result Processing**:
   * Compliant files are copied to the output directory
   * Non-compliant files generate detailed validation reports

## Configuration Guide

### General Configuration (configs.yaml)

```yaml
input_path: "data/input"      # Directory containing files to validate
output_path: "data/output"    # Directory for validated files (maintains input directory structure)
report_path: "data/reports"   # Directory for validation reports (maintains input directory structure)

# Note: The directory structure from input_path will be replicated in both output_path and report_path.
# Files in output_path are exact copies of validated files, remaining unmodified from their original state.
```
Note: The directory structure from input_path will be replicated in both output_path and report_path.
Files in output_path are exact copies of validated files, remaining unmodified from their original state.

### Registry Configuration (registry.yaml)

```yaml
"sales_*.csv":                # Pattern matching sales data files
  validators:                  # List of validators to apply
    column_presence:           # Validator for required columns
      required_columns:        # List of mandatory columns
        - "transaction_id"
        - "product_code"
        - "quantity"
        - "unit_price"
        - "total_amount"
        - "transaction_date"
        - "customer_id"
    data_type:                 # Validator for data types
      column_types:
        transaction_id: "str"
        product_code: "str"
        quantity: "int"
        unit_price: "float"
        total_amount: "float"
        transaction_date: "datetime"
        customer_id: "str"
    value_range:               # Validator for numerical constraints
      rules:
        quantity:
          min: 1
          max: 1000
        unit_price:
          min: 0.01
        total_amount:
          min: 0.01
    date_format:               # Validator for date formatting
      transaction_date: "%Y-%m-%d"
```

## Validator Implementation Guide

### Validator Return Types

All validators must take as input df and messages as a rule and return one of the following:
* **Boolean**: `True` if validation passes, `False` otherwise
* **Tuple**: A tuple where:
  - First element is a boolean (`True` if validation passes)
  - Subsequent elements can contain additional validation information

Furthrermore all validators must be inserted in VALIDATORS_DICT dictionary.

### Creating Custom Validators

1. **Define Validator Function**:
```python
def custom_validator(dataset, messages):
    """Custom validation function
    Args:
        dataset: pandas DataFrame to validate (mandatory)
        messages: list to store validation messages (mandatory)
    """
    # Implement validation logic
    return True
```

2. **Register Validator**:
```python
VALIDATORS_DICT = {
    'another_validator': another_validator,
    'custom_validator': custom_validator
}
```

### Standard Validator Parameters

* `dataset`: Pandas DataFrame containing the data to validate
* `messages`: List for storing validation messages

## Usage Examples

### Basic Usage
The validation must be run from main.py file. 

```python
from engine.execute_checks import Validator

# Initialize validator
validator = Validator()

# Prepare input data
input_data = {
    'data/input/sample.csv': pd.read_csv('data/input/sample.csv')
}

# Run validation
results = validator.validate_files(input_data)
```

### Adding Custom Validation Rules

1. Create validator function in `validators.py`
2. Add to registry.yaml:
```yaml
"data/*.csv":
  validators:
    custom_validator:
      param1: value1
      param2: value2
```

## Future Enhancements

* Support for additional file formats (JSON, XML, etc.)
* Integration with cloud storage services (Azure Blob, AWS S3)
* Database connectivity for validation against reference data
* Real-time validation monitoring and alerts
* REST API for remote validation requests

## Contributing

Contributions are welcome and greatly appreciated! This project has many opportunities for enhancement to make it more practical and customizable. Here are some areas where you can contribute:

### Areas for Contribution

* **New File Format Support**: Add support for JSON, XML, Excel, or other data formats
* **Additional Validators**: Implement new validation rules and checks
* **Performance Optimization**: Improve processing speed for large datasets
* **Cloud Integration**: Add support for cloud storage services (AWS S3, Azure Blob, etc.)
* **Documentation**: Improve guides, add examples, and create tutorials
* **Error Handling**: Enhance error messages and validation reporting
* **Partial validation**: why reject all the records incoming from data? we can reject bad records but accept good ones
* **Cross Validation**: permit validtion between multiple files.
