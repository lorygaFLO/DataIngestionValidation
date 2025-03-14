# DataIngestion

This repository provides a simple, practical, and flexible tool for data ingestion and validation. The goal is to ensure that data complies with user-defined rules, providing a detailed report in case of non-compliance.

Starting from an input data folder, the project identifies all CSV and Parquet files (the formats currently supported) present.

After reading the data, the validation phase begins. The checks to be performed are defined by the user in a registry file. Each check is associated with a validator, a customizable Python function. This allows the user to define any type of desired validation.

Upon completion of the validation, two outcomes are possible:

* The file is compliant and is saved to the output folder without modifications.
* The file is not compliant, and a detailed report of the non-compliance issues found is generated.


Currently, the project work on local machine, but in the future, who knows, nothing prevent us to read from a blob storage or db of a company. If you think the repo is useful feel to contribute.


# Adding and Running Validators

Follow these steps to add and run your custom data validators:

1.  **Configuration:**
    * Navigate to the `configs.yaml` and `constants.py` files.
    * Set the appropriate configuration values within these files.

2.  **Validator Implementation:**
    * Open the `validators.py` file.
    * Implement your custom validator as a Python function.
    * Add your validator function to the dictionary located at the end of the `validators.py` file. The key associated with your validator in this dictionary will be used to reference it in the registry.
    * Ensure your validator function adheres to the validator standards as described in this guide.

3.  **Registry Integration:**
    * Insert your validator name wth related parameters into the project's registry. This registry is the file that contains the instructions for the validation of the data.

4.  **Validation Execution:**
    * Run the data validation process.