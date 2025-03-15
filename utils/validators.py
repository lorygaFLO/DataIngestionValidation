

"""

Validator Requirements:
1. Each validator function must accept at least two parameters parameters in this order:
   - df: The dataframe to validate (pandas.DataFrame or polars.DataFrame).
   - messages: A list to store validation messages.
2. Each validator function must return a boolean indicating the validation result, or a tuple with the first value as a boolean.
3. Only is_empty_dataframe and required_columns validators will raise ValueError and stop validation process.
   These are critical checks as:
   - is_empty_dataframe ensures there is data to validate
   - required_columns ensures necessary columns exist for other validations
4. All other validators will continue processing even when they find issues, collecting messages for the final report.
5. Add the new validator function to the VALIDATORS_DICT at the end of the file. Then you will be able to call it from the registry.
6. Validators should be designed to be as generic as possible, capable of handling both pandas and polars DataFrames.
   When implementing a validator:
   - Always include proper type checking for both DataFrame types (if you need both, it depends from your need)


"""

import pandas as pd
import polars as pl

def required_columns(df, messages, columns):
    """
    Check if required columns are present in the dataframe.

    Parameters:
    df (pandas.DataFrame or polars.DataFrame): The dataframe to check.
    messages (list): List to store validation messages.
    columns (list): List of required columns.

    Returns:
    bool: True if all required columns are present, False otherwise.
    """

    if isinstance(df, pd.DataFrame) or isinstance(df, pl.DataFrame):
        missing_columns = [col for col in columns if col not in df.columns]
    else:
        raise TypeError("df must be either a pandas or polars DataFrame")

    if missing_columns:
        messages.append(f"Missing columns: {', '.join(missing_columns)}")
        output_msg = "Mandatory columns are missing, cannot proceed with validation."
        raise ValueError(output_msg)
    
    return True





def additional_columns(df, messages, columns):
    """
    Check if there are no additional columns in the dataframe.

    Parameters:
    df (pandas.DataFrame or polars.DataFrame): The dataframe to check.
    messages (list): List to store validation messages.
    columns (list): List of expected columns.

    Returns:
    bool: True if no additional columns are present, False otherwise.
    """

    if isinstance(df, pd.DataFrame) or isinstance(df, pl.DataFrame):
        additional_columns = [col for col in df.columns if col not in columns]
    else:
        raise TypeError("df must be either a pandas or polars DataFrame")

    if additional_columns:
        messages.append(f"Additional columns found: {', '.join(additional_columns)}")
        return False
    
    return True




def is_empty_dataframe(df, messages):
    """
    Check if the dataframe is empty.

    Parameters:
    df (pandas.DataFrame or polars.DataFrame): The dataframe to check.
    params (dict): Not used in this validator.
    messages (list): List to store validation messages.

    Returns:
    bool: True if the dataframe is not empty, False otherwise.
    """

    if isinstance(df, pd.DataFrame) or isinstance(df, pl.DataFrame):
        if len(df) == 0:
            messages.append("The dataframe is empty, cannot proceed with validation.")
            raise ValueError("The dataframe is empty, cannot proceed with validation.")
    else:
        raise TypeError("df must be either a pandas or polars DataFrame")
    
    return True





def check_null_values(df, messages, params):
    """
    Check for null values and custom-defined null values in specified columns and provide sample rows.

    Parameters:
    df (pandas.DataFrame or polars.DataFrame): The dataframe to check.
    messages (list): List to store validation messages.
    params (dict): Dictionary containing:
        - List of columns to check
        - custom_null_values (optional): Set of strings to be treated as null values

    Returns:
    bool: True if no null values are found, False otherwise.
    """
    columns = params
    custom_null_values = None
    sample_size = 5
    
    if isinstance(params, dict):
        columns = params.get('check_null_values', [])
        custom_null_values = set(params.get('custom_null_values', []))
    if not isinstance(df, (pd.DataFrame, pl.DataFrame)):
        raise TypeError("df must be either a pandas or polars DataFrame")

    is_valid = True
    for column in columns:
        if column not in df.columns:
            messages.append(f"Column {column} not found in dataframe. Impossible verify null values.")
            is_valid = False
            continue

        if isinstance(df, pd.DataFrame):
            # Check for standard null values
            null_mask = df[column].isna()
            
            # Check for custom null values if provided
            if custom_null_values:
                # Temporarily convert to string for custom null check
                temp_series = df[column].astype(str)
                custom_null_mask = temp_series.isin(custom_null_values)
                null_mask = null_mask | custom_null_mask
            
            null_count = null_mask.sum()
            if null_count > 0:
                sample_rows = df[null_mask].head(sample_size)
                row_indices = sample_rows.index.tolist()
        else:  # polars DataFrame
            # Check for standard null values
            null_mask = df[column].is_null()
            
            # Check for custom null values if provided
            if custom_null_values:
                # Temporarily convert to string for custom null check
                temp_series = df[column].cast(pl.Utf8)
                custom_null_mask = temp_series.is_in(list(custom_null_values))
                null_mask = null_mask | custom_null_mask
            
            null_count = null_mask.sum()
            if null_count > 0:
                sample_rows = df.filter(null_mask).limit(sample_size)
                row_indices = sample_rows.row_nr().to_list()

        if null_count > 0:
            messages.append(f"Column {column} has {null_count} null values. Sample row indices: {row_indices}")
            is_valid = False

    return is_valid


def value_range(df, messages, params):
    """
    Check if values in specified columns fall within defined ranges.

    Parameters:
    df (pandas.DataFrame or polars.DataFrame): The dataframe to check.
    messages (list): List to store validation messages.
    params (dict): Dictionary of column names and their min/max constraints.
                 Format: {column_name: {"min": value, "max": value}}
                 Both min and max are optional.

    Returns:
    bool: True if all values are within ranges, False otherwise.
    """
    if not isinstance(df, (pd.DataFrame, pl.DataFrame)):
        raise TypeError("df must be either a pandas or polars DataFrame")

    is_valid = True
    for column, constraints in params.items():
        if column not in df.columns:
            messages.append(f"Column {column} not found in dataframe")
            is_valid = False
            continue

        min_val = constraints.get("min")
        max_val = constraints.get("max")

        if min_val is not None:
            if isinstance(df, pd.DataFrame):
                invalid_min = df[df[column] <= min_val]
            else:  # polars DataFrame
                invalid_min = df.filter(pl.col(column) <= min_val)
            
            if len(invalid_min) > 0:
                messages.append(f"Column {column} has {len(invalid_min)} values below minimum {min_val}")
                is_valid = False

        if max_val is not None:
            if isinstance(df, pd.DataFrame):
                invalid_max = df[df[column] >= max_val]
            else:  # polars DataFrame
                invalid_max = df.filter(pl.col(column) >= max_val)
            
            if len(invalid_max) > 0:
                messages.append(f"Column {column} has {len(invalid_max)} values above maximum {max_val}")
                is_valid = False

    return is_valid


VALIDATORS_DICT = {
    "required_columns": required_columns,
    "additional_columns": additional_columns,
    "is_empty_dataframe": is_empty_dataframe,
    "value_range": value_range,
    "check_null_values": check_null_values
}