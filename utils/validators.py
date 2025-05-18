import pandas as pd
import polars as pl
import numpy as np
import re
from typing import List


def always_true_validator(df, messages, params=None):
    """
    Always returns True. Used for testing.

    Parameters:
    df (pandas.DataFrame or polars.DataFrame): The dataframe to check.
    messages (list): List to store validation messages.
    params (dict): Not used in this validator.

    Returns:
    bool: Always True.
    """
    return True


def always_false_validator(df, messages, params=None):
    """
    Always returns False. Used for testing.

    Parameters:
    df (pandas.DataFrame or polars.DataFrame): The dataframe to check.
    messages (list): List to store validation messages.
    params (dict): Not used in this validator.

    Returns:
    bool: Always False.
    """
    messages.append("This validator always returns False.")
    return False


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
    Check if there are additional columns in the dataframe that are not in the list.

    Parameters:
    df (pandas.DataFrame or polars.DataFrame): The dataframe to check.
    messages (list): List to store validation messages.
    columns (list): List of allowed columns.

    Returns:
    bool: True if there are no additional columns, False otherwise.
    """

    if isinstance(df, pd.DataFrame) or isinstance(df, pl.DataFrame):
        additional_cols = [col for col in df.columns if col not in columns]
    else:
        raise TypeError("df must be either a pandas or polars DataFrame")

    if additional_cols:
        messages.append(f"Additional columns found: {', '.join(additional_cols)}")
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


def value_range(df, messages, params):
    """
    Check if values in specified columns are within the given range.

    Parameters:
    df (pandas.DataFrame or polars.DataFrame): The dataframe to check.
    messages (list): List to store validation messages.
    params (dict): Dictionary with column names as keys and dictionaries with min and/or max values as values.

    Returns:
    bool: True if all values are within the specified range, False otherwise.
    """

    if not isinstance(df, (pd.DataFrame, pl.DataFrame)):
        raise TypeError("df must be either a pandas or polars DataFrame")

    is_valid = True

    for column, range_dict in params.items():
        if column not in df.columns:
            messages.append(f"Column {column} not found in dataframe")
            is_valid = False
            continue

        min_val = range_dict.get('min')
        max_val = range_dict.get('max')

        if isinstance(df, pd.DataFrame):
            if min_val is not None:
                below_min = df[df[column] < min_val]
                if not below_min.empty:
                    messages.append(f"Column {column} has {len(below_min)} values below the minimum of {min_val}")
                    is_valid = False

            if max_val is not None:
                above_max = df[df[column] > max_val]
                if not above_max.empty:
                    messages.append(f"Column {column} has {len(above_max)} values above the maximum of {max_val}")
                    is_valid = False
        else:  # polars DataFrame
            if min_val is not None:
                below_min = df.filter(df[column] < min_val)
                if len(below_min) > 0:
                    messages.append(f"Column {column} has {len(below_min)} values below the minimum of {min_val}")
                    is_valid = False

            if max_val is not None:
                above_max = df.filter(df[column] > max_val)
                if len(above_max) > 0:
                    messages.append(f"Column {column} has {len(above_max)} values above the maximum of {max_val}")
                    is_valid = False

    return is_valid


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




def check_hierarchy(df, messages, params):
    """
    Check if the hierarchy between higher_level_columns and lower_level_columns is respected.
    Verifies that for each unique combination of lower_level_columns, there is only one set of higher_level_columns.
    
    Parameters:
    df (pandas.DataFrame or polars.DataFrame): The dataframe to check.
    messages (list): List to store validation messages.
    params (dict): Dictionary containing:
        - higher_level_columns: List of column names that define the higher level in the hierarchy
        - lower_level_columns: List of column names that define the lower level in the hierarchy
        
    Returns:
    bool: True if the hierarchy is respected, False otherwise.
    """
    if not isinstance(df, (pd.DataFrame, pl.DataFrame)):
        raise TypeError("df must be either a pandas or polars DataFrame")
    
    # Extract column names from params
    higher_level_columns = params.get('higher_level_columns', [])
    lower_level_columns = params.get('lower_level_columns', [])
    
    # Check if required columns exist
    needed_columns = higher_level_columns + lower_level_columns
    missing_columns = [col for col in needed_columns if col not in df.columns]
    
    if missing_columns:
        for col in missing_columns:
            messages.append(f"Column {col} not found in dataframe")
        return False
    
    is_valid = True
    
    # Process based on DataFrame type
    if isinstance(df, pd.DataFrame):
        # Group by lower_level_columns and check if higher_level_columns are consistent
        grouped = df.groupby(lower_level_columns)
        for lower_values, group in grouped:
            higher_combinations = group[higher_level_columns].drop_duplicates()
            if len(higher_combinations) > 1:
                lower_values_str = " - ".join([f"{col}={val}" for col, val in zip(lower_level_columns, lower_values)])
                messages.append(f"Hierarchy violation: Found {len(higher_combinations)} different higher level combinations for lower level ({lower_values_str}).\n")
                is_valid = False


    else:  # polars DataFrame
        # Group by lower_level_columns and check if higher_level_columns are consistent
        grouped = df.group_by(lower_level_columns).agg(
            [pl.col(col).unique().alias(f"{col}_unique") for col in higher_level_columns]
        )
        for row in grouped.iter_rows():
            lower_values = row[:len(lower_level_columns)]
            higher_values = row[len(lower_level_columns):]
            if any(len(val) > 1 for val in higher_values):
                lower_values_str = " - ".join([f"{col}={val}" for col, val in zip(lower_level_columns, lower_values)])
                messages.append(f"Hierarchy violation: Found multiple higher level combinations for lower level ({lower_values_str}).\n")
                is_valid = False

    return is_valid





############################################################################################################
# Dictionary to map validator names to functions - All new vAlidators must be added here
VALIDATORS_DICT = {
    "always_true_validator": always_true_validator,
    "always_false_validator": always_false_validator,
    "required_columns": required_columns,
    "additional_columns": additional_columns,
    "is_empty_dataframe": is_empty_dataframe,
    "value_range": value_range,
    "check_null_values": check_null_values,
    "check_hierarchy": check_hierarchy,
}
