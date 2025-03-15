"""
Validator Requirements:
1. Each validator function must accept three parameters:
   - df: The dataframe to validate (pandas.DataFrame or polars.DataFrame).
   - messages: A list to store validation messages.
2. Each validator function must return a boolean indicating the validation result, or a tuple with the first value as a boolean.
3. If a validation fails and you want to stop further validations, raise a ValueError with an appropriate message.
4. Add the new validator function to the VALIDATORS_DICT at the end of the file.
"""

import pandas as pd
import polars as pl

def required_columns(df, columns, messages):
    """
    Check if required columns are present in the dataframe.

    Parameters:
    df (pandas.DataFrame or polars.DataFrame): The dataframe to check.
    columns (list): List of required columns.
    messages (list): List to store validation messages.

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





def additional_columns(df, columns, messages):
    """
    Check if there are no additional columns in the dataframe.

    Parameters:
    df (pandas.DataFrame or polars.DataFrame): The dataframe to check.
    columns (list): List of expected columns.
    messages (list): List to store validation messages.

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





VALIDATORS_DICT = {
    "required_columns": required_columns,
    "additional_columns": additional_columns,
    "is_empty_dataframe": is_empty_dataframe
}