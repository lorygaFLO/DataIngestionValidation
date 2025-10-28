import pandas as pd
import polars as pl
from typing import Union, List, LiteralString



def strings_strip_whitespace(
    df: Union[pd.DataFrame, pl.DataFrame], 
    columns: List[str], 
    strip_whitespace: bool = True, 
    messages: str = None ,
) -> Union[pd.DataFrame, pl.DataFrame]:
    """
    Clean string columns by applying various transformations.
    
    Parameters:
    df: Input DataFrame
    messages: string with messages to print
    columns: List of column names containing strings
    strip_whitespace: Whether to strip leading/trailing whitespace
    
    Returns:
    DataFrame with cleaned strings
    """
    if not isinstance(df, (pd.DataFrame, pl.DataFrame)):
        raise TypeError("df must be either a pandas or polars DataFrame")
    
    result_df = df.copy()
    
    if isinstance(df, pd.DataFrame):
        for col in columns:
            if col in df.columns:
                if strip_whitespace:
                    result_df[col] = df[col].str.strip()

    else:  # polars DataFrame
        for col in columns:
            if col in df.columns:
                expr = pl.col(col)
                if strip_whitespace:
                    expr = expr.str.strip_chars()

    messages.append(f"{strings_strip_whitespace.__name__}: string columns {columns} transformed.")
    
    return result_df



def case_transform(
    df: Union[pd.DataFrame, pl.DataFrame], 
    columns: List[str], 
    to_uppercase: bool = False,
    to_lowercase: bool = False,
    messages: str = None
) -> Union[pd.DataFrame, pl.DataFrame]:
    """
    Transform string columns to upper or lower case.
    
    Parameters:
    df: Input DataFrame (pandas or polars)
    columns: List of column names containing strings
    to_uppercase: Whether to convert strings to uppercase
    to_lowercase: Whether to convert strings to lowercase
    
    Returns:
    DataFrame with transformed strings
    """
    if not isinstance(df, (pd.DataFrame, pl.DataFrame)):
        raise TypeError("df must be either a pandas or polars DataFrame")
    
    if to_uppercase and to_lowercase:
        raise ValueError("Cannot set both to_uppercase and to_lowercase to True")
    
    if not (to_uppercase or to_lowercase):
        raise ValueError("At least one of to_uppercase or to_lowercase must be True")
    
    result_df = df.copy()
    
    if isinstance(df, pd.DataFrame):
        for col in columns:
            if col in df.columns:
                if to_uppercase:
                    result_df[col] = df[col].str.upper()
                if to_lowercase:
                    result_df[col] = df[col].str.lower()
    else:  # polars DataFrame
        for col in columns:
            if col in df.columns:
                expr = pl.col(col)
                if to_uppercase:
                    expr = expr.str.to_uppercase()
                if to_lowercase:
                    expr = expr.str.to_lowercase()
                result_df = result_df.with_columns(expr.alias(col))
    
    messages.append(f"{case_transform.__name__}: string columns {columns} transformed.")

    return result_df

def blank(df: Union[pd.DataFrame, pl.DataFrame], messages: str = None) -> Union[pd.DataFrame, pl.DataFrame]:
    """
    Transform string columns to upper or lower case.
    
    Parameters:
    df: Input DataFrame (pandas or polars)
    columns: List of column names containing strings
    
    Returns:
    DataFrame with transformed strings
    """
    if not isinstance(df, (pd.DataFrame, pl.DataFrame)):
        raise TypeError("df must be either a pandas or polars DataFrame")

    result_df = df.copy()
    
    messages.append(f"{blank.__name__} No operation has been correctly made.")

    return result_df


############################################################################################################
# Dictionary to map transformer names to functions - All new transformers must be added here
TRANSFORMERS_DICT = {
    "strings_strip_whitespace": strings_strip_whitespace,
    "case_transform": case_transform,
    "blank": blank,
} 