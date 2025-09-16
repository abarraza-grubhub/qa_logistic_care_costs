"""Data processing utilities for QA logistic care costs."""

import pandas as pd
from typing import Optional, Dict, Any


def load_data(file_path: str, **kwargs) -> pd.DataFrame:
    """
    Load data from various file formats.

    Args:
        file_path: Path to the data file
        **kwargs: Additional arguments passed to pandas read functions

    Returns:
        pandas.DataFrame: Loaded data
    """
    if file_path.endswith(".csv"):
        return pd.read_csv(file_path, **kwargs)
    elif file_path.endswith((".xlsx", ".xls")):
        return pd.read_excel(file_path, **kwargs)
    elif file_path.endswith(".parquet"):
        return pd.read_parquet(file_path, **kwargs)
    else:
        raise ValueError(f"Unsupported file format: {file_path}")


def clean_data(
    df: pd.DataFrame, config: Optional[Dict[str, Any]] = None
) -> pd.DataFrame:
    """
    Apply basic data cleaning operations.

    Args:
        df: Input DataFrame
        config: Configuration dictionary for cleaning operations

    Returns:
        pandas.DataFrame: Cleaned data
    """
    cleaned_df = df.copy()

    # Remove duplicates
    cleaned_df = cleaned_df.drop_duplicates()

    # Handle missing values based on config
    if config and "fill_na" in config:
        cleaned_df = cleaned_df.fillna(config["fill_na"])

    return cleaned_df


def validate_data(df: pd.DataFrame, schema: Dict[str, Any]) -> bool:
    """
    Validate data against a schema.

    Args:
        df: DataFrame to validate
        schema: Schema definition

    Returns:
        bool: True if data is valid
    """
    # Basic validation implementation
    required_columns = schema.get("required_columns", [])

    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Required column '{col}' not found in data")

    return True
