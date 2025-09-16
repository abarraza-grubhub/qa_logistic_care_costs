"""Analysis utilities for QA logistic care costs."""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Optional, List, Dict, Any


def descriptive_stats(
    df: pd.DataFrame, columns: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Generate descriptive statistics for specified columns.

    Args:
        df: Input DataFrame
        columns: List of columns to analyze. If None, analyzes all numeric columns

    Returns:
        pandas.DataFrame: Descriptive statistics
    """
    if columns is None:
        columns = df.select_dtypes(include=[np.number]).columns.tolist()

    return df[columns].describe()


def plot_distribution(df: pd.DataFrame, column: str, bins: int = 30) -> None:
    """
    Plot distribution of a column.

    Args:
        df: Input DataFrame
        column: Column name to plot
        bins: Number of bins for histogram
    """
    plt.figure(figsize=(10, 6))

    plt.subplot(1, 2, 1)
    plt.hist(df[column].dropna(), bins=bins, alpha=0.7)
    plt.title(f"Distribution of {column}")
    plt.xlabel(column)
    plt.ylabel("Frequency")

    plt.subplot(1, 2, 2)
    sns.boxplot(y=df[column])
    plt.title(f"Box Plot of {column}")

    plt.tight_layout()
    plt.show()


def correlation_analysis(df: pd.DataFrame, method: str = "pearson") -> pd.DataFrame:
    """
    Perform correlation analysis on numeric columns.

    Args:
        df: Input DataFrame
        method: Correlation method ('pearson', 'spearman', 'kendall')

    Returns:
        pandas.DataFrame: Correlation matrix
    """
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    return df[numeric_cols].corr(method=method)


def identify_outliers(df: pd.DataFrame, column: str, method: str = "iqr") -> pd.Series:
    """
    Identify outliers in a column.

    Args:
        df: Input DataFrame
        column: Column name to analyze
        method: Method for outlier detection ('iqr', 'zscore')

    Returns:
        pandas.Series: Boolean series indicating outliers
    """
    if method == "iqr":
        Q1 = df[column].quantile(0.25)
        Q3 = df[column].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        return (df[column] < lower_bound) | (df[column] > upper_bound)

    elif method == "zscore":
        z_scores = np.abs((df[column] - df[column].mean()) / df[column].std())
        return z_scores > 3

    else:
        raise ValueError(f"Unknown method: {method}")


def cost_trend_analysis(
    df: pd.DataFrame, date_col: str, cost_col: str
) -> Dict[str, Any]:
    """
    Analyze cost trends over time.

    Args:
        df: Input DataFrame
        date_col: Date column name
        cost_col: Cost column name

    Returns:
        dict: Trend analysis results
    """
    # Ensure date column is datetime
    df_copy = df.copy()
    df_copy[date_col] = pd.to_datetime(df_copy[date_col])

    # Sort by date
    df_copy = df_copy.sort_values(date_col)

    # Calculate basic metrics
    total_cost = df_copy[cost_col].sum()
    avg_cost = df_copy[cost_col].mean()
    cost_std = df_copy[cost_col].std()

    # Monthly aggregation
    df_copy["month"] = df_copy[date_col].dt.to_period("M")
    monthly_costs = df_copy.groupby("month")[cost_col].sum()

    return {
        "total_cost": total_cost,
        "average_cost": avg_cost,
        "cost_std": cost_std,
        "monthly_costs": monthly_costs,
        "trend_direction": (
            "increasing"
            if monthly_costs.iloc[-1] > monthly_costs.iloc[0]
            else "decreasing"
        ),
    }
