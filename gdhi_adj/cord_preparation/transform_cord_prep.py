"""Module for imputing values ready for CORD in the gdhi_adj project."""

import glob
import os
from pathlib import Path
from typing import List

import pandas as pd

from gdhi_adj.utils.helpers import read_with_schema
from gdhi_adj.utils.logger import GDHI_adj_logger

GDHI_adj_LOGGER = GDHI_adj_logger(__name__)
logger = GDHI_adj_LOGGER.logger


def append_all_sub_components(config: dict) -> pd.DataFrame:
    """
    Append all DataFrames that contain separatesub-components together so that
    each LSOA has all sub-components present in one DataFrame.

    Args:
      config (dict): Pipeline configuration dictionary containing filepaths
        for the location of sub-component data.

    Returns:
      pd.DataFrame: DataFrame with each LSOA having many sub-components
        component appended.
    """
    root_dir = config["user_settings"]["shared_root_dir"]
    subcomponent_folder = config["cord_prep_settings"][
        "input_subcomponent_folder"
    ]
    # Get all CSV files in the folder (adjust pattern as needed)
    file_pattern = os.path.join(
        os.path.expanduser("~"), root_dir, subcomponent_folder, "*.csv"
    )
    file_paths = glob.glob(file_pattern)

    input_cord_prep_schema_path = Path(
        config["schema_paths"]["schema_dir"],
        config["schema_paths"]["output_mapping_schema_path"],
    )

    # Initialize empty list to store DataFrames
    dfs = []

    # Loop through each file
    for file_path in file_paths:
        try:
            # Read the file using common schema
            df = read_with_schema(file_path, input_cord_prep_schema_path)
            dfs.append(df)

        except Exception as e:
            logger.info(f"Error reading {file_path}: {e}")

    # Concatenate all DataFrames
    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        logger.info(f"Successfully appended {len(dfs)} sub-component files.")
        return combined_df
    else:
        return pd.DataFrame()  # Return empty DataFrame if no files found


def impute_suppression_x(
    df: pd.DataFrame,
    target_cols: List[str],
    transaction_col: str = "transaction",
    lsoa_col: str = "lsoa_code",
    transaction_value: str = "D623",
    lsoa_val: List[str] = ["95", "S"],
) -> pd.DataFrame:
    """
    Set cells in target_cols to "X" where both conditions are met:
      - The value in transaction_col equals transaction_value.
      - The value in lsoa_col starts with any values in lsoa_val list.

    Args:
      df (pd.DataFrame): input DataFrame
      target_cols (List[str]): list of column names to modify.
      transaction_col (str): name of the transaction column.
      lsoa_col (str): name of the LSOA column.
      transaction_value (str): transaction value to match.
      lsoa_val (List[str]): list of starting strings for LSOA codes to match (
        case sensitive).

    Returns:
      pd.DataFrame: DataFrame with suppressed values.
    """
    target_cols = list(target_cols)
    missing = [c for c in target_cols if c not in df.columns]
    if missing:
        raise KeyError(f"Target columns not found in DataFrame: {missing}")

    # Change column dtypes so that we can insert 'X' strings
    for c in target_cols:
        df[c] = df[c].astype("string")

    # Create mask for rows matching both conditions
    mask = (df[transaction_col] == transaction_value) & (
        df[lsoa_col].str.startswith(tuple(lsoa_val))
    )

    # Apply 'X' to all target columns for rows matching mask
    df.loc[mask, target_cols] = "X"

    return df
