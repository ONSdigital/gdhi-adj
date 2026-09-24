"""
Module for deduplicating data between runs in the GDHI pipeline.

Public Functions:
    * combine_outputs
    * drop_duplicate_LSAO_codes
    * standardise_deduplicated_df

Private Functions:
    * None.
"""

import pandas as pd


def combine_outputs(input_dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Concatenates the preprocessing and disclosure data.

    Args:
        preprocess_df (pd.DataFrame): The preprocessed GDHI data.
        disc_df (pd.DataFrame): The GDHI data with disclosure.
    """

    combined_df = pd.concat(input_dfs, ignore_index=True)

    return combined_df


def drop_duplicate_LSAO_codes(combined_df: pd.DataFrame) -> pd.DataFrame:
    """
    Drops duplicate LSAO codes from the combined dataframe.

    Args:
        combined_df (pd.DataFrame): The combined dataframe of preprocessing and disclosure data.

    Returns:
        pd.DataFrame: Deduplicated data.
    """

    dedup_df = combined_df.drop_duplicates("LSAO_code", ignore_index=True)

    return dedup_df


def standardise_deduplicated_df(deduplicated_df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardises the deduplicated data.

    * Any columns starting with "Unnamed" are removed.
    * String valus containing decimals are standardised to whole values e.g., 2010.00 -> 2020.

    Args:
        deduplicated_df (pd.DataFrame): The deduplicated data.

    Returns:
        pd.DataFrame: Standardised deduplicated data.
    """
    # Remove any columns that start with the name "Unnamed"
    df = deduplicated_df.loc[:, ~deduplicated_df.columns.str.contains("^Unnamed")]

    # Remove decimal values from year column
    df["Year"] = df.Year.str.replace(r"\..*", "", regex=True)

    return df
