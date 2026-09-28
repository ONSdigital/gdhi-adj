"""
Module for deduplicating data between runs in the GDHI pipeline.

Public Functions:
    * combine_outputs
    * drop_duplicate_LSOA_codes
    * standardise_deduplicated_df

Private Functions:
    * None.
"""

import pandas as pd


def combine_outputs(input_dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Concatenates the preprocessing and disclosure data.

    Args:
        input_dfs (list[pd.DataFrame]): A list of preprocessed GDHI DataFrames as input.

    Returns:
        The con
    """
    combined_df = pd.concat(input_dfs, ignore_index=True)

    return combined_df


def drop_duplicate_LSOA_codes(combined_df: pd.DataFrame) -> pd.DataFrame:
    """
    Drops duplicate LSOA codes from the combined dataframe.

    Args:
        combined_df (pd.DataFrame): The combined dataframe of preprocessing and disclosure data.

    Returns:
        pd.DataFrame: Deduplicated data.
    """
    dedup_df = combined_df.drop_duplicates("lsoa_code", ignore_index=True)

    return dedup_df


def standardise_deduplicated_df(deduplicated_df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardises the deduplicated data.

    * Any columns starting with "Unnamed" are removed.
    * String values containing decimals are standardised to whole values e.g., 2010.00 -> 2020.

    Args:
        deduplicated_df (pd.DataFrame): The deduplicated data.

    Returns:
        pd.DataFrame: Standardised deduplicated data.
    """
    df = deduplicated_df.loc[:, ~deduplicated_df.columns.str.contains("^Unnamed")]

    df["year"] = df.year.str.replace(r"\..*", "", regex=True)

    return df
