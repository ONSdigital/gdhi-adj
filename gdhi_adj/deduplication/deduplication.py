"""
Module for deduplicating data between runs in the GDHI pipeline.

Public Functions:
    * drop_duplicate_LSOA_codes

Private Functions:
    * _standardise_deduplicated_df
"""

import pandas as pd

from gdhi_adj.utils.logger import GDHI_adj_logger

GDHI_adj_LOGGER = GDHI_adj_logger(__name__)
logger = GDHI_adj_LOGGER.logger


def drop_duplicate_LSOA_codes(input_dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Combines a list of dataframes and deduplicates them.

    Duplicate LSOA codes are dropped from the combined output and
    colum names are standardised.

    Args:
        input_dfs (list[pd.DataFrame]): A list of preprocessed GDHI DataFrames as input.

    Returns:
        pd.DataFrame: Deduplicated data.

    """
    logger.info("Combining input datasets.")
    combined_df = pd.concat(input_dfs, ignore_index=True)

    logger.info("Dropping duplicate values.")
    dedup_df = combined_df.drop_duplicates("LSOA code", ignore_index=True)

    df = _standardise_deduplicated_df(dedup_df)

    return df


def _standardise_deduplicated_df(deduplicated_df: pd.DataFrame) -> pd.DataFrame:
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

    df["Year"] = df.Year.str.replace(r"\..*", "", regex=True)

    return df
