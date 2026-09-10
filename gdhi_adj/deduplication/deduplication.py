"""Module for deduplicating data in the GDHI pipeline.

Public Functions:
    * combine_outputs
    * drop_duplicate_LSAO_codes

Private Functions:
    * None.
"""

import pandas as pd


def combine_outputs(preprocess_df: pd.DataFrame, disc_df: pd.DataFrame) -> pd.DataFrame:
    """
    Concatenates the preprocessing and disclosure data.

    Args:
        preprocess_df (pd.DataFrame): The preprocessed GDHI data.
        disc_df (pd.DataFrame): The GDHI data with disclosure.
    """

    combined_df = pd.concat([preprocess_df, disc_df], ignore_index=True)

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
