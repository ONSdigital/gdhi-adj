"""
Applies filtering to the first-run of the preprocessed GDHI output data.

After preprocessing the data on an initial run, this module applies filtering to remove LSOAs
that have been processed and labelled as 'FALSE' for adjustment.

Public Functions:
    * apply_post_run_filter

Private Functions:
    * None
"""

import pandas as pd


def apply_post_run_filter(prev_class_df: pd.DataFrame, preprocess_df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply post-run filtering to the preprocessed GDHI output data.

    Preprocessed data is compared to previously classified data to check for LSOAs
    that have already been examined. If the LSOA exists in the classified data and 'Adjust'
    is marked as False, they filtered from the preprocessed data.

    Args:
        prev_class_df (pd.DataFrame): Data containing previously examined LSOAs.
        preprocess_df (pd.DataFrame): The current preprocessed data.
    """
    filtered_df = preprocess_df.loc[
        (
            (~preprocess_df["LSOA code"].isin(prev_class_df["LSOA code"]))
            & (preprocess_df["Adjust"] is False)
        )
    ]

    return filtered_df


def export_filtered_output(filtered_df: pd.DataFrame, output_path: str, output_name: str) -> None:
    """
    Export the post-run filtered DataFrame to a CSV file.

    Args:
        filtered_df (pd.DataFrame): The filtered DataFrame to be exported.
        output_path (str): The file path where the filtered DataFrame will be saved.
        output_name (str): The name of the filtered CSV file.
    """
    filtered_df.to_csv(output_path + output_name, index=False)
