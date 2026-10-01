"""
Applies filtering to the first-run of the preprocessed GDHI output data.

After preprocessing the data on an initial run, this module applies filtering to remove LSOAs
that have been processed and labelled as 'FALSE' for adjustment.

Public Functions:
    *

Private Functions:
    *
"""

import pandas as pd


def apply_post_run_filter(run_1_df: pd.DataFrame, run_2_df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply post-run filtering to the preprocessed GDHI output data.

    Args:
        run_1_df (pd.DataFrame): The DataFrame containing the preprocessed GDHI output data
                                 from the first module run.
        run_2_df (pd.DataFrame): The DataFrame containing the second run of the preprocessed GDHI
                                 output data.
    """
