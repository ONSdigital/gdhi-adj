"""Module for apportioning values from adjustment in the gdhi_adj project."""

import numpy as np
import pandas as pd


def apportion_adjustment(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apportion the adjustment values to all years for each LSOA.

    Args:
        df (pd.DataFrame): DataFrame containing data to adjust.

    Returns:
        pd.DataFrame: DataFrame with outlier values imputed and adjustment
        values apportioned accross all years within LSOA.
    """
    adjusted_df = df.copy()

    adjusted_df["lsoa_count"] = adjusted_df.groupby(["lad_code", "year"])[
        "lsoa_code"
    ].transform("count")

    adjusted_df["adjusted_con_gdhi"] = np.where(
        adjusted_df["imputed_gdhi"].notna(),
        adjusted_df["imputed_gdhi"],
        adjusted_df["con_gdhi"],
    )

    adjusted_df["adjusted_con_gdhi"] += np.where(
        adjusted_df["adjustment_val"].notna(),
        adjusted_df["adjustment_val"] / adjusted_df["lsoa_count"],
        0,
    )

    return adjusted_df.sort_values(by=["lad_code", "year"]).reset_index(
        drop=True
    )


def apportion_negative_adjustment(df: pd.DataFrame) -> pd.DataFrame:
    """
    Change negative values to 0 and apportion negative adjustment values to all
    LSOAs within an LAD/year group.

    Args:
        df (pd.DataFrame): DataFrame containing data to adjust.

    Returns:
        pd.DataFrame: DataFrame with negative adjustment values apportioned
        accross all years within LSOA.
    """
    adjusted_df = df.copy()

    adjusted_df["negative_diff"] = np.where(
        adjusted_df["adjusted_con_gdhi"] < 0,
        0 - adjusted_df["adjusted_con_gdhi"],
        0,
    )

    adjusted_df["adjustment_val"] = adjusted_df.groupby(["lad_code", "year"])[
        "negative_diff"
    ].transform("sum")

    adjusted_df["lsoa_count"] = (
        adjusted_df[adjusted_df["adjusted_con_gdhi"] > 0]
        .groupby(["lad_code", "year"])["lsoa_code"]
        .transform("count")
    )

    zero_lsoa_check = adjusted_df[
        (adjusted_df["adjusted_con_gdhi"] > 0)
        & (~adjusted_df["lsoa_count"].map(lambda x: x >= 0))
    ].empty

    if zero_lsoa_check is False:
        raise ValueError(
            "Zero LSOA count check failed: no LSOAs have been found with a "
            "positive adjusted_con_gdhi value, this will lead to div0 error."
        )

    adjusted_df["adjusted_con_gdhi"] = np.where(
        adjusted_df["adjusted_con_gdhi"] > 0,
        adjusted_df["adjusted_con_gdhi"]
        - (adjusted_df["adjustment_val"] / adjusted_df["lsoa_count"]),
        0,
    )

    # Checks after adjustment
    # Check that there are no negative values in adjusted_con_gdhi
    adjusted_df_check = adjusted_df.copy()
    negative_value_check = adjusted_df_check[
        adjusted_df_check["adjusted_con_gdhi"] < 0
    ].empty

    if negative_value_check is False:
        raise ValueError(
            "Negative value check failed: negative values found in "
            "adjusted_con_gdhi after adjustment."
        )

    # Adjustment check: sums by (lad_code, year) should match pre- and post-
    # adjustment
    adjusted_df_check["unadjusted_sum"] = adjusted_df.groupby(
        ["lad_code", "year"]
    )["con_gdhi"].transform("sum")

    adjusted_df_check["adjusted_sum"] = adjusted_df_check.groupby(
        ["lad_code", "year"]
    )["adjusted_con_gdhi"].transform("sum")

    adjusted_df_check["adjustment_check"] = abs(
        adjusted_df_check["unadjusted_sum"] - adjusted_df_check["adjusted_sum"]
    )

    adjusted_df_check = adjusted_df_check[
        adjusted_df_check["adjustment_check"] > 0.000001
    ]

    if not adjusted_df_check.empty:
        raise ValueError(
            "Adjustment check failed: LAD sums do not match after adjustment."
        )

    return adjusted_df.sort_values(by=["lad_code", "year"]).reset_index(
        drop=True
    )
