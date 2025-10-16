"""Module for calculations to adjust data in the gdhi_adj project."""

import numpy as np
import pandas as pd


def calc_scaling_factors(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate scaling factors for the adjustment.

    Args:
        df (pd.DataFrame): DataFrame with unconstrained and constrained data.

    Returns:
        pd.DataFrame: DataFrame with scaling factors added.
    """
    df_uncon = df[["year", "uncon_gdhi"]].groupby("year", as_index=False).sum()
    df_con = df[["year", "con_gdhi"]].groupby("year", as_index=False).sum()

    df_scaling = df_uncon.merge(df_con, on=["year"], how="left")

    df_scaling["scaling"] = np.where(
        df_scaling["uncon_gdhi"] != 0,
        df_scaling["con_gdhi"] / df_scaling["uncon_gdhi"],
        0,
    )

    return df_scaling


def calc_adjustment_headroom_val(
    df: pd.DataFrame,
    df_scaling: pd.DataFrame,
    lsoa_code: str,
    year_to_adjust: int,
) -> pd.DataFrame:
    """
    Calculate the adjustment headroom available to smooth timeseries.

    Args:
        df (pd.DataFrame): DataFrame containing constrained and unconstrained
        data.
        df_scaling (pd.DataFrame): DataFrame containing scaling factors.
        lsoa_code (str): LSOA code for the adjustment.
        year_to_adjust (int): Year for the adjustment.

    Returns:
        uncon_non_out_sum (float): The sum of non outlier unconstrained GDHI.
        headroom_val (float): The calculated headroom for adjustment.
    """
    mean_scaling = df_scaling[df_scaling["year"] != year_to_adjust][
        "scaling"
    ].mean()

    uncon_non_out_sum = df[
        (df["lsoa_code"] != lsoa_code) & (df["year"] == year_to_adjust)
    ]["uncon_gdhi"].sum()

    con_out_val = df_scaling[df_scaling["year"] == year_to_adjust][
        "con_gdhi"
    ].iloc[0]

    headroom_val = con_out_val - (uncon_non_out_sum * mean_scaling)

    return uncon_non_out_sum, headroom_val


def calc_midpoint_val(
    df: pd.DataFrame,
    lsoa_code: str,
    year_to_adjust: int,
) -> tuple[float, float]:
    """
    Calculate the midpoint value for a given LSOA code.

    Args:
        df (pd.DataFrame): DataFrame containing data to calculate midpoint.
        lsoa_code (str): LSOA code for the adjustment.
        year_to_adjust (int): Year for the adjustment.

    Returns:
        tuple[float, float]: The isolated outlier value and calculated midpoint
        value for the specified LSOA code.
    """
    # if year_to_adjust is the first or last year in the series,
    # method required for determining what to do with midpoint
    outlier_val = df[
        (df["lsoa_code"] == lsoa_code) & (df["year"] == year_to_adjust)
    ]["con_gdhi"].iloc[0]

    prev_year_val = df[
        (df["lsoa_code"] == lsoa_code) & (df["year"] == (year_to_adjust - 1))
    ]["con_gdhi"].iloc[0]

    next_year_val = df[
        (df["lsoa_code"] == lsoa_code) & (df["year"] == (year_to_adjust + 1))
    ]["con_gdhi"].iloc[0]

    midpoint_val = (prev_year_val + next_year_val) / 2

    return outlier_val, midpoint_val


def calc_adjustment_val(
    headroom_val: float, outlier_val: float, midpoint_val: float
) -> float:
    """
    Calculate the adjustment value based on the midpoint and scaled difference.

    Args:
        headroom_val (float): Scaled difference value calculated from the data.
        outlier_val (float): Outlier value to be adjusted.
        midpoint_val (float): Midpoint value to peak/trough.

    Returns:
        adjustment_val (float): The adjustment value to be applied.
    """
    if abs((headroom_val - outlier_val) <= abs(midpoint_val)):
        adjustment_val = headroom_val - outlier_val
    else:
        adjustment_val = midpoint_val - outlier_val

    return adjustment_val


def apply_adjustment(
    df: pd.DataFrame,
    year_to_adjust: int,
    adjustment_val: float,
    uncon_non_out_sum: float,
) -> pd.DataFrame:
    """
    Apply the adjustment values to the LSOAs for the anomaly year.

    Args:
        df (pd.DataFrame): DataFrame containing data to adjust.
        year_to_adjust (int): Year for the adjustment.
        adjustment_val (float): Adjustment value to be applied.
        uncon_non_out_sum (float): Sum of non-outlier unconstrained GDHI.

    Returns:
        pd.DataFrame: DataFrame with adjustment values calculated.
    """
    condition_outlier = (df["adjust"].astype("boolean").fillna(False)) & (
        df["year"] == year_to_adjust
    )
    df.loc[condition_outlier, "con_gdhi"] += adjustment_val

    condition_non_outlier = (~df["adjust"].astype("boolean").fillna(False)) & (
        df["year"] == year_to_adjust
    )
    df.loc[condition_non_outlier, "con_gdhi"] = df["con_gdhi"] + (
        abs(adjustment_val) * (df["uncon_gdhi"] / uncon_non_out_sum)
    )

    return df
