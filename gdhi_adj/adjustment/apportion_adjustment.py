"""Module for apportioning values from adjustment in the gdhi_adj project."""

import numpy as np
import pandas as pd

from gdhi_adj.utils.transform_helpers import sum_match_check


def calc_non_outlier_proportions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the proportion of a non-outlier LSOA to the LAD for each year.

    Args:
        df (pd.DataFrame): DataFrame containing all GDHI data.

    Returns:
        pd.DataFrame: DataFrame with LAD totals and proportions for non-outlier
            LSOAs calculated per year/LAD group.
    """
    # Filter into outlier and non-outlier LSOAs that need adjusting
    mask = df.apply(lambda r: (r["year"] in r["year_to_adjust"]), axis=1)

    # Calculate the total GDHI for each LAD per year
    df["lad_total"] = df.groupby(["lad_code", "year"])["con_gdhi"].transform(
        "sum"
    )

    # Calculate the total GDHI for each LAD per year for non outlier years
    df["non_outlier_total"] = (
        df[~mask].groupby(["lad_code", "year"])["con_gdhi"].transform("sum")
    )

    # Guard: if any non_outlier_total is zero this will cause div-by-zero
    # when calculating proportions — raise a clear error with offending groups.
    zero_mask = df["non_outlier_total"] == 0
    if zero_mask.any():
        bad_groups = (
            df.loc[zero_mask, ["lad_code", "year"]]
            .drop_duplicates()
            .sort_values(["lad_code", "year"])  # deterministic order
        )
        # convert to a compact list of tuples for a friendly error message
        bad_list = [tuple(x) for x in bad_groups.values.tolist()]
        raise ValueError(
            "Non-outlier total check failed: found zero non_outlier_total for "
            f"groups: {bad_list}"
        )

    df["gdhi_proportion"] = df["con_gdhi"] / df["non_outlier_total"]

    return df


def apportion_adjustment(
    df: pd.DataFrame, imputed_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Apportion the adjustment values to all years for each LSOA.

    Args:
        df (pd.DataFrame): DataFrame containing data to adjust.
        imputed_df (pd.DataFrame): DataFrame containing outlier imputed values.

    Returns:
        pd.DataFrame: DataFrame with outlier values imputed and adjustment.
            values apportioned accross all years within LSOA.
    """
    adjusted_df = df.merge(
        imputed_df[["lsoa_code", "year", "imputed_gdhi"]],
        on=["lsoa_code", "year"],
        how="left",
    )

    adjusted_df["adjusted_total"] = adjusted_df[
        "lad_total"
    ] - adjusted_df.groupby(["lad_code", "year"])["imputed_gdhi"].transform(
        "sum"
    )

    adjusted_df["adjusted_con_gdhi"] = np.where(
        adjusted_df["imputed_gdhi"].notna(),
        adjusted_df["imputed_gdhi"],
        adjusted_df["gdhi_proportion"] * adjusted_df["adjusted_total"],
    )

    # Adjustment check: sums by (lad_code, year) should match pre- and post-
    # adjustment
    adjusted_sum_check_df = adjusted_df.copy()
    sum_match_check(
        adjusted_sum_check_df,
        grouping_cols=["lad_code", "year"],
        unadjusted_col="con_gdhi",
        adjusted_col="adjusted_con_gdhi",
        sum_tolerance=0.000001,
    )

    return adjusted_df


def check_no_negative_values_col(df: pd.DataFrame, col: str) -> None:
    """
    Check that adjusted_con_gdhi has no negative values.

    Args:
        df (pd.DataFrame): DataFrame with adjusted_con_gdhi column.

    Raises:
        ValueError: If negative values are found.
    """
    if not df[df[col] < 0].empty:
        raise ValueError(
            "Negative value check failed: negative values found in "
            f"{col} after adjustment."
        )


def apportion_negative_adjustment(df: pd.DataFrame) -> pd.DataFrame:
    """
    Change negative values to 0 and apportion negative adjustment values to all
    LSOAs within an LAD/year group.

    Args:
        df (pd.DataFrame): DataFrame containing data to adjust.

    Returns:
        pd.DataFrame: DataFrame with negative adjustment values apportioned
            across all years within LSOA.
    """
    adjusted_df = df.rename(
        columns={"adjusted_con_gdhi": "previously_adjusted_con_gdhi"}
    )

    adjusted_df["no_neg_adjusted_gdhi"] = np.where(
        adjusted_df["previously_adjusted_con_gdhi"] < 0,
        0,
        adjusted_df["previously_adjusted_con_gdhi"],
    )

    adjusted_df["sum_neg_adjusted_gdhi"] = adjusted_df.groupby(
        ["lad_code", "year"]
    )["previously_adjusted_con_gdhi"].transform(lambda x: x[x < 0].sum())

    adjusted_df["adjusted_gdhi_proportion"] = adjusted_df[
        "no_neg_adjusted_gdhi"
    ] / (adjusted_df["lad_total"] - adjusted_df["sum_neg_adjusted_gdhi"])

    adjusted_df["adjusted_con_gdhi"] = (
        adjusted_df["adjusted_gdhi_proportion"] * adjusted_df["lad_total"]
    )

    # Checks after adjustment
    # Check that there are no negative values in adjusted_con_gdhi
    check_no_negative_values_col(adjusted_df, "adjusted_con_gdhi")

    # Adjustment check: sums by (lad_code, year) should match pre- and post-
    # adjustment
    adjusted_sum_check_df = adjusted_df.copy()
    sum_match_check(
        adjusted_sum_check_df,
        grouping_cols=["lad_code", "year"],
        unadjusted_col="con_gdhi",
        adjusted_col="adjusted_con_gdhi",
        sum_tolerance=0.000001,
    )

    return adjusted_df.sort_values(by=["lad_code", "year"]).reset_index(
        drop=True
    )


def apportion_rollback_years(df: pd.DataFrame) -> pd.DataFrame:
    """
    Continue to apportion the adjustments for years that are flagged as
    rollback years.

    Args:
        df (pd.DataFrame): DataFrame containing all data including adjusted
            and rollback years.

    Returns:
        pd.DataFrame: DataFrame with reapportioned values for rollback years.
    """
    adjusted_df = df.copy()
    max_rollback_year = adjusted_df[adjusted_df["rollback_flag"]]["year"].max()
    min_rollback_year = adjusted_df[adjusted_df["rollback_flag"]]["year"].min()
    # Not to include max year as that year itself has been adjusted
    if max_rollback_year > 0:
        rollback_years_to_adjust = list(
            range(min_rollback_year, max_rollback_year)
        )
    else:
        rollback_years_to_adjust = []

    # Get the last rollback year's gdhi per lsoa and sum per lad
    lsoa_max_rollback_gdhi = (
        adjusted_df[adjusted_df["year"] == max_rollback_year]
        .groupby("lsoa_code")["adjusted_con_gdhi"]
        .min()
    )
    lad_max_rollback_sums = (
        adjusted_df[adjusted_df["year"] == max_rollback_year]
        .groupby("lad_code")["lad_total"]
        .min()
    )

    adjusted_df["adjusted_rollback_flag"] = adjusted_df[
        "rollback_flag"
    ] & adjusted_df.apply(
        lambda r: max_rollback_year in r["year_to_adjust"], axis=1
    )
    adjusted_df["rollback_adjust_flag"] = adjusted_df.groupby(
        ["lad_code", "year"]
    )["adjusted_rollback_flag"].transform("any")

    # Map back to dataframe and calculate
    adjusted_df["rollback_con_gdhi"] = np.where(
        (
            (adjusted_df["rollback_adjust_flag"])
            & (adjusted_df["year"].isin(rollback_years_to_adjust))
        ),
        (
            adjusted_df["lad_total"]
            * (
                adjusted_df["lsoa_code"].map(lsoa_max_rollback_gdhi)
                / adjusted_df["lad_code"].map(lad_max_rollback_sums)
            )
        ),
        adjusted_df["adjusted_con_gdhi"],
    )

    # Adjustment check: sums by (lad_code, year) should match pre- and post-
    # adjustment
    adjusted_sum_check_df = adjusted_df.copy()
    sum_match_check(
        adjusted_sum_check_df,
        grouping_cols=["lad_code", "year"],
        unadjusted_col="con_gdhi",
        adjusted_col="rollback_con_gdhi",
        sum_tolerance=0.000001,
    )

    return adjusted_df
