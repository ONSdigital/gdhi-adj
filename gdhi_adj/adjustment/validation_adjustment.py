"""Module for adjustment data validation in the gdhi_adj project."""

import pandas as pd

from gdhi_adj.utils.logger import GDHI_adj_logger
from gdhi_adj.utils.transform_helpers import ensure_list

GDHI_adj_LOGGER = GDHI_adj_logger(__name__)
logger = GDHI_adj_LOGGER.logger


def check_lsoas_flagged(df: pd.DataFrame) -> pd.DataFrame:
    """
    Check that not all LSOAs within an LAD are flagged for adjustment.

    This is so that there are some non-outlier LSOAs to calculate non-outlier
    proportions of the total GDHI within an LAD.

    Args:
        df (pd.DataFrame): Input DataFrame to be checked.

    Returns:
        pd.DataFrame: The original DataFrame, unchanged. This allows the
            function to be used in method chaining (e.g., .pipe()).

    Raises:
        ValueError: If every 'lsoa_code' within an lad_code is marked for
            adjustment.
    """
    flagged_lsoas = df[["lsoa_code", "lad_code", "year", "adjust"]].copy()

    # Count unique LSOAs per LAD and year
    flagged_lsoas["lsoa_count"] = flagged_lsoas.groupby(["lad_code", "year"])[
        "lsoa_code"
    ].transform("nunique")

    # Count unique LSOAs per LAD and year that are marked for adjustment
    flagged_lsoas["adjust_count"] = (
        flagged_lsoas[flagged_lsoas["adjust"]]
        .groupby(["lad_code", "year"])["lsoa_code"]
        .transform("nunique")
    )

    # Filter where all LSOAs are marked for adjustment
    flagged_lsoas = flagged_lsoas[
        flagged_lsoas["lsoa_count"] == flagged_lsoas["adjust_count"]
    ].drop_duplicates()

    if not flagged_lsoas.empty:
        error_msg = "All LSOAs have been marked for adjustment."
        logger.error(error_msg)
        raise ValueError(error_msg)
    else:
        logger.info(
            "All LADs have at least some LSOAs not marked for adjustment."
        )

    return df


def check_years_flagged(df: pd.DataFrame) -> pd.DataFrame:
    """
    Check that not all years within an LSOA are flagged for adjustment.

    This is so that there are some non-outlier LSOAs to interpolate/extrapolate
    from.

    Args:
        df (pd.DataFrame): Input DataFrame to be checked.

    Returns:
        pd.DataFrame: The original DataFrame, unchanged. This allows the
            function to be used in method chaining (e.g., .pipe()).

    Raises:
        ValueError: If every year within an 'lsoa_code' is marked for
            adjustment.
    """
    flagged_years = df[
        ["lsoa_code", "year", "adjust", "year_to_adjust"]
    ].copy()

    # ensure year_to_adjust is list-like and normalize missing
    flagged_years["year_to_adjust"] = flagged_years["year_to_adjust"].apply(
        ensure_list
    )

    # Count years per LSOA
    flagged_years["year_count"] = flagged_years.groupby("lsoa_code")[
        "year"
    ].transform("nunique")

    # Count years flagged for adjustment per LSOA
    flagged_years["adjust_count"] = flagged_years["year_to_adjust"].apply(
        lambda x: len(x) if isinstance(x, list) else 0
    )

    # Filter to LSOAs where all years are marked for adjustment
    flagged_years = flagged_years[
        (flagged_years["year_count"] == flagged_years["adjust_count"])
    ]

    flagged_full_years_lsoas = flagged_years["lsoa_code"].unique().tolist()

    if flagged_years.empty:
        logger.info("All LSOAs have at least some years as non-outliers.")
    else:
        error_msg = (
            "The following lsoa_codes have all years marked for adjustment: "
            f"{', '.join(flagged_full_years_lsoas)}"
        )
        logger.error(error_msg)
        raise ValueError(error_msg)

    return df


def check_adjust_year_not_empty(df: pd.DataFrame) -> pd.DataFrame:
    """
    Check that for LSOAs marked for adjustment, the year column is not empty.

    Args:
        df (pd.DataFrame): Input DataFrame to be checked.

    Returns:
        pd.DataFrame: The original DataFrame, unchanged. This allows the
            function to be used in method chaining (e.g., .pipe()).

    Raises:
        ValueError: If an LSOA marked for adjustment does not have a year
            specified to adjust.
    """
    adjust_df = df[["lsoa_code", "adjust", "year_to_adjust"]].copy()

    # ensure year_to_adjust is list-like and normalize missing
    adjust_df["year_to_adjust"] = adjust_df["year_to_adjust"].apply(
        ensure_list
    )

    # Count years flagged for adjustment per LSOA
    adjust_df["adjust_count"] = adjust_df["year_to_adjust"].apply(
        lambda x: len(x) if isinstance(x, list) else 0
    )

    # Filter to LSOAs marked for adjustment with empty year_to_adjust
    adjust_df = adjust_df[
        ((adjust_df["adjust"]) & (adjust_df["adjust_count"] == 0))
    ]

    lsoas_missing_years_to_adjust = adjust_df["lsoa_code"].unique().tolist()

    if not adjust_df.empty:
        error_msg = (
            "The following lsoa_codes are marked for adjustment but have no "
            "years specified: "
            f"{', '.join(lsoas_missing_years_to_adjust)}"
        )
        logger.error(error_msg)
        raise ValueError(error_msg)
    else:
        logger.info(
            "All LSOAs marked for adjustment have at least one year specified."
        )

    return df
