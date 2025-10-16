"""Module for adjusting data in the gdhi_adj project."""

import os

from gdhi_adj.adjustment.calc_adjustment import (
    apply_adjustment,
    calc_adjustment_headroom_val,
    calc_adjustment_val,
    calc_midpoint_val,
    calc_scaling_factors,
)
from gdhi_adj.adjustment.filter_adjustment import (
    filter_anomaly_list,
    filter_lsoa_data,
)
from gdhi_adj.adjustment.join_adjustment import (
    join_analyst_constrained_data,
    join_analyst_unconstrained_data,
)
from gdhi_adj.adjustment.pivot_adjustment import (
    pivot_adjustment_long,
    pivot_wide_dataframe,
)
from gdhi_adj.utils.helpers import read_with_schema, write_with_schema
from gdhi_adj.utils.logger import GDHI_adj_logger

GDHI_adj_LOGGER = GDHI_adj_logger(__name__)
logger = GDHI_adj_LOGGER.logger


def run_adjustment(config: dict) -> None:
    """
    Run the adjustment steps for the GDHI adjustment project.

    This function performs the following steps:
    1. Load the configuration settings.
    2. Load the input data.
    3. Filter PowerBI adjustment selection data.
    4. Join analyst output with Regional Accounts data.
    5. Pivot the DataFrame to long format for manipulation.
    6. Calculate scaling factors.
    7. Calculate headroom and midpoint of time series.
    8. Calculate adjustment and distribute amongst time series.
    9. Pivot data back to wide format.
    10. Save the adjusted data.

    Args:
        config (dict): Configuration dictionary containing user settings and
        pipeline settings.
    Returns:
        None: The function does not return any value. It saves the processed
        DataFrame to a CSV file.
    """
    logger.info("Adjustment started")

    logger.info("Loading configuration settings")
    local_or_shared = config["user_settings"]["local_or_shared"]
    filepath_dict = config[f"adjustment_{local_or_shared}_settings"]
    schema_path = config["pipeline_settings"]["schema_path"]

    input_adj_file_path = (
        "C:/Users/" + os.getlogin() + filepath_dict["input_adj_file_path"]
    )
    input_constrained_file_path = (
        "C:/Users/"
        + os.getlogin()
        + filepath_dict["input_constrained_file_path"]
    )
    input_unconstrained_file_path = (
        "C:/Users/"
        + os.getlogin()
        + filepath_dict["input_unconstrained_file_path"]
    )

    input_adj_schema_path = (
        schema_path + config["pipeline_settings"]["input_adj_schema_name"]
    )
    input_constrained_schema_path = (
        schema_path
        + config["pipeline_settings"]["input_constrained_schema_name"]
    )
    input_unconstrained_schema_path = (
        schema_path
        + config["pipeline_settings"]["input_unconstrained_schema_name"]
    )

    output_dir = "C:/Users/" + os.getlogin() + filepath_dict["output_dir"]
    output_schema_path = (
        schema_path
        + config["pipeline_settings"]["output_adjustment_schema_path"]
    )
    new_filename = filepath_dict.get("output_filename", None)

    logger.info("Reading in data with schemas")
    df_powerbi_output = read_with_schema(
        input_adj_file_path, input_adj_schema_path
    )
    df_constrained = read_with_schema(
        input_constrained_file_path, input_constrained_schema_path
    )
    df_unconstrained = read_with_schema(
        input_unconstrained_file_path, input_unconstrained_schema_path
    )

    logger.info("Filtering for data that requires adjustment.")
    df_powerbi_output = filter_lsoa_data(df_powerbi_output)

    logger.info("Joining analyst output and constrained DAP output")
    df = join_analyst_constrained_data(df_constrained, df_powerbi_output)

    logger.info("Joining analyst output and unconstrained DAP output")
    df = join_analyst_unconstrained_data(df_unconstrained, df)

    logger.info("Pivoting DataFrame long")
    df = pivot_adjustment_long(df)

    logger.info("Calculate scaling factors")
    df_scaling = calc_scaling_factors(df)

    logger.info("Calculating adjustment")
    df_anomaly_lsoas = filter_anomaly_list(df)

    for i in range(len(df_anomaly_lsoas)):
        lsoa_code = df_anomaly_lsoas.iloc[i]["lsoa_code"]
        year_to_adjust = df_anomaly_lsoas.iloc[i]["year_to_adjust"].astype(int)

        logger.info("Calculating adjustment headroom")
        uncon_non_out_sum, headroom_val = calc_adjustment_headroom_val(
            df, df_scaling, lsoa_code, year_to_adjust
        )

        logger.info("Calculating adjustment midpoint")
        outlier_val, midpoint_val = calc_midpoint_val(
            df, lsoa_code, year_to_adjust
        )

        logger.info("Calculating adjustment value")
        adjustment_val = calc_adjustment_val(
            headroom_val, outlier_val, midpoint_val
        )

        logger.info("Updating anomaly year")
        df = apply_adjustment(
            df,
            year_to_adjust,
            adjustment_val,
            uncon_non_out_sum,
        )
    df = pivot_wide_dataframe(df)

    # Save output file with new filename if specified
    if config["user_settings"]["output_data"]:
        # Write DataFrame to CSV
        write_with_schema(df, output_schema_path, output_dir, new_filename)
