"""Module for pre-processing data in the gdhi_adj project."""

import os

import pandas as pd

from gdhi_adj.cord_preparation.transform_cord_prep import (
    append_all_sub_components,
    impute_suppression_x,
)
from gdhi_adj.cord_preparation.validation_cord_prep import (
    check_lsoa_consistency,
    check_no_negative_values,
    check_no_nulls,
    check_subcomponent_lookup,
    check_year_column_completeness,
)
from gdhi_adj.utils.helpers import write_with_schema
from gdhi_adj.utils.logger import GDHI_adj_logger

GDHI_adj_LOGGER = GDHI_adj_logger(__name__)
logger = GDHI_adj_LOGGER.logger


def run_cord_preparation(config: dict) -> None:
    """
    Run the CORD preparation steps for the GDHI adjustment project.

    This function performs the following steps:
    1. Load the configuration settings.
    2. Load the input data.
    3. Apply CORD-specific transformations.
    4. Save the prepared CORD data for further processing.

    Args:
        config (dict): Configuration dictionary containing user settings and
        pipeline settings.
    Returns:
        None: The function does not return any value. It saves the processed
        DataFrame to a CSV file.
    """
    logger.info("CORD Preparation started")

    logger.info("Loading configuration settings")
    module_config = config["cord_prep_settings"]
    schema_path = config["pipeline_settings"]["schema_path"]

    output_data_prefix = config["user_settings"]["output_data_prefix"] + "_"
    output_dir = os.path.join(
        "C:/Users", os.getlogin(), module_config["output_dir"]
    )
    output_schema_path = os.path.join(
        schema_path,
        config["pipeline_settings"]["output_cord_prep_schema_path"],
    )
    output_filename = output_data_prefix + module_config.get(
        "output_filename", None
    )

    logger.info("Reading in mapped data for CORD preparation")
    df = append_all_sub_components(config)
    subcomponent_lookup = pd.read_csv(
        os.path.join(
            "C:/Users",
            os.getlogin(),
            module_config["input_subcomponent_folder"],
            module_config["subcomponent_lookup_file_path"],
        )
    )

    logger.info("Performing validation checks on input data")
    check_subcomponent_lookup(df, subcomponent_lookup)
    check_lsoa_consistency(df)
    if config["cord_prep_settings"]["accept_negatives"] is False:
        check_no_negative_values(df)
    check_year_column_completeness(df)

    logger.info("Applying CORD-specific transformations and filters")
    df = impute_suppression_x(
        df,
        target_cols=[
            "2010",
            "2011",
            "2012",
        ],
        transaction_col="transaction",
        lad_col="lad_code",
        transaction_value="D623",
        lad_val=["95", "S"],
    )

    check_no_nulls(df)

    # Save prepared CORD data file with new filename if specified
    if config["user_settings"]["output_data"]:
        write_with_schema(df, output_schema_path, output_dir, output_filename)

    logger.info("CORD Preparation completed and data saved")
