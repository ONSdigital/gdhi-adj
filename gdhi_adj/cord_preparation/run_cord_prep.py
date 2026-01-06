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
    3. Perform validation checks on the input data.
    4. Apply CORD-specific transformations.
    5. Save the prepared CORD data for further processing.

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
    schema_dir = config["schema_paths"]["schema_dir"]
    root_dir = config["user_settings"]["shared_root_dir"]

    output_data_prefix = config["user_settings"]["output_data_prefix"] + "_"
    output_dir = os.path.join(
        os.path.expanduser("~"), root_dir, module_config["output_dir"]
    )
    output_schema_path = os.path.join(
        schema_dir,
        config["schema_paths"]["output_cord_prep_schema_path"],
    )
    output_filename = output_data_prefix + module_config.get(
        "output_filename", None
    )

    logger.info("Reading in mapped data for CORD preparation")
    df = append_all_sub_components(config)
    subcomponent_lookup = pd.read_csv(
        os.path.join(
            os.path.expanduser("~"),
            root_dir,
            module_config["input_subcomponent_folder"],
            module_config["subcomponent_lookup_file_path"],
        )
    )

    logger.info("Performing validation checks on input data")
    check_subcomponent_lookup(df, subcomponent_lookup)
    check_lsoa_consistency(df)
    check_year_column_completeness(df)
    if config["user_settings"]["accept_negatives_cord"] is False:
        check_no_negative_values(df)

    logger.info("Applying CORD-specific transformations and filters")
    df = impute_suppression_x(
        df,
        target_cols=[
            "2010",
            "2011",
            "2012",
        ],
        transaction_col="transaction",
        lsoa_col="lsoa_code",
        transaction_value="D623",
        lsoa_val=["95", "S"],
    )

    check_no_nulls(df)

    df = df.drop(columns=["lsoa_name", "lad_code", "lad_name"])

    logger.info("Final formatting and saving prepped CORD data")
    year_cols = [col for col in df.columns if col.isdigit()]
    id_cols = ["transaction", "lsoa_code", "account_entry"]
    df = df.reindex(columns=id_cols + year_cols)
    df = df.sort_values(
        by=["lsoa_code", "transaction", "account_entry"]
    ).reset_index(drop=True)

    # Save prepared CORD data file with new filename if specified
    if config["user_settings"]["output_data"]:
        write_with_schema(df, output_schema_path, output_dir, output_filename)

    logger.info("CORD Preparation completed and data saved")
