"""Runs the deduplication module of the GDHI adjustment pipeline."""

import pathlib

from gdhi_adj.deduplication.deduplication import combine_outputs, drop_duplicate_LSAO_codes
from gdhi_adj.utils.helpers import read_with_schema
from gdhi_adj.utils.logger import GDHI_adj_logger

GDHI_adj_LOGGER = GDHI_adj_logger(__name__)
logger = GDHI_adj_LOGGER.logger


def run_deduplication(config: dict) -> None:
    """
    Run the deduplication process for the GDHI pipeline.

    Args:
        config (dict): The configuration settings used in the deduplication process.

    Returns:
        None.
        The deduplicated data is saved as a csv file. It saves the processed
    """
    logger.info("Deduplication started")

    logger.info("Loading configuration settings")
    module_config = config["deduplication_settings"]
    schema_dir = config["schema_paths"]["schema_dir"]
    root_dir = config["user_settings"]["shared_root_dir"]
    input_paths = module_config["input_filepaths"]

    input_paths = [pathlib.Path(root_dir).joinpath(path).expanduser() for path in input_paths]
    input_dedup_schema_path = pathlib.Path(
        schema_dir, config["schema_paths"]["input_dedup_schema_name"]
    )
    logger.info("Configuration settings loaded successfully")

    logger.info("Reading in data with schemas")
    input_dfs = [read_with_schema(file_path, input_dedup_schema_path) for file_path in input_paths]

    logger.info("Combining preprocessing and disclosure data")
    df = combine_outputs(input_dfs)

    logger.info("Dropping duplicate values.")
    df = drop_duplicate_LSAO_codes(df)

    # Save output file with new filename if specified
    # if config["user_settings"]["output_data"]:
    #     # Write DataFrame to CSV
    #     write_with_schema(df, output_schema_path, output_dir, new_filename)
