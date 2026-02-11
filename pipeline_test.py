"""Test outputs from pipeline modules match expected results.

Only test one module at a time.
"""

import pathlib

import pandas as pd

from gdhi_adj.pipeline import run_pipeline
from gdhi_adj.utils.helpers import load_toml_config
from gdhi_adj.utils.logger import GDHI_adj_logger

GDHI_adj_LOGGER = GDHI_adj_logger(__name__)
logger = GDHI_adj_LOGGER.logger

# config path
config_path = pathlib.Path("config/config.toml")

# Load config
config = load_toml_config(config_path)

# Run the pipeline with config path
result_df = run_pipeline(config_path)

expected_df = pd.read_csv(
    pathlib.Path.expanduser(
        pathlib.Path(config["user_settings"]["shared_root_dir"])
        / pathlib.Path(
            config["adjustment_settings"]["expected_output_filepath"],
        )
    )
)

logger.info("Filtering result data to match LSOAs in worked example")
expected_lsoas = expected_df["lsoa_code"].tolist()
result_df = result_df[result_df["lsoa_code"].isin(expected_lsoas)].reset_index(
    drop=True
)

logger.info("Checking whether pipeline output matches expected output.")
result_df.columns = result_df.columns.astype(str)
breakpoint()
pd.testing.assert_frame_equal(
    result_df,
    expected_df,
    check_dtype=False,
    check_column_type=False,
    rtol=1e-12,
)

logger.info("Pipeline output matches expected output with tolerance.")
