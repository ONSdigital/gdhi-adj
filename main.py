"""Main file to run the GDHI pipeine"""

from gdhi_adj.pipeline import run_pipeline

# config path
config_path = "config/config.toml"

# Run the pipeline with config path
run_pipeline(config_path)
