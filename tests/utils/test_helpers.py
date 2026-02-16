"""Unit tests for helper functions."""
import logging
import pathlib

import pandas as pd
import pytest
import toml

from gdhi_adj.utils.helpers import (
    convert_column_types,
    load_toml_config,
    read_with_schema,
    rename_columns,
    validate_schema,
    write_with_schema,
)
from gdhi_adj.utils.logger import GDHI_adj_logger

GDHI_adj_LOGGER = GDHI_adj_logger(__name__)
logger = GDHI_adj_LOGGER.logger


@pytest.fixture
def test_schema() -> str:
    """Create a sample schema to test pass scenario."""
    schema_content = """
    [new_col_name]
    old_name = "Old col name"
    Deduced_Data_Type = "int"

    [lsoa_code]
    old_name = "LSOA code"
    Deduced_Data_Type = "str"

    [2010]
    old_name = "2010"
    Deduced_Data_Type = "float"

    [Adjust]
    old_name = "Adjust"
    Deduced_Data_Type = "bool"
    """
    return schema_content


@pytest.fixture
def test_schema_wrong_col_type() -> str:
    """Create a sample schema to test incorrect data type."""
    schema_content = """
    [new_col_name]
    old_name = "Old col name"
    Deduced_Data_Type = "int"
    """
    return schema_content


@pytest.fixture
def test_schema_file(tmp_path, test_schema: str) -> str:
    """Create a sample schema file path for testing."""
    test_schema_filepath = tmp_path / "schema.toml"
    test_schema_filepath.write_text(test_schema)

    return test_schema_filepath


@pytest.fixture
def test_schema_wrong_col() -> str:
    """Create a sample schema to test when data columns do not match schema."""
    schema_content = """
    [new_col_name]
    old_name = "Old col nom"
    Deduced_Data_Type = "int"

    [lsoa_code]
    old_name = "LSOA code"
    Deduced_Data_Type = "str"
    """
    return schema_content


@pytest.fixture
def test_schema_file_wrong_col(tmp_path, test_schema_wrong_col: str) -> str:
    """Create schema file path to test if data columns do not match schema."""
    test_schema_filepath = tmp_path / "schema.toml"
    test_schema_filepath.write_text(test_schema_wrong_col)

    return test_schema_filepath


@pytest.fixture
def input_data() -> pd.DataFrame:
    """Create a sample input DataFrame for testing."""
    data = {
        "Old col name": [1, 2],
        "LSOA code": ["A1", "B2"],
        "Additional col": ["test1", "test2"],
        "2010": [1.1, 2.2],
        "Adjust": [True, False],
    }
    return pd.DataFrame(data)


@pytest.fixture
def expout_data() -> pd.DataFrame:
    """Create a sample expected output DataFrame for testing."""
    data = {
        "new_col_name": [1, 2],
        "lsoa_code": ["A1", "B2"],
        "Additional col": ["test1", "test2"],
        "2010": [1.1, 2.2],
        "Adjust": [True, False],
    }
    return pd.DataFrame(data)


@pytest.fixture
def test_csv_file(tmp_path, input_data: pd.DataFrame) -> str:
    """Create a sample CSV file for testing."""
    # Write the input data to a csv for testing
    filepath = tmp_path / "test.csv"
    input_data.to_csv(filepath, index=False)
    return filepath


class TestLoadTomlConfig:
    def test_load_toml_config_path_not_exist(self, caplog):
        """Test load_toml_config raises error when path does not exist."""
        caplog.set_level(logging.ERROR)
        load_toml_config(pathlib.Path("test_path.caml"))

        assert "Config file does not exist" in caplog.text
        assert "test_path.caml" in caplog.text

    def test_load_toml_config_path_not_toml(
        self, caplog, tmp_path, test_csv_file
    ):
        """Test load_toml_config raises error when path does not exist."""
        caplog.set_level(logging.ERROR)
        load_toml_config(pathlib.Path(tmp_path / "test.csv"))

        assert "Expected a .toml file. Got" in caplog.text
        assert ".csv" in caplog.text


class TestValidateSchema:
    def test_validate_schema_missing_col(self, expout_data, test_schema):
        """Test validate_schema when columns in data do not match schema."""
        df = expout_data.drop(columns=["lsoa_code"])  # Drop col for mismatch
        test_schema = toml.loads(test_schema)

        with pytest.raises(ValueError) as excinfo:
            validate_schema(df, test_schema)

        assert "Missing expected column:" in str(excinfo.value)
        assert "lsoa_code" in str(excinfo.value)

    def test_validate_schema_type_mismatch(self, expout_data, test_schema):
        """Test validate_schema when column data types do not match schema."""
        df = expout_data.copy()
        df["new_col_name"] = df["new_col_name"].astype(str)
        test_schema = toml.loads(test_schema)

        with pytest.raises(TypeError) as excinfo:
            validate_schema(df, test_schema)

        assert "does not match expected type" in str(excinfo.value)
        assert "new_col_name" in str(excinfo.value)
        assert "int" in str(excinfo.value)


def test_rename_columns(input_data, test_schema, expout_data):
    """Test renaming columns based on schema."""
    # Convert test_schema to dict
    test_schema = toml.loads(test_schema)
    # Rename columns using the function
    renamed_df = rename_columns(input_data, test_schema, logger)

    # Check that the renamed df matches the expected output
    pd.testing.assert_frame_equal(renamed_df, expout_data)

    # Ensure old column names are not present
    with pytest.raises(KeyError, match="Old col name"):
        renamed_df["Old col name"]


def test_convert_column_types_wrong_col_type(
    caplog, test_schema_wrong_col_type
):
    # make a value that will cause int conversion to fail after coercion
    df = pd.DataFrame({"new_col_name": ["not_a_number", "2"]})
    test_schema_wrong_col_type = toml.loads(test_schema_wrong_col_type)

    caplog.set_level(logging.WARNING)
    convert_column_types(
        df.copy(), test_schema_wrong_col_type, GDHI_adj_LOGGER.logger
    )

    # ensure the warning about conversion was logged
    assert "Failed to convert column 'new_col_name' from" in caplog.text
    assert "to int" in caplog.text


def test_read_with_schema(test_csv_file, test_schema_file, expout_data):
    # Creating df using the function and test csv
    df = read_with_schema(test_csv_file, test_schema_file)
    # Make sure the reader function has returned a df
    assert isinstance(df, pd.DataFrame)
    # Check that the df is the same as the expected data
    pd.testing.assert_frame_equal(df, expout_data)
    pytest.raises(KeyError, match="Old col name")  # Ensure old col not present


def test_read_with_schema_missing_col(
        test_csv_file, test_schema_file_wrong_col
):
    """Test reading CSV with schema if columns in data do not match schema."""
    with pytest.raises(
        expected_exception=ValueError,
        match="schema does not exist in DataFrame"
    ):
        read_with_schema(test_csv_file, test_schema_file_wrong_col)


def test_write_with_schema_pass(
        tmp_path, input_data, test_schema_file, expout_data
):
    # Write the output data to a csv using the write function
    output_filepath = tmp_path
    write_with_schema(
        input_data, test_schema_file, output_filepath, "test_output.csv"
    )

    # Read the output file back in
    new_output_filepath = tmp_path / "test_output.csv"
    output_df = pd.read_csv(new_output_filepath)

    # Check that the output df is the same as the expected data
    pd.testing.assert_frame_equal(output_df, expout_data)


def test_write_with_schema_missing_col(
        tmp_path, input_data, test_schema_file_wrong_col
):
    """Test reading CSV with schema if columns in data do not match schema."""
    output_filepath = tmp_path / "test_output.csv"
    with pytest.raises(
        expected_exception=ValueError,
        match="schema does not exist in DataFrame"
    ):
        write_with_schema(
            input_data, test_schema_file_wrong_col,
            output_filepath, "test_output.csv"
        )
