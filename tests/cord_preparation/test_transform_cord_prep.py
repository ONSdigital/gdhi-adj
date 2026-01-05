import pandas as pd

from gdhi_adj.cord_preparation.transform_cord_prep import (
    append_all_sub_components,
    impute_suppression_x,
)


class TestAppendAllSubComponents:
    def test_append_all_sub_components_success(self, mocker):
        """
        Test successfully appending multiple CSV files into a single DataFrame.
        """
        # Mock config dictionary
        config = {
            "cord_prep_settings": {
                "input_subcomponent_folder": "/fake/path"
            },
            "schema_paths": {
                "schema_dir": "/fake/schema",
                "output_mapping_schema_path": "schema.toml"
            }
        }

        # Mock file paths that glob.glob should return
        mock_file_paths = [
            "C:/fake/path/file1.csv",
            "C:/fake/path/file2.csv",
            "C:/fake/path/file3.csv"
        ]

        # Mock DataFrames to be returned by read_with_schema
        mock_df1 = pd.DataFrame({
            "col1": [1, 2],
            "col2": ["a", "b"]
        })
        mock_df2 = pd.DataFrame({
            "col1": [3, 4],
            "col2": ["c", "d"]
        })
        mock_df3 = pd.DataFrame({
            "col1": [5, 6],
            "col2": ["e", "f"]
        })

        # Mock glob.glob to return our fake file paths
        mock_glob = mocker.patch(
            "gdhi_adj.cord_preparation.transform_cord_prep.glob.glob"
        )
        mock_glob.return_value = mock_file_paths

        # Mock read_with_schema to return different DataFrames for each call
        mock_read = mocker.patch(
            "gdhi_adj.cord_preparation.transform_cord_prep.read_with_schema"
        )
        mock_read.side_effect = [mock_df1, mock_df2, mock_df3]

        # Call the function
        result_df = append_all_sub_components(config)

        # Verify the result is concatenated correctly
        expected_df = pd.concat(
            [mock_df1, mock_df2, mock_df3], ignore_index=True
        )
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_append_all_sub_components_no_files(self, mocker):
        """Test behavior when no files are found in the folder."""
        config = {
            "cord_prep_settings": {
                "input_subcomponent_folder": "/empty/path"
            },
            "schema_paths": {
                "schema_dir": "/fake/schema",
                "output_mapping_schema_path": "schema.toml"
            }
        }

        # Mock glob.glob to return empty list
        mock_glob = mocker.patch(
            "gdhi_adj.cord_preparation.transform_cord_prep.glob.glob"
        )
        mock_glob.return_value = []

        result_df = append_all_sub_components(config)

        # Verify empty DataFrame is returned
        assert result_df.empty
        assert len(result_df.columns) == 0

    def test_append_all_sub_components_read_error(self, mocker):
        """Test behavior when one file fails to read."""
        config = {
            "cord_prep_settings": {
                "input_subcomponent_folder": "/fake/path"
            },
            "schema_paths": {
                "schema_dir": "/fake/schema",
                "output_mapping_schema_path": "schema.toml"
            }
        }

        mock_file_paths = [
            "/fake/path/file1.csv",
            "/fake/path/file2.csv"  # This will fail
        ]

        mock_df1 = pd.DataFrame({
            "col1": [1, 2],
            "col2": ["a", "b"]
        })

        mock_glob = mocker.patch(
            "gdhi_adj.cord_preparation.transform_cord_prep.glob.glob"
        )
        mock_glob.return_value = mock_file_paths

        # Mock read_with_schema to succeed for first file, fail for second
        mock_read = mocker.patch(
            "gdhi_adj.cord_preparation.transform_cord_prep.read_with_schema"
        )
        mock_read.side_effect = [mock_df1, Exception("File read error")]

        result_df = append_all_sub_components(config)

        # Should still return the successfully read DataFrame
        pd.testing.assert_frame_equal(result_df, mock_df1)

        # Verify both calls were attempted
        assert mock_read.call_count == 2


def test_impute_suppression_x():
    """Test the impute_suppression_x function returns the expected midpoint row
    """
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E1", "S2", "S2", "95A", "95A",],
        "transaction": ["D33", "D623", "D33", "D623", "D33", "D623",],
        "2010": [10.0, 11.0, 12.0, 13.0, 14.0, 15.0,],
        "2011": [20.0, 21.0, 22.0, 23.0, 24.0, 25.0,],
        "2012": [30.0, 31.0, 32.0, 33.0, 34.0, 35.0,],
        "2013": [40.0, 41.0, 42.0, 43.0, 44.0, 45.0,],
    })

    result_df = impute_suppression_x(
        df,
        target_cols=["2010", "2011", "2012",],
        transaction_col="transaction",
        lsoa_col="lsoa_code",
        transaction_value="D623",
        lsoa_val=["95", "S"]
    )

    expected_df = pd.DataFrame({
        "lsoa_code": ["E1", "E1", "S2", "S2", "95A", "95A",],
        "transaction": ["D33", "D623", "D33", "D623", "D33", "D623",],
        "2010": ["10.0", "11.0", "12.0", "X", "14.0", "X",],
        "2011": ["20.0", "21.0", "22.0", "X", "24.0", "X",],
        "2012": ["30.0", "31.0", "32.0", "X", "34.0", "X",],
        "2013": [40.0, 41.0, 42.0, 43.0, 44.0, 45.0,],
    })

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)
