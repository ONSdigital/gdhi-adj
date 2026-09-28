"""Tests functions with deduplication.deduplication.py."""
import pandas as pd

from gdhi_adj.deduplication.deduplication import (
    combine_outputs,
    drop_duplicate_LSOA_codes,
    standardise_deduplicated_df,
)


class TestDeduplication:
    """
    Tests functions within deduplication.py.
    * combine_outputs
    * drop_duplicate_LSOA_codes
    * standardise_deduplicated_df
    """

    def test_combine_outputs(self):
        """
        Tests functionality of combine outputs.
        """
        # Arrange
        test_df_1_columns = ["lsoa_code", "lsoa_name", "2010", "adjust", "year"]

        test_df_1_values = [["E0100", "Barnet", -0.087, True, "2010"],
                            ["E0101", "Camden", 200, False, ""],
                            ["E0102", "Hackney", -300.12, True, "2010"]]

        test_1_df = pd.DataFrame(test_df_1_values, columns=test_df_1_columns)

        test_df_2_columns = ["lsoa_code", "lsoa_name", "2010", "adjust", "year"]

        test_df_2_values = [["E0100", "Barnet", -0.087, True, "2010"],  # duplicate row
                            ["E0103", "Haringey", -0892.0, True, "2010"],
                            ["E0104", "Islington", 0, False, ""],
                            ["E0105", "Kensington", -300.12, True, "2010"]]

        test_2_df = pd.DataFrame(test_df_2_values, columns=test_df_2_columns)

        expected_cols = ["lsoa_code", "lsoa_name", "2010", "adjust", "year"]

        test_df_3_cols = ["lsoa_code", "lsoa_name", "2010", "adjust", "year"]

        test_df_3_values = [["E0100", "Barnet", -0.087, True, "2010"],
                            ["E0101", "Camden", 200, False, ""],  # duplicate row
                            ["E0102", "Hackney", -300.12, True, "2010"]]  # duplicate row
        test_3_df = pd.DataFrame(test_df_3_values, columns=test_df_3_cols)

        test_input_dfs = [test_1_df, test_2_df, test_3_df]

        expected_data = [["E0100", "Barnet", -0.087, True, "2010"],
                         ["E0101", "Camden", 200, False, ""],
                         ["E0102", "Hackney", -300.12, True, "2010"],
                         ["E0100", "Barnet", -0.087, True, "2010"],  # duplicate row
                         ["E0103", "Haringey", -0892.0, True, "2010"],
                         ["E0104", "Islington", 0, False, ""],
                         ["E0105", "Kensington", -300.12, True, "2010"],
                         ["E0100", "Barnet", -0.087, True, "2010"],
                         ["E0101", "Camden", 200, False, ""],  # duplicate row
                         ["E0102", "Hackney", -300.12, True, "2010"]]  # duplicate row

        expected_df = pd.DataFrame(expected_data, columns=expected_cols)

        # Act
        result_df = combine_outputs(test_input_dfs)

        # assert
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_drop_duplicate_LSOA_codes(self):
        """Tests functionality of drop_duplicate_LSOA_codes."""
        # Arrange
        test_cols = ["lsoa_code", "lsoa_name", "2010", "adjust", "year"]

        test_data = [["E0100", "Barnet", -0.087, True, "2010"],
                     ["E0101", "Camden", 200, False, ""],
                     ["E0102", "Hackney", -300.12, True, "2010"],
                     ["E0100", "Barnet", -0.087, True, "2010"],  # duplicate row
                     ["E0103", "Haringey", -0892.0, True, "2010"],
                     ["E0104", "Islington", 0, False, ""],
                     ["E0105", "Kensington", -300.12, True, "2010"],
                     ["E0100", "Barnet", -0.087, True, "2010"],
                     ["E0101", "Camden", 200, False, ""],  # duplicate row
                     ["E0102", "Hackney", -300.12, True, "2010"]]  # duplicate row

        test_df = pd.DataFrame(test_data, columns=test_cols)

        expected_cols = ["lsoa_code", "lsoa_name", "2010", "adjust", "year"]

        expected_data = [["E0100", "Barnet", -0.087, True, "2010"],
                         ["E0101", "Camden", 200, False, ""],
                         ["E0102", "Hackney", -300.12, True, "2010"],
                         ["E0103", "Haringey", -0892.0, True, "2010"],
                         ["E0104", "Islington", 0, False, ""],
                         ["E0105", "Kensington", -300.12, True, "2010"]]

        expected_df = pd.DataFrame(expected_data, columns=expected_cols)

        # Act
        result_df = drop_duplicate_LSOA_codes(test_df)

        # Assert
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_standardise_deduplicated_df(self):
        """
        Tests functionality of standardise_deduplicated_df.
        """
        # Arrange
        test_cols = ["lsoa_code", "lsoa_name", "2010", "adjust", "year", "Unnamed_year", "Unnamed"]

        test_data = [["E0100", "Barnet", -0.087, True, "2010.00", "", ""],
                     ["E0101", "Camden", 200, False, "", "", ""],
                     ["E0102", "Hackney", -300.12, True, "2010", "", ""],
                     ["E0103", "Haringey", -0892.0, True, "2010.00", "", ""],
                     ["E0104", "Islington", 0, False, ""],
                     ["E0105", "Kensington", -300.12, True, "2010", "", ""]]

        test_df = pd.DataFrame(test_data, columns=test_cols)

        expected_cols = ["lsoa_code", "lsoa_name", "2010", "adjust", "year"]

        expected_data = [["E0100", "Barnet", -0.087, True, "2010"],
                         ["E0101", "Camden", 200, False, ""],
                         ["E0102", "Hackney", -300.12, True, "2010"],
                         ["E0103", "Haringey", -0892.0, True, "2010"],
                         ["E0104", "Islington", 0, False, ""],
                         ["E0105", "Kensington", -300.12, True, "2010"]]

        expected_df = pd.DataFrame(expected_data, columns=expected_cols)

        # Act
        result_df = standardise_deduplicated_df(test_df)

        # Assert
        pd.testing.assert_frame_equal(result_df, expected_df)
