"""Tests functions with deduplication.deduplication.py."""
import pandas as pd

from gdhi_adj.deduplication.deduplication import (
    _standardise_deduplicated_df,
    drop_duplicate_LSOA_codes,
)


class TestDeduplication:
    """
    Tests functions within deduplication.py.
    * drop_duplicate_LSOA_codes
    * _standardise_deduplicated_df
    """

    def test_drop_duplicate_LSOA_codes(self):
        """Tests functionality of drop_duplicate_LSOA_codes."""
        # Arrange
        df_columns = ["LSOA code", "LSOA name", "2010", "Adjust", "Year"]

        test_df_1_values = [["E0100", "Barnet", -0.087, True, "2010"],  # duplicate row
                            ["E0101", "Camden", 200, False, ""],
                            ["E0102", "Hackney", -300.12, True, "2010"]]  # duplicate row

        test_1_df = pd.DataFrame(test_df_1_values, columns=df_columns)

        test_df_2_values = [["E0100", "Barnet", -0.087, True, "2010"],  # duplicate row
                            ["E0103", "Haringey", -0892.0, True, "2010"],
                            ["E0104", "Islington", 0, False, ""],
                            ["E0105", "Kensington", -300.12, True, "2010"],
                            ["E0101", "Camden", 200, False, ""],  # duplicate row
                            ["E0102", "Hackney", -300.12, True, "2010"]]  # duplicate row

        test_2_df = pd.DataFrame(test_df_2_values, columns=df_columns)

        expected_data = [["E0100", "Barnet", -0.087, True, "2010"],
                         ["E0101", "Camden", 200, False, ""],
                         ["E0102", "Hackney", -300.12, True, "2010"],
                         ["E0103", "Haringey", -0892.0, True, "2010"],
                         ["E0104", "Islington", 0, False, ""],
                         ["E0105", "Kensington", -300.12, True, "2010"]]

        expected_df = pd.DataFrame(expected_data, columns=df_columns)

        # Act
        result_df = drop_duplicate_LSOA_codes([test_1_df, test_2_df])

        # Assert
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_standardise_deduplicated_df(self):
        """
        Tests functionality of standardise_deduplicated_df.
        """
        # Arrange
        test_cols = ["LSOA code", "LSOA name", "2010", "Adjust", "Year", "Unnamed_year", "Unnamed"]

        test_data = [["E0100", "Barnet", -0.087, True, "2010.00", "", ""],
                     ["E0101", "Camden", 200, False, "", "", ""],
                     ["E0102", "Hackney", -300.12, True, "2010", "", ""],
                     ["E0103", "Haringey", -0892.0, True, "2010.00", "", ""],
                     ["E0104", "Islington", 0, False, ""],
                     ["E0105", "Kensington", -300.12, True, "2010", "", ""]]

        test_df = pd.DataFrame(test_data, columns=test_cols)

        expected_cols = ["LSOA code", "LSOA name", "2010", "Adjust", "Year"]

        expected_data = [["E0100", "Barnet", -0.087, True, "2010"],
                         ["E0101", "Camden", 200, False, ""],
                         ["E0102", "Hackney", -300.12, True, "2010"],
                         ["E0103", "Haringey", -0892.0, True, "2010"],
                         ["E0104", "Islington", 0, False, ""],
                         ["E0105", "Kensington", -300.12, True, "2010"]]

        expected_df = pd.DataFrame(expected_data, columns=expected_cols)

        # Act
        result_df = _standardise_deduplicated_df(test_df)

        # Assert
        pd.testing.assert_frame_equal(result_df, expected_df)
