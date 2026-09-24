"""Tests functions with deduplication.deduplication.py."""
import pandas as pd

from gdhi_adj.deduplication.deduplication import combine_outputs


class TestDeduplication:
    """
    Tests functions within deduplication.py.
    * combine_outputs
    * drop_duplicate_LSAO_codes
    * standardise_deduplicated_df
    """

    def test_combine_outputs(self):
        """
        Tests functionality of combine outputs.
        """
        # Arrange
        test_df_1_columns = ["LSAO_code", "LSAO_name", "2010", "Adjust", "year"]

        test_df_1_values = [["E0100", "Barnet", -0.087, True, "[2010]"],
                            ["E0101", "Camden", 200, False, ""],
                            ["E0102", "Hackney", -300.12, True, "[2010]"]]

        test_1_df = pd.DataFrame(test_df_1_values, columns=test_df_1_columns)

        test_df_2_columns = ["LSAO_code", "LSAO_name", "2010", "Adjust", "year"]

        test_df_2_values = [["E0103", "Haringey", -0892.0, True, "[2010]"],
                            ["E0104", "Islington", 0, False, ""],
                            ["E0105", "Kensington", -300.12, True, "[2010]"]]

        test_2_df = pd.DataFrame(test_df_2_values, columns=test_df_2_columns)

        expected_cols = ["LSAO_code", "LSAO_name", "2010", "Adjust", "year"]

        expected_data = [["E0100", "Barnet", -0.087, True, "[2010]"],
                         ["E0101", "Camden", 200, False, ""],
                         ["E0102", "Hackney", -300.12, True, "[2010]"],
                         ["E0103", "Haringey", -0892.0, True, "[2010]"],
                         ["E0104", "Islington", 0, False, ""],
                         ["E0105", "Kensington", -300.12, True, "[2010]"]]

        expected_df = pd.DataFrame(expected_data, columns=expected_cols)

        # Act
        result_df = combine_outputs([test_1_df, test_2_df])

        # assert
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_drop_duplicate_LSOA_codes(self):
        """Tests functionality of drop_duplicate_LSOA_codes."""
        # Arrange

        # Act

        # Assert
