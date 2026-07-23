import numpy as np
import pandas as pd

from gdhi_adj.adjustment.flag_adjustment import flag_negative_adjustment, identify_safe_years


class TestFlagAdjustment:
    """
    Tests for functions in the flag_adjustment module.
        * identify_safe_years
        * flag_negative_adjustment
    """
    def test_identify_safe_years_middle(self):
        df = pd.DataFrame({
            "lsoa_code": ["E1", "E1", "E1", "E1", "E2"],
            "year": [2000, 2001, 2002, 2003, 2000],
            "con_gdhi": [10.0, 20.0, 30.0, 40.0, 50.0],
            "year_to_adjust": [
                [2001, 2002], [2001, 2002], [2001, 2002], [2001, 2002], []],
        })

        base_df, result_df = identify_safe_years(
            df, start_year=2000, end_year=2003
        )

        expected_df = pd.DataFrame({
            "lsoa_code": ["E1", "E1"],
            "year": [2001, 2002],
            "con_gdhi": [20.0, 30.0],
            "year_to_adjust": [[2001, 2002], [2001, 2002]],
            "prev_safe_year": [2000, 2000],
            "prev_con_gdhi": [10.0, 10.0],
            "next_safe_year": [2003, 2003],
            "next_con_gdhi": [40.0, 40.0],
        })

        pd.testing.assert_frame_equal(base_df, df)
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_identify_safe_years_end(self):
        df = pd.DataFrame({
            "lsoa_code": ["E1", "E1", "E1", "E1", "E2"],
            "year": [2000, 2001, 2002, 2003, 2000],
            "con_gdhi": [10.0, 20.0, 30.0, 40.0, 50.0],
            "year_to_adjust": [
                [2000, 2001], [2000, 2001], [2000, 2001], [2000, 2001], []
            ],
        })

        base_df, result_df = identify_safe_years(
            df, start_year=2000, end_year=2003
        )

        expected_df = pd.DataFrame({
            "lsoa_code": ["E1", "E1"],
            "year": [2000, 2001],
            "con_gdhi": [10.0, 20.0],
            "year_to_adjust": [[2000, 2001], [2000, 2001]],
            "prev_safe_year": [1999, 1999],
            "prev_con_gdhi": [np.nan, np.nan],
            "next_safe_year": [2002, 2002],
            "next_con_gdhi": [30.0, 30.0],
        })

        pd.testing.assert_frame_equal(base_df, df)
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_flag_negative_adjustment_neg(self):
        """
        Tests that negative values are correctly flagged for adjustment.
        """
        # Arrange
        test_df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "D1", "D1", "W1"],
            "year": [2010, 2010, 2010, 2011, 2010],
            # edge case where negative value is already flagged
            "year_to_adjust": [[], [], [], [2011], []],
            "uncon_gdhi": [10.0, -2, 30.0, -100, 50.0],
            # edge case where negative value is already flagged
            "adjust": [False, False, False, True, False]})

        expected_df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "D1", "D1", "W1"],
            "year": [2010, 2010, 2010, 2011, 2010],
            "year_to_adjust": [[], [2010], [], [2011], []],
            "uncon_gdhi": [10.0, -2, 30.0, -100, 50.0],
            "adjust": [False, True, False, True, False]})

        # Act
        result_df = flag_negative_adjustment(test_df)

        # Assert
        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_flag_negative_adjustment_pos(self):
        """
        Tests that positive values remain unchanged.
        """
        # Arrange
        test_df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "D1", "D1", "W1"],
            "year": [2010, 2010, 2010, 2011, 2010],
            "year_to_adjust": [[], [], [], [], []],
            "uncon_gdhi": [10.0, 0, 30.0, 5, 50.0],
            "adjust": [False, False, False, False, False]})

        expected_df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "D1", "D1", "W1"],
            "year": [2010, 2010, 2010, 2011, 2010],
            "year_to_adjust": [[], [], [], [], []],
            "uncon_gdhi": [10.0, 0, 30.0, 5, 50.0],
            "adjust": [False, False, False, False, False]})

        # Act
        result_df = flag_negative_adjustment(test_df)

        # Assert
        pd.testing.assert_frame_equal(result_df, expected_df)
