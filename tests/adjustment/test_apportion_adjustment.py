import pandas as pd
import pytest

from gdhi_adj.adjustment.apportion_adjustment import (
    apportion_adjustment,
    apportion_negative_adjustment,
    apportion_rollback_years,
    calc_non_outlier_proportions,
    check_no_negative_values_col,
)


class TestCalcNoneOutlierProportions():
    """Test suite for calc_non_outlier_proportions function."""
    def test_calc_non_outlier_proportions_success(self):
        """Tests for the calc_non_outlier_proportions function."""
        df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E1", "E2", "E1", "E2",],
            "lad_code": ["E01", "E01", "E01", "E01", "E01", "E01"],
            "year": [2000, 2000, 2001, 2001, 2002, 2002],
            "con_gdhi": [3.0, 9.0, 8.0, 12.0, 10.0, 15.0],
            "year_to_adjust": [[2001], [], [2001], [], [2001], []],
        })

        result_df = calc_non_outlier_proportions(df)

        expected_df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E1", "E2", "E1", "E2",],
            "lad_code": ["E01", "E01", "E01", "E01", "E01", "E01"],
            "year": [2000, 2000, 2001, 2001, 2002, 2002],
            "con_gdhi": [3.0, 9.0, 8.0, 12.0, 10.0, 15.0],
            "year_to_adjust": [[2001], [], [2001], [], [2001], []],
            "lad_total": [12.0, 12.0, 20.0, 20.0, 25.0, 25.0],
            "non_outlier_total": [12.0, 12.0, None, 12.0, 25.0, 25.0],
            "gdhi_proportion": [0.25, 0.75, None, 1.0, 0.4, 0.6],
        })

        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_calc_non_outlier_proportions_zero_error(self):
        """Tests for the calc_non_outlier_proportions function."""
        df = pd.DataFrame({
            "lsoa_code": ["E1", "E2"],
            "lad_code": ["E01", "E01"],
            "year": [2001, 2001],
            "con_gdhi": [8.0, 0.0],
            "year_to_adjust": [[2001], []],
        })

        with pytest.raises(
            ValueError,
            match="Non-outlier total check failed:"
        ):
            calc_non_outlier_proportions(df)


class TestApportionAdjustment:
    """Test suite for apportion_adjustment function."""
    def test_apportion_adjustment_success(self):
        """Test apportion_adjustment computes year_count and adjusted_con_gdhi
        correctly and returns the full dataframe sorted.
        """
        df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E1", "E2"],
            "lad_code": ["E01", "E01", "E01", "E01"],
            "year": [2000, 2000, 2001, 2001],
            "con_gdhi": [3.0, 9.0, 8.0, 12.0],
            "lad_total": [12.0, 12.0, 20.0, 20.0],
            "gdhi_proportion": [0.25, 0.75, None, 1.0],
        })

        imputed_df = pd.DataFrame({
            "lsoa_code": ["E1"],
            "year": [2001],
            "con_gdhi": [12.0],
            "year_to_adjust": [[2001]],
            "rollback_flag": [True],
            "prev_safe_year": [2000],
            "prev_con_gdhi": [10.0],
            "next_safe_year": [2003],
            "next_con_gdhi": [40.0],
            "imputed_gdhi": [5.0],
        })

        result_df = apportion_adjustment(df, imputed_df)

        expected_df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E1", "E2"],
            "lad_code": ["E01", "E01", "E01", "E01"],
            "year": [2000, 2000, 2001, 2001],
            "con_gdhi": [3.0, 9.0, 8.0, 12.0],
            "lad_total": [12.0, 12.0, 20.0, 20.0],
            "gdhi_proportion": [0.25, 0.75, None, 1.0],
            "imputed_gdhi": [None, None, 5.0, None],
            "adjusted_total": [12.0, 12.0, 15.0, 15.0],
            "adjusted_con_gdhi": [3.0, 9.0, 5.0, 15.0],
        })

        pd.testing.assert_frame_equal(
            result_df, expected_df, check_dtype=False
        )


class TestApportionNegativeAdjustment:
    """Test suite for apportion_negative_adjustment function."""
    def test_apportion_negative_adjustment_negatives(self):
        """Test apportion_negative_adjustment computes adjusted_con_gdhi
        correctly when adjusted_con_gdhi is negative for a given (lad_code,
        year) group.
        """

        df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E3", "E4", "E1", "E2", "E3", "E4"],
            "lad_code": [
                "E01", "E01", "E01", "E01", "E01", "E01", "E01", "E01"
            ],
            "year": [2000, 2000, 2000, 2000, 2001, 2001, 2001, 2001],
            "con_gdhi": [1.0, 2.0, 3.0, 4.0, 2.0, 3.0, 4.0, 5.0],
            "lad_total": [10.0, 10.0, 10.0, 10.0, 14.0, 14.0, 14.0, 14.0],
            "adjusted_con_gdhi": [-0.5, 5.5, -1.0, 6.0, 3.0, 1.0, 4.0, 6.0],
        })

        result_df = apportion_negative_adjustment(df)

        expected_df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E3", "E4", "E1", "E2", "E3", "E4"],
            "lad_code": [
                "E01", "E01", "E01", "E01", "E01", "E01", "E01", "E01"
            ],
            "year": [2000, 2000, 2000, 2000, 2001, 2001, 2001, 2001],
            "con_gdhi": [1.0, 2.0, 3.0, 4.0, 2.0, 3.0, 4.0, 5.0],
            "lad_total": [10.0, 10.0, 10.0, 10.0, 14.0, 14.0, 14.0, 14.0],
            "previously_adjusted_con_gdhi": [
                -0.5, 5.5, -1.0, 6.0, 3.0, 1.0, 4.0, 6.0
            ],
            "no_neg_adjusted_gdhi": [0.0, 5.5, 0.0, 6.0, 3.0, 1.0, 4.0, 6.0],
            "sum_neg_adjusted_gdhi": [
                -1.5, -1.5, -1.5, -1.5, 0.0, 0.0, 0.0, 0.0
            ],
            "adjusted_gdhi_proportion": [
                0.0, 0.4783, 0.0, 0.5217, 0.2143, 0.0714, 0.2857, 0.4286
            ],
            "adjusted_con_gdhi": [
                0.0, 4.7826, 0.0, 5.2174, 3.0, 1.0, 4.0, 6.0
            ],
        })

        pd.testing.assert_frame_equal(
            result_df, expected_df, check_dtype=False, rtol=0.001,
        )

    def test_apportion_negative_adjustment_no_negatives(self):
        """Test apportion_negative_adjustment returns the correct adjusted
        values.
        """

        df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E3", "E4", "E1", "E2", "E3", "E4"],
            "lad_code": [
                "E01", "E01", "E01", "E01", "E01", "E01", "E01", "E01"
            ],
            "year": [2000, 2000, 2000, 2000, 2001, 2001, 2001, 2001],
            "con_gdhi": [1.0, 2.0, 3.0, 4.0, 2.0, 3.0, 4.0, 5.0],
            "lad_total": [10.0, 10.0, 10.0, 10.0, 14.0, 14.0, 14.0, 14.0],
            "adjusted_con_gdhi": [0.5, 3.5, 1.0, 5.0, 3.0, 1.0, 4.0, 6.0],
        })

        result_df = apportion_negative_adjustment(df)

        expected_df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E3", "E4", "E1", "E2", "E3", "E4"],
            "lad_code": [
                "E01", "E01", "E01", "E01", "E01", "E01", "E01", "E01"
            ],
            "year": [2000, 2000, 2000, 2000, 2001, 2001, 2001, 2001],
            "con_gdhi": [1.0, 2.0, 3.0, 4.0, 2.0, 3.0, 4.0, 5.0],
            "lad_total": [10.0, 10.0, 10.0, 10.0, 14.0, 14.0, 14.0, 14.0],
            "previously_adjusted_con_gdhi": [
                0.5, 3.5, 1.0, 5.0, 3.0, 1.0, 4.0, 6.0
            ],
            "no_neg_adjusted_gdhi": [0.5, 3.5, 1.0, 5.0, 3.0, 1.0, 4.0, 6.0],
            "sum_neg_adjusted_gdhi": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "adjusted_gdhi_proportion": [
                0.05, 0.35, 0.1, 0.5, 0.2143, 0.0714, 0.2857, 0.4286
            ],
            "adjusted_con_gdhi": [0.5, 3.5, 1.0, 5.0, 3.0, 1.0, 4.0, 6.0],
        })

        pd.testing.assert_frame_equal(
            result_df, expected_df, check_dtype=False, rtol=0.001,
        )


class TestCheckNoNegativeValuesCol:
    """Test suite for check_no_negative_values_col function."""
    def test_check_no_negative_adjusted_gdhi_raises(self):
        """Test that check_no_negative_values_col raises ValueError for
        negatives."""
        df = pd.DataFrame({"adjusted_con_gdhi": [1.0, -1.0]})

        with pytest.raises(ValueError, match="Negative value check failed"):
            check_no_negative_values_col(df, "adjusted_con_gdhi")

    def test_check_no_negative_adjusted_gdhi_passes(self):
        """Test that check_no_negative_values_col passes for no negatives."""
        df = pd.DataFrame({"adjusted_con_gdhi": [1.0, 2.0]})

        check_no_negative_values_col(df, "adjusted_con_gdhi")


class TestApportionRollbackYears:
    """Test suite for apportion_rollback_years function."""

    def test_apportion_rollback_years_basic(self):
        """Test apportion_rollback_years correctly calculates rollback_con_gdhi
        for rollback_flag rows.
        """
        df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E1", "E2", "E1", "E2", "E1", "E2"],
            "lad_code": [
                "E01", "E01", "E01", "E01", "E01", "E01", "E01", "E01"
            ],
            "adjust": [True, False, True, False, True, False, True, False],
            "year_to_adjust": [
                [2014], [2014], [2014], [2014], [2014], [2014], [2014], [2014],
            ],
            "year": [2012, 2012, 2013, 2013, 2014, 2014, 2015, 2015],
            "con_gdhi": [5.0, 15.0, 15.0, 15.0, 16.0, 24.0, 15.0, 35.0],
            "lad_total": [20.0, 20.0, 30.0, 30.0, 40.0, 40.0, 50.0, 50.0],
            "adjusted_con_gdhi": [
                6.0, 14.0, 14.0, 16.0, 17.0, 23.0, 16.0, 34.0
            ],
            "rollback_flag": [
                True, False, True, False, True, False, False, False
            ],
        })

        result_df = apportion_rollback_years(df)

        expected_df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E1", "E2", "E1", "E2", "E1", "E2"],
            "lad_code": [
                "E01", "E01", "E01", "E01", "E01", "E01", "E01", "E01"
            ],
            "adjust": [True, False, True, False, True, False, True, False],
            "year_to_adjust": [
                [2014], [2014], [2014], [2014], [2014], [2014], [2014], [2014],
            ],
            "year": [2012, 2012, 2013, 2013, 2014, 2014, 2015, 2015],
            "con_gdhi": [5.0, 15.0, 15.0, 15.0, 16.0, 24.0, 15.0, 35.0],
            "lad_total": [20.0, 20.0, 30.0, 30.0, 40.0, 40.0, 50.0, 50.0],
            "adjusted_con_gdhi": [
                6.0, 14.0, 14.0, 16.0, 17.0, 23.0, 16.0, 34.0
            ],
            "rollback_flag": [
                True, False, True, False, True, False, False, False
            ],
            "adjusted_rollback_flag": [
                True, False, True, False, True, False, False, False
            ],
            "rollback_adjust_flag": [
                True, True, True, True, True, True, False, False
            ],
            "rollback_con_gdhi": [
                8.5, 11.5, 12.75, 17.25, 17.0, 23.0, 16.0, 34.0
            ],
        })

        pd.testing.assert_frame_equal(
            result_df, expected_df, rtol=0.001
        )

    def test_apportion_rollback_years_no_rollback(self):
        """Test apportion_rollback_years returns unchanged adjusted_con_gdhi
        when rollback_flag is False for all rows.
        """
        df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E1", "E2", "E1", "E2", "E1", "E2"],
            "lad_code": [
                "E01", "E01", "E01", "E01", "E01", "E01", "E01", "E01"
            ],
            "adjust": [True, True, True, True, True, True, False, False],
            "year_to_adjust": [
                [2014], [2014], [2014], [2014], [2014], [2014], [2014], [2014],
            ],
            "year": [2012, 2012, 2013, 2013, 2014, 2014, 2015, 2015],
            "con_gdhi": [5.0, 15.0, 15.0, 15.0, 16.0, 24.0, 15.0, 35.0],
            "lad_total": [20.0, 20.0, 30.0, 30.0, 40.0, 40.0, 50.0, 50.0],
            "adjusted_con_gdhi": [
                6.0, 14.0, 14.0, 16.0, 17.0, 23.0, 16.0, 34.0
            ],
            "rollback_flag": [
                False, False, False, False, False, False, False, False
            ],
        })

        result_df = apportion_rollback_years(df)

        expected_df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E1", "E2", "E1", "E2", "E1", "E2"],
            "lad_code": [
                "E01", "E01", "E01", "E01", "E01", "E01", "E01", "E01"
            ],
            "adjust": [True, True, True, True, True, True, False, False],
            "year_to_adjust": [
                [2014], [2014], [2014], [2014], [2014], [2014], [2014], [2014],
            ],
            "year": [2012, 2012, 2013, 2013, 2014, 2014, 2015, 2015],
            "con_gdhi": [5.0, 15.0, 15.0, 15.0, 16.0, 24.0, 15.0, 35.0],
            "lad_total": [20.0, 20.0, 30.0, 30.0, 40.0, 40.0, 50.0, 50.0],
            "adjusted_con_gdhi": [
                6.0, 14.0, 14.0, 16.0, 17.0, 23.0, 16.0, 34.0
            ],
            "rollback_flag": [
                False, False, False, False, False, False, False, False
            ],
            "adjusted_rollback_flag": [
                False, False, False, False, False, False, False, False
            ],
            "rollback_adjust_flag": [
                False, False, False, False, False, False, False, False
            ],
            "rollback_con_gdhi": [
                6.0, 14.0, 14.0, 16.0, 17.0, 23.0, 16.0, 34.0
            ],
        })

        pd.testing.assert_frame_equal(
            result_df, expected_df, rtol=0.001
        )

    def test_apportion_rollback_years_multi_lad(self):
        """Test apportion_rollback_years correctly calculates rollback_con_gdhi
        for rollback_flag rows for multiple LAD codes, both LADs have 2014
        flagged for adjustment, but only one LSOA in one LAD contains rollback
        years.
        """
        df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E1", "E2", "E1", "E2",
                          "E3", "E4", "E3", "E4", "E3", "E4"],
            "lad_code": ["E01", "E01", "E01", "E01", "E01", "E01",
                         "E02", "E02", "E02", "E02", "E02", "E02",
                         ],
            "adjust": [True, False, True, False, True, False,
                       False, False, False, False, False, False],
            "year_to_adjust": [
                [2014], [2014], [2014], [2014], [2014], [2014],
                [2014], [2014], [2014], [2014], [2014], [2014],
            ],
            "year": [2013, 2013, 2014, 2014, 2015, 2015,
                     2013, 2013, 2014, 2014, 2015, 2015,],
            "con_gdhi": [5.0, 15.0, 15.0, 15.0, 16.0, 24.0,
                         1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            "lad_total": [20.0, 20.0, 30.0, 30.0, 40.0, 40.0,
                          10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
            "adjusted_con_gdhi": [6.0, 14.0, 14.0, 16.0, 17.0, 23.0,
                                  1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            "rollback_flag": [True, False, True, False, False, False,
                              False, False, False, False, False, False],
        })

        result_df = apportion_rollback_years(df)

        expected_df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E1", "E2", "E1", "E2",
                          "E3", "E4", "E3", "E4", "E3", "E4"],
            "lad_code": ["E01", "E01", "E01", "E01", "E01", "E01",
                         "E02", "E02", "E02", "E02", "E02", "E02",
                         ],
            "adjust": [True, False, True, False, True, False,
                       False, False, False, False, False, False],
            "year_to_adjust": [
                [2014], [2014], [2014], [2014], [2014], [2014],
                [2014], [2014], [2014], [2014], [2014], [2014],
            ],
            "year": [2013, 2013, 2014, 2014, 2015, 2015,
                     2013, 2013, 2014, 2014, 2015, 2015,],
            "con_gdhi": [5.0, 15.0, 15.0, 15.0, 16.0, 24.0,
                         1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            "lad_total": [20.0, 20.0, 30.0, 30.0, 40.0, 40.0,
                          10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
            "adjusted_con_gdhi": [6.0, 14.0, 14.0, 16.0, 17.0, 23.0,
                                  1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            "rollback_flag": [True, False, True, False, False, False,
                              False, False, False, False, False, False],
            "adjusted_rollback_flag": [
                True, False, True, False, False, False,
                False, False, False, False, False, False,
            ],
            "rollback_adjust_flag": [
                True, True, True, True, False, False,
                False, False, False, False, False, False,
            ],
            "rollback_con_gdhi": [
                9.333, 10.667, 14.0, 16.0, 17.0, 23.0,
                1.0, 2.0, 3.0, 4.0, 5.0, 6.0,
            ],
        })

        pd.testing.assert_frame_equal(
            result_df, expected_df, rtol=0.001
        )
