import pandas as pd
import pytest

from gdhi_adj.adjustment.validation_adjustment import (
    check_adjust_year_not_empty,
    check_lsoas_flagged,
    check_years_flagged,
)


class TestCheckAdjustLSOACount:
    """Test suite for check_lsoas_flagged function."""
    def test_check_lsoas_flagged_pass(self):
        """Test check_lsoas_flagged passes when not all LSOAs are flagged."""
        df = pd.DataFrame({
            "lsoa_code": ["LSOA1", "LSOA2", "LSOA1", "LSOA2"],
            "lad_code": ["LAD1", "LAD1", "LAD1", "LAD1"],
            "year": [2010, 2010, 2011, 2011],
            "adjust": [True, False, False, False],
        })

        # This should pass without raising an error
        result_df = check_lsoas_flagged(df)

        pd.testing.assert_frame_equal(result_df, df)

    def test_check_lsoas_flagged_error(self):
        """Test check_lsoas_flagged errors when all LSOAs are flagged."""
        df = pd.DataFrame({
            "lsoa_code": ["LSOA1", "LSOA2", "LSOA1", "LSOA2"],
            "lad_code": ["LAD1", "LAD1", "LAD1", "LAD1"],
            "year": [2010, 2010, 2011, 2011],
            "adjust": [True, True, False, False],
        })

        with pytest.raises(
            ValueError,
            match="All LSOAs have been marked for adjustment.",
        ):
            check_lsoas_flagged(df)


class TestCheckYearsFlagged:
    """Test suite for check_years_flagged function."""
    def test_check_years_flagged_pass(self):
        """Test check_years_flagged passes when not all LSOAs are flagged."""
        df = pd.DataFrame({
            "lsoa_code": ["LSOA1", "LSOA2", "LSOA1", "LSOA2"],
            "year": [2010, 2010, 2011, 2011],
            "adjust": [True, True, True, True],
            "year_to_adjust": [(2010), (2011), (2010), (2011)],
        })

        # This should pass without raising an error
        result_df = check_years_flagged(df)

        pd.testing.assert_frame_equal(result_df, df)

    def test_check_years_flagged_error(self):
        """Test check_years_flagged errors when all LSOAs are flagged."""
        df = pd.DataFrame({
            "lsoa_code": ["LSOA1", "LSOA2", "LSOA1", "LSOA2"],
            "year": [2010, 2010, 2011, 2011],
            "adjust": [True, True, True, True],
            "year_to_adjust": [
                (2010, 2011), (2010, 2011), (2010, 2011), (2010, 2011)
            ],
        })

        with pytest.raises(
            ValueError,
            match=(
                "The following lsoa_codes have all years marked for "
                "adjustment: LSOA1, LSOA2"),
        ):
            check_years_flagged(df)


class TestCheckAdjustYearNotEmpty:
    """Test suite for check_adjust_year_not_empty function."""
    def test_check_adjust_year_not_empty_pass(self):
        """Test check_adjust_year_not_empty passes when all rows to adjust
        have years specified to adjust."""
        df = pd.DataFrame({
            "lsoa_code": ["LSOA1", "LSOA2", "LSOA1", "LSOA2"],
            "year": [2010, 2010, 2011, 2011],
            "adjust": [True, True, True, True],
            "year_to_adjust": [(2010), (2011), (2010), (2011)],
        })

        # This should pass without raising an error
        result_df = check_adjust_year_not_empty(df)

        pd.testing.assert_frame_equal(result_df, df)

    def test_check_adjust_year_not_empty_error(self):
        """
        Test check_adjust_year_not_empty errors when all LSOAs are flagged.
        """
        df = pd.DataFrame({
            "lsoa_code": ["LSOA1", "LSOA2", "LSOA1", "LSOA2"],
            "year": [2010, 2010, 2011, 2011],
            "adjust": [True, True, True, True],
            "year_to_adjust": [(2010), (), (2010), ()],
        })

        with pytest.raises(
            ValueError,
            match=(
                "The following lsoa_codes are marked for adjustment but have "
                "no years specified: LSOA2"),
        ):
            check_adjust_year_not_empty(df)
