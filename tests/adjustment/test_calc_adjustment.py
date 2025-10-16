import pandas as pd

from gdhi_adj.adjustment.calc_adjustment import (
    apply_adjustment,
    calc_adjustment_headroom_val,
    calc_adjustment_val,
    calc_midpoint_val,
    calc_scaling_factors,
)


class TestCalcScalingFactors:
    def test_calc_scaling_factors_normal(self):
        """Test the calc_scaling_factors function."""
        df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E3", "E1", "E2", "E3"],
            "lad_code": ["E01", "E01", "E01", "E01", "E01", "E01"],
            "year": [2002, 2002, 2002, 2003, 2003, 2003],
            "uncon_gdhi": [10.0, 15.0, 25.0, 30.0, 50.0, 40.0],
            "con_gdhi": [30.0, 40.0, 10.0, 10.0, 20.0, 30.0]
        })

        result_df = calc_scaling_factors(df)

        expected_df = pd.DataFrame({
            "year": [2002, 2003],
            "uncon_gdhi": [50.0, 120.0],
            "con_gdhi": [80.0, 60.0],
            "scaling": [1.6, 0.5]
        })

        pd.testing.assert_frame_equal(result_df, expected_df)

    def test_calc_scaling_factors_div_zero(self):
        """Test the calc_scaling_factors when denominator is 0."""
        df = pd.DataFrame({
            "lsoa_code": ["E1", "E2", "E3", "E1", "E2", "E3"],
            "lad_code": ["E01", "E01", "E01", "E01", "E01", "E01"],
            "year": [2002, 2002, 2002, 2003, 2003, 2003],
            "uncon_gdhi": [10.0, 15.0, 25.0, 0.0, 0.0, 0.0],
            "con_gdhi": [30.0, 40.0, 10.0, 10.0, 20.0, 30.0]
        })

        result_df = calc_scaling_factors(df)

        expected_df = pd.DataFrame({
            "year": [2002, 2003],
            "uncon_gdhi": [50.0, 0.0],
            "con_gdhi": [80.0, 60.0],
            "scaling": [1.6, 0.0]
        })

        pd.testing.assert_frame_equal(result_df, expected_df)


def test_calc_adjustment_headroom_val():
    """Test the calc_adjustment_headroom_val function."""
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E1", "E2", "E2", "E3", "E3"],
        "year": [2002, 2003, 2002, 2003, 2002, 2003],
        "uncon_gdhi": [10.0, 11.0, 12.0, 13.0, 19.0, 17.0],
        "con_gdhi": [5.0, 15.0, 25.0, 20.0, 10.0, 12.0]
    })

    df_scaling = pd.DataFrame({
        "year": [2002, 2003, 2004, 2005],
        "uncon_gdhi": [10.0, 20.0, 30.0, 40.0],
        "con_gdhi": [50.0, 66.0, 70.0, 80.0],
        "scaling": [2.0, 10.0, 1.0, 3.0]
    })

    lsoa_code = "E1"
    year_to_adjust = 2003

    result_uncon_sum, result_headroom_val = calc_adjustment_headroom_val(
        df, df_scaling, lsoa_code, year_to_adjust
    )

    expected_uncon_sum = 30.0
    expected_headroom_val = 6.0

    assert result_uncon_sum == expected_uncon_sum
    assert result_headroom_val == expected_headroom_val


def test_calc_midpoint_val():
    """Test the calc_midpoint_val function."""
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E1", "E1", "E2"],
        "year": [2002, 2003, 2004, 2003],
        "uncon_gdhi": [10.0, 20.0, 26.0, 45.0],
        "con_gdhi": [5.0, 8.0, 10.0, 15.0]
    })

    lsoa_code = "E1"
    year_to_adjust = 2003

    result_outlier_val, result_midpoint_val = calc_midpoint_val(
        df, lsoa_code, year_to_adjust
    )

    expected_outlier_val = 8.0
    expected_midpoint_val = 7.5

    assert result_outlier_val == expected_outlier_val
    assert result_midpoint_val == expected_midpoint_val


def test_calc_adjustment_val():
    """Test the calc_adjustment_val function."""
    headroom_val = 15.0
    outlier_val = 7.5
    midpoint_val = -8.0

    result_adjustment_val = calc_adjustment_val(
        headroom_val, outlier_val, midpoint_val
    )

    expected_adjustment_val = 7.5

    assert result_adjustment_val == expected_adjustment_val

    headroom_val_high = 30.0

    result_adjustment_val_high_head = calc_adjustment_val(
        headroom_val_high, outlier_val, midpoint_val
    )

    expected_adjustment_val_high_head = -15.5

    assert result_adjustment_val_high_head == expected_adjustment_val_high_head


def test_apply_adjustment():
    """Test the apply_adjustment function."""
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E3", "E4"],
        "adjust": [True, float("NaN"), float("NaN"), float("NaN")],
        "year": [2002, 2002, 2002, 2003],
        "uncon_gdhi": [10.0, 20.0, 30.0, 40.0],
        "con_gdhi": [5.0, 15.0, 12.0, 20.0]
    })

    year_to_adjust = 2002
    adjustment_val = 7.5
    uncon_non_out_sum = 30.0

    result_df = apply_adjustment(
        df, year_to_adjust, adjustment_val, uncon_non_out_sum
    )

    expected_df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E3", "E4"],
        "adjust": [True, float("NaN"), float("NaN"), float("NaN")],
        "year": [2002, 2002, 2002, 2003],
        "uncon_gdhi": [10.0, 20.0, 30.0, 40.0],
        "con_gdhi": [12.5, 20.0, 19.5, 20.0]
    })

    pd.testing.assert_frame_equal(result_df, expected_df)
