import pandas as pd

from gdhi_adj.adjustment.calc_adjustment import (
    calc_imputed_adjustment,
    calc_lad_totals,
    extrapolate_imputed_val,
    interpolate_imputed_val,
)


def test_calc_lad_totals_aggregates_sum_per_lad_and_year():
    """Test calc_lad_totals correctly aggregates con_gdhi sums per
    (lad_code, year) group."""
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E1", "E3"],
        "lad_code": ["E01", "E01", "E01", "E02"],
        "year": [2000, 2000, 2001, 2000],
        "con_gdhi": [100.0, 50.0, 30.0, 20.0],
    })

    result_df = calc_lad_totals(df)

    expected_df = pd.DataFrame({
        "lad_code": ["E01", "E01", "E02"],
        "year": [2000, 2001, 2000],
        "con_gdhi": [150.0, 30.0, 20.0],
    })

    pd.testing.assert_frame_equal(result_df, expected_df)


class TestInterpolateImputedVal:
    """Tests for the interpolate_imputed_val function."""
    def test_interpolate_imputed_val(self):
        """Test the interpolate_imputed_val function returns the expected
        imputed values.
        """
        df = pd.DataFrame({
            "lsoa_code": ["E1", "E1"],
            "year": [2001, 2002],
            "con_gdhi": [12.0, 13.0],
            "year_to_adjust": [[2001, 2002], [2001, 2002]],
            "prev_safe_year": [2000, 2000],
            "prev_con_gdhi": [10.0, 10.0],
            "next_safe_year": [2003, 2003],
            "next_con_gdhi": [40.0, 40.0],
        })

        result_df = interpolate_imputed_val(df)

        expected_df = pd.DataFrame({
            "lsoa_code": ["E1", "E1"],
            "year": [2001, 2002],
            "con_gdhi": [12.0, 13.0],
            "year_to_adjust": [[2001, 2002], [2001, 2002]],
            "prev_safe_year": [2000, 2000],
            "prev_con_gdhi": [10.0, 10.0],
            "next_safe_year": [2003, 2003],
            "next_con_gdhi": [40.0, 40.0],
            "imputed_gdhi": [20.0, 30.0],
        })

        pd.testing.assert_frame_equal(
            result_df, expected_df, check_names=False
        )


class TestExtrapolateImputedVal:
    """Tests for the extrapolate_imputed_val function."""
    def test_extrapolate_imputed_val(self):
        """Test the extrapolate_imputed_val function returns the expected
        imputed values.
        """
        df = pd.DataFrame({
            "lsoa_code": ["E1", "E1"],
            "year": [2000, 2001],
            "con_gdhi": [35.0, 32.0],
            "year_to_adjust": [[2000, 2001], [2000, 2001]],
            "prev_safe_year": [1999, 1999],
            "prev_con_gdhi": [None, None],
            "next_safe_year": [2002, 2002],
            "next_con_gdhi": [24.0, 24.0],
        })

        result_df = extrapolate_imputed_val(df)

        expected_df = pd.DataFrame({
            "lsoa_code": ["E1", "E1"],
            "year": [2000, 2001],
            "con_gdhi": [35.0, 32.0],
            "year_to_adjust": [[2000, 2001], [2000, 2001]],
            "prev_safe_year": [1999, 1999],
            "prev_con_gdhi": [None, None],
            "next_safe_year": [2002, 2002],
            "next_con_gdhi": [24.0, 24.0],
            "imputed_gdhi": [17.0, 21.0],
        })

        pd.testing.assert_frame_equal(
            result_df, expected_df, check_names=False
        )


# class TestCalcImputedVal:
#     """Tests for the calc_imputed_val function."""
#     def test_calc_imputed_val_interpolation(self):
#         """Test the calc_imputed_val function returns the expected imputed
#         values when values are within the year range.
#         """
#         df = pd.DataFrame({
#             "lsoa_code": ["E1", "E1", "E1", "E1"],
#             "year": [2002, 2003, 2004, 2005],
#             "con_gdhi": [10.0, 8.0, 10.0, 16.0],
#             "year_to_adjust": [
#                 [2003, 2004], [2003, 2004], [2003, 2004], [2003, 2004]
#             ],
#         })

#         result_df = calc_imputed_val(df)

#         expected_df = pd.DataFrame({
#             "lsoa_code": ["E1", "E1"],
#             "year": [2003, 2004],
#             "con_gdhi": [8.0, 10.0],
#             "year_to_adjust": [[2003, 2004], [2003, 2004]],
#             "prev_safe_year": [2002, 2002],
#             "prev_con_gdhi": [10.0, 10.0],
#             "next_safe_year": [2005, 2005],
#             "next_con_gdhi": [16.0, 16.0],
#             "imputed_gdhi": [12.0, 14.0],
#         })

#         pd.testing.assert_frame_equal(
#             result_df, expected_df, check_names=False
#         )

#     def test_calc_imputed_val_extrapolation(self):
#         """Test the calc_imputed_val function returns the expected imputed
#         values when values are at the end of the year range.
#         """
#         df = pd.DataFrame({
#             "lsoa_code": ["E1", "E1", "E1", "E1", "E1", "E1", "E1"],
#             "year": [2002, 2003, 2004, 2005, 2006, 2007, 2008],
#             "con_gdhi": [35.0, 32.0, 24.0, 26.0, 30.0, 28.0, 36.0],
#             "year_to_adjust": [
#                 [2002, 2003], [2002, 2003], [2002, 2003], [2002, 2003],
#                 [2002, 2003], [2002, 2003], [2002, 2003],
#             ],
#         })

#         result_df = calc_imputed_val(df)

#         expected_df = pd.DataFrame({
#             "lsoa_code": ["E1", "E1"],
#             "year": [2002, 2003],
#             "con_gdhi": [35.0, 32.0],
#             "year_to_adjust": [[2002, 2003], [2002, 2003]],
#             "prev_safe_year": [2001, 2001],
#             "prev_con_gdhi": [np.nan, np.nan],
#             "next_safe_year": [2004, 2004],
#             "next_con_gdhi": [24.0, 24.0],
#             "imputed_gdhi": [18.0, 21.0],
#         })

#         pd.testing.assert_frame_equal(
#             result_df, expected_df, check_names=False
#         )


def test_calc_imputed_adjustment():
    """Test calc_imputed_adjustment computes imputed_diff and apportions
    the summed adjustment_val across all rows for the same LSOA.
    """
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E3", "E1"],
        "lad_code": ["E01", "E01", "E01", "E01"],
        "year": [2002, 2002, 2002, 2003],
        "con_gdhi": [5.0, 8.0, 10.0, 15.0],
    })

    # imputed_df contains only the outlier row(s) with their computed
    # imputed_gdhi
    imputed_df = pd.DataFrame({
        "lsoa_code": ["E2", "E3"],
        "year": [2002, 2002],
        "con_gdhi": [8.0, 10.0],
        "imputed_gdhi": [7.5, 11.0],
    })

    result_df = calc_imputed_adjustment(df, imputed_df)

    expected_df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E3", "E1"],
        "lad_code": ["E01", "E01", "E01", "E01"],
        "year": [2002, 2002, 2002, 2003],
        "con_gdhi": [5.0, 8.0, 10.0, 15.0],
        "imputed_gdhi": [None, 7.5, 11.0, None],
        "imputed_diff": [None, 0.5, -1.0, None],
        # group sum of imputed_diff for E01 2002 is -0.5; for E01 2003 (all
        # NaN) pandas produces 0.0 when summing; transform('sum') therefore
        # gives 0.5 for 2002 rows and 0.0 for 2003 rows.
        "adjustment_val": [-0.5, -0.5, -0.5, 0.0],
    })

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)
