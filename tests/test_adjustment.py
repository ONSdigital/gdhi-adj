import pandas as pd
import pytest

from gdhi_adj.adjustment import (
    apply_adjustment,
    calc_adjustment_headroom_val,
    calc_adjustment_val,
    calc_midpoint_val,
    calc_scaling_factors,
    create_anomaly_list,
    filter_lsoa_data,
    join_analyst_constrained_data,
    join_analyst_unconstrained_data,
    pivot_adjustment_long,
    pivot_wide_dataframe,
)


def test_filter_lsoa_data():
    """Test the filter_lsoa_data function."""
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E3"],
        "lad_code": ["E01", "E02", "E03"],
        "master_flag": [True, None, True],
        "adjust": [True, None, False],
        "year": [2003, 2004, 2005],
        "2002": [10, 20, 30],
    })
    result_df = filter_lsoa_data(df)

    expected_df = pd.DataFrame({
        "lsoa_code": ["E1"],
        "lad_code": ["E01"],
        "adjust": [True],
        "year": [2003],
    })

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)

    df_mismatch = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E3"],
        "lad_code": ["E01", "E02", "E03"],
        "master_flag": [True, None, True],
        "adjust": [True, False, False],
        "year": [2003, 2004, 2005]
    })

    with pytest.raises(
        expected_exception=ValueError,
        match="Mismatch: master_flag and Adjust column booleans do not match."
    ):
        filter_lsoa_data(df_mismatch)


def test_join_analyst_constrained_data():
    """Test the join_analyst_constrained_data function."""
    df_constrained = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "lad_code": ["E01", "E01"],
        "2002": [10.0, 11.0],
        "2003": [20.0, 21.0]
    })

    df_analyst = pd.DataFrame({
        "lsoa_code": ["E1"],
        "lad_code": ["E01"],
        "adjust": [True],
        "year": [2002]
    })

    result_df = join_analyst_constrained_data(df_constrained, df_analyst)

    expected_df = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "lad_code": ["E01", "E01"],
        "CON_2002": [10.0, 11.0],
        "CON_2003": [20.0, 21.0],
        "adjust": [True, float("NaN")],
        "year": [2002, float("NaN")]
    })

    pd.testing.assert_frame_equal(result_df, expected_df)


def test_join_analyst_constrained_data_adjust_failed_merge():
    """Test the join_analyst_constrained_data function where the analyst data
    fails to join."""
    df_constrained = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "lad_code": ["E01", "E01"],
        "2002": [10.0, 11.0],
        "2003": [20.0, 21.0]
    })

    df_analyst = pd.DataFrame({
        "lsoa_code": ["E1"],
        "lad_code": ["E02"],  # Different lad_code
        "adjust": [True],
        "year": [2002]
    })

    with pytest.raises(
        expected_exception=ValueError,
        match="Number of rows to adjust between analyst and constrained data"
    ):
        join_analyst_constrained_data(df_constrained, df_analyst)


def test_join_analyst_constrained_data_row_increase():
    """Test the join_analyst_constrained_data function where merging the
    analyst data increases the number of rows."""
    df_constrained = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "lad_code": ["E01", "E01"],
        "2002": [10.0, 11.0],
        "2003": [20.0, 21.0]
    })

    df_analyst = pd.DataFrame({
        "lsoa_code": ["E1", "E1"],  # Duplicate entries for E1
        "lad_code": ["E01", "E01"],
        "adjust": [True, True],
        "year": [2002, 2003]
    })

    with pytest.raises(
        expected_exception=ValueError,
        match="Number of rows of constrained data after join has increased."
    ):
        join_analyst_constrained_data(df_constrained, df_analyst)


def test_join_analyst_unconstrained_data():
    """Test the join_analyst_unconstrained_data function."""
    df_unconstrained = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "lad_code": ["E01", "E01"],
        "2002": [10.0, 11.0],
        "2003": [20.0, 21.0]
    })

    df_analyst = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "lad_code": ["E01", "E01"],
        "adjust": [True, float("NaN")],
        "year": [2002, float("NaN")],
        "CON_2002": [10.0, 11.0],
        "CON_2003": [20.0, 21.0]
    })

    result_df = join_analyst_unconstrained_data(df_unconstrained, df_analyst)

    expected_df = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "lad_code": ["E01", "E01"],
        "2002": [10.0, 11.0],
        "2003": [20.0, 21.0],
        "adjust": [True, False],
        "year": [2002, float("NaN")],
        "CON_2002": [10.0, 11.0],
        "CON_2003": [20.0, 21.0]
    })

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)


def test_join_analyst_unconstrained_data_adjust_failed_merge():
    """Test the join_analyst_unconstrained_data function where the analyst data
    fails to join."""
    df_constrained = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "lad_code": ["E01", "E01"],
        "2002": [10.0, 11.0],
        "2003": [20.0, 21.0]
    })

    df_analyst = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "lad_code": ["E02", "E01"],  # Different lad_code
        "adjust": [True, float("NaN")],
        "year": [2002, float("NaN")],
        "CON_2002": [10.0, 11.0],
        "CON_2003": [20.0, 21.0]
    })

    with pytest.raises(
        expected_exception=ValueError,
        match="Number of rows to adjust between analyst and unconstrained data"
    ):
        join_analyst_unconstrained_data(df_constrained, df_analyst)


def test_join_analyst_unconstrained_data_row_increase():
    """Test the join_analyst_unconstrained_data function where merging the
    analyst data increases the number of rows."""
    df_constrained = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "lad_code": ["E01", "E01"],
        "2002": [10.0, 11.0],
        "2003": [20.0, 21.0]
    })

    df_analyst = pd.DataFrame({
        "lsoa_code": ["E1", "E1"],  # Duplicate entries for E1
        "lad_code": ["E01", "E01"],
        "adjust": [True, float("NaN")],
        "year": [2002, float("NaN")],
        "CON_2002": [10.0, 11.0],
        "CON_2003": [20.0, 21.0]
    })

    with pytest.raises(
        expected_exception=ValueError,
        match="Number of rows of unconstrained data after join has increased."
    ):
        join_analyst_unconstrained_data(df_constrained, df_analyst)


def test_pivot_adjustment_long():
    """Test the pivot_adjustment_long function."""
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "lad_code": ["E01", "E01"],
        "2002": [10.0, 11.0],
        "2003": [20.0, 21.0],
        "CON_2002": [30.0, 31.0],
        "CON_2003": [40.0, 41.0],
        "adjust": [True, float("NaN")],
        "year": [2002, float("NaN")]
    })

    result_df = pivot_adjustment_long(df)

    expected_df = pd.DataFrame({
        "lsoa_code": ["E1", "E2", "E1", "E2"],
        "lad_code": ["E01", "E01", "E01", "E01"],
        "adjust": [True, float("NaN"), True, float("NaN")],
        "year_to_adjust": [2002, float("NaN"), 2002, float("NaN")],
        "year": [2002, 2002, 2003, 2003],
        "uncon_gdhi": [10.0, 11.0, 20.0, 21.0],
        "con_gdhi": [30.0, 31.0, 40.0, 41.0]
    })

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)


def test_calc_scaling_factors():
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


def test_create_anomaly_list():
    """Test create_anomaly_list function."""
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E1", "E2", "E3", "E3", "E3"],
        "lad_code": ["E01", "E01", "E01", "E01", "E01", "E01"],
        "adjust": [True, True, False, True, True, True],
        "year_to_adjust": [2002, 2002, float("NaN"), 2002, 2003, 2002],
        "uncon_gdhi": [10, 11, 12, 13, 14, 15]
    })

    result_df = create_anomaly_list(df)

    expected_df = pd.DataFrame({
        "lsoa_code": ["E1", "E3", "E3"],
        "year_to_adjust": [2002, 2002, 2003],
    })

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)


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


def test_pivot_wide_dataframe():
    """Test the pivot_wide_dataframe function."""
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E1", "E1", "E2", "E2", "E2"],
        # Testing lad_code column is dropped during pivot
        "lad_code": ["E01", "E01", "E01", "E01", "E01", "E01"],
        "year": [2002, 2003, 2004, 2002, 2003, 2004],
        "uncon_gdhi": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
        "con_gdhi": [5.0, 15.0, 25.0, 35.0, 45.0, 55.0]
    })

    result_df = pivot_wide_dataframe(df)

    expected_df = pd.DataFrame({
        "lsoa_code": ["E1", "E2"],
        "Adjust_Con_2002": [5.0, 35.0],
        "Adjust_Con_2003": [15.0, 45.0],
        "Adjust_Con_2004": [25.0, 55.0],
    })

    pd.testing.assert_frame_equal(result_df, expected_df)
