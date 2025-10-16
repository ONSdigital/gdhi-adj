import pandas as pd
import pytest

from gdhi_adj.adjustment.filter_adjustment import (
    filter_anomaly_list,
    filter_lsoa_data,
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


def test_filter_anomaly_list():
    """Test filter_anomaly_list function."""
    df = pd.DataFrame({
        "lsoa_code": ["E1", "E1", "E2", "E3", "E3", "E3"],
        "lad_code": ["E01", "E01", "E01", "E01", "E01", "E01"],
        "adjust": [True, True, False, True, True, True],
        "year_to_adjust": [2002, 2002, float("NaN"), 2002, 2003, 2002],
        "uncon_gdhi": [10, 11, 12, 13, 14, 15]
    })

    result_df = filter_anomaly_list(df)

    expected_df = pd.DataFrame({
        "lsoa_code": ["E1", "E3", "E3"],
        "year_to_adjust": [2002, 2002, 2003],
    })

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)
