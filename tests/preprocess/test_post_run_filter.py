import pandas as pd

from gdhi_adj.preprocess.post_run_filter import apply_post_run_filter


class TestApplyPostRunFilter:
    """Tests functions within apply_post_run_filter module.

    * apply_post_run_filter
    """

    def test_apply_post_run_filter(self):
        """Test apply_post_run_filter removes previously examined LSOAs."""
        # Arrange
        test_prev_cols = ["lsoa_code", "lsoa_name", "Adjust", "Year"]

        test_prev_data = [["E0102", "Harrogate", False, 2019],
                          ["E0103", "Leeds", True, 2020],
                          ["E0104", "Bradford", False, 2021],
                          ["E0105", "York", False, 2022],
                          ["E0106", "Sheffield", False, 2023],
                          ["E0107", "Hull", True, 2024]]

        test_prev_df = pd.DataFrame(test_prev_data, columns=test_prev_cols)

        test_preprocess_cols = ["lsoa_code", "lsoa_name", "master_flag"]

        test_preprocess_data = [["E0102", "Harrogate", "TRUE"],
                                ["E0103", "Leeds", "MEAN"],
                                ["E0104", "Bradford", "TRUE"],
                                ["E0107", "Hull", "MEAN"],
                                ["E0108", "Wakefield", "TRUE"],
                                ["E0109", "Doncaster", "MEAN"],
                                ["E0110", "Rotherham", "TRUE"],
                                ["E0111", "Barnsley", "MEAN"]]
        test_preprocess_data = pd.DataFrame(test_preprocess_data, columns=test_preprocess_cols)

        expected_data = [["E0102", "Harrogate", "TRUE"],
                         ["E0103", "Leeds", "MEAN"],

                         ["E0105", "York", "MEAN"],
                         ["E0106", "Sheffield", "TRUE"],
                         ["E0107", "Hull", "MEAN"],
                         ["E0108", "Wakefield", "TRUE"],
                         ["E0109", "Doncaster", "MEAN"],
                         ["E0110", "Rotherham", "TRUE"],
                         ["E0111", "Barnsley", "MEAN"]]

        expected_df = pd.DataFrame(expected_data, columns=test_preprocess_cols)

        # Act
        result_df = apply_post_run_filter(test_prev_df, test_preprocess_data)

        # Assert
        pd.testing.assert_frame_equal(result_df.reset_index(drop=True),
                                      expected_df.reset_index(drop=True))
