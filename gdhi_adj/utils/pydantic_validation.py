"""Pydantic validation of main configuration file."""

import pydantic


class UserSettingsSchema(pydantic.BaseModel):
    """
    Pydantic model for validating the user settings in the configuration file.
    """

    preprocessing: bool
    adjustment: bool
    cord_preparation: bool
    # Common settings
    shared_root_dir: str
    export_qa_files: bool
    output_data: bool
    output_data_prefix: str
    rollback_year_start: int
    rollback_year_end: int
    # Preprocessing settings
    zscore_calculation: bool
    iqr_calculation: bool
    zscore_lower_threshold: float
    zscore_upper_threshold: float
    iqr_lower_quantile: float
    iqr_upper_quantile: float
    iqr_multiplier: float
    transaction_name: str
    # Adjustment settings
    filter_sub_component: bool
    sas_code_filter: str
    cord_code_filter: str
    credit_debit_filter: str
    accept_negatives_adjustment: bool
    # CORD preparation settings
    data_lad_code: str
    data_lad_name: str
    aggregate_to_lad: bool
    accept_negatives_cord: bool

    @pydantic.field_validator("shared_root_dir")
    def validate_shared_root_dir(cls, value):
        if not value.startswith(("~/")):
            raise ValueError(
                "shared_root_dir must start with '~/' to allow "
                "pathlib to expand the user local directory."
            )
        return value

    @pydantic.field_validator("rollback_year_start", "rollback_year_end")
    def validate_rollback_years(cls, value):
        if value < 1900 or value > 2100:
            raise ValueError(
                "rollback_year_start and rollback_year_end must "
                "be between 1900 and 2100"
            )
        return value


class PreprocessingSettingsSchema(pydantic.BaseModel):
    """
    Pydantic model for validating the preprocessing settings in the
    configuration file.
    """

    input_unconstrained_file_path: str
    input_ra_lad_file_path: str
    output_dir: str
    interim_filename: str
    output_filename: str

    @pydantic.field_validator("output_dir")
    def validate_output_dir(cls, value):
        if not value.endswith("/"):
            raise ValueError("output_dir must end with a forward slash.")
        return value

    @pydantic.field_validator("*")
    def validate_file_paths(cls, value, info):
        if info.field_name == "output_dir":
            return value
        if not value.endswith(".csv"):
            raise ValueError("File path values must end with .csv.")
        return value


class AdjustmentSettingsSchema(pydantic.BaseModel):
    """
    Pydantic model for validating the adjustment settings in the
    configuration file.
    """

    input_adj_file_path: str
    input_constrained_file_path: str
    input_unconstrained_file_path: str
    output_dir: str
    interim_filename: str
    output_filename: str

    @pydantic.field_validator("output_dir")
    def validate_output_dir(cls, value):
        if not value.endswith("/"):
            raise ValueError("output_dir must end with a forward slash.")
        return value

    @pydantic.field_validator("*")
    def validate_file_paths(cls, value, info):
        if info.field_name == "output_dir":
            return value
        if not value.endswith(".csv"):
            raise ValueError("File path values must end with .csv.")
        return value


class CordPrepSettingsSchema(pydantic.BaseModel):
    """
    Pydantic model for validating the CORD preparation settings in the
    configuration file.
    """

    input_subcomponent_folder: str
    subcomponent_lookup_file_path: str
    input_lau_lad_mapper_dir: str
    input_lau_lad_mapper_file: str
    output_dir: str
    output_filename: str

    @pydantic.field_validator(
        "subcomponent_lookup_file_path",
        "input_lau_lad_mapper_file",
        "output_filename",
    )
    def validate_file_paths(cls, value):
        if not value.endswith(".csv"):
            raise ValueError("File path values must end with .csv.")
        return value

    @pydantic.field_validator(
        "input_subcomponent_folder", "input_lau_lad_mapper_dir", "output_dir"
    )
    def validate_dir_paths(cls, value):
        if not value.endswith("/"):
            raise ValueError("Directory paths must end with a forward slash.")
        return value


class SchemaPaths(pydantic.BaseModel):
    """
    Pydantic model for validating the schema paths section in the
    configuration file.
    """

    schema_dir: str
    input_gdhi_schema_name: str
    input_ra_lad_schema_name: str
    output_preprocess_schema_path: str
    input_adj_schema_name: str
    input_constrained_schema_name: str
    input_unconstrained_schema_name: str
    output_adjustment_schema_path: str
    input_mapping_lau_lad_schema_name: str
    input_cord_prep_schema_path: str
    output_cord_prep_schema_path: str

    @pydantic.field_validator("schema_dir")
    def validate_schema_dir(cls, value):
        if value != "config/schemas/":
            raise ValueError("schema_dir path must be 'config/schemas/")
        return value

    @pydantic.field_validator("*")
    def validate_schema_file_paths(cls, value, info):
        if info.field_name == "schema_dir":
            return value

        if not value.endswith(".toml"):
            raise ValueError("Toml schema path values must end with .toml")
        return value


class ConfigSchema(pydantic.BaseModel):
    """Pydantic model for validating the input adjustment data schema."""

    user_settings: UserSettingsSchema
    preprocessing_settings: PreprocessingSettingsSchema
    adjustment_settings: AdjustmentSettingsSchema
    cord_prep_settings: CordPrepSettingsSchema
    schema_paths: SchemaPaths
