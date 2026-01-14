# Changelog

All notable changes to this project will be documented in this file.

## [0.0.2] - Unreleased

### Added
- Additional config parameters.
- Filter for selected years and components.
- Formatting of input data for adjustment.
- Test coverage report.
- Apportion over adjusted negative values.
- Added .bat environment setup file.
- Added GitHub pages functionality.
- Added open repository files as per guidance.
- GDHIDAP-58: aggregation from LAU to LAD level.
- GDHIDAP-59: negative value apportionment after adjustment.
- GDHIDAP-61: Set up module for preparation of data for CORD.
- GDHIDAP-65: integrated mapper functions into main pipeline.
- GDHIDAP-70: GitHub open repository security and guidance.
- GDHIDAP-73: Create function to extract range from headings.
- GDHIDAP-74: Add adjustment validation checks.

### Changed
- Adjustment values now determined by interpolation/ extrapolation.
- Vectorised applying adjustment instead of for loop.
- GDHIDAP-60: updated methodology for adjustments.
- GDHIDAP-72: convert adjust str values to lower case before reformat.
- GDHIDAP-75: update adjusted constrained value assignment in runner.
- GDHIDAP-76: Reorganise main config file.
- GDHIDAP-79: Change negative value apportionment to adjust on updated proportions.
- GDHIDAP-80: Update conditions for calculating rollback_con_gdhi within apportion_rollback_years.
- GDHIDAP-81: Integrate mapper module into CORD prep module.

### Deprecated

### Fixed

### Removed
- Scaling factor and headroom adjustment value calculations.

## [0.0.1] - 2025-10-16

### Added
- Full pipeline setup with basic functionality to run a preprocess module
to flag LSOAs to review, and an adjustment module to impute values for LSOAs
marked to be adjusted.
