@echo off
setlocal ENABLEDELAYEDEXPANSION

:: ============================================================
:: MODULE DOCSTRING
:: ============================================================
:: Script Name: setup_gdhi_adj_env.bat
::
:: Purpose:
::   - Configure pip.ini and .condarc for Artifactory access
::   - Validate or create conda environment "gdhi_adj" with Python 3.12+
::   - Install current project in editable mode
::
:: Features:
::   - Dry-run mode: checks what would be done without making changes
::   - Idempotent: skips configuration if already correct
::   - Validates Python version in existing environment
::
:: Usage:
::   setup_gdhi_adj_env.bat [--dry-run]
::
:: Options:
::   --dry-run    Show actions without applying changes
::
:: Example:
::   setup_gdhi_adj_env.bat --dry-run
:: ============================================================

:: Parse arguments
set DRYRUN=0
if "%~1"=="--dry-run" set DRYRUN=1

cd /d "%~dp0"

:: Variables
set "USER_NAME=%USERNAME%"
set "ENV_NAME=gdhi_adj"
set "pipConfigDir=%APPDATA%\pip"
set "pipConfigFile=%pipConfigDir%\pip.ini"
set "condaConfigDir=C:\Users\%USERNAME%"
set "condaConfigFile=%condaConfigDir%\.condarc"

:: Check conda
where conda >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Conda not found. Install Anaconda or Miniconda.
    pause & exit /b 1
)

:: ------------------------------------------------------------
:: Determine whether pip.ini / .condarc need updating
:: ------------------------------------------------------------
if not exist "%pipConfigDir%" (
    if %DRYRUN%==1 (
        echo [DRY-RUN] Would create directory: %pipConfigDir%
    ) else (
        mkdir "%pipConfigDir%"
    )
)

set pipNeedsUpdate=0
if exist "%pipConfigFile%" (
    findstr /C:"onsart-01" "%pipConfigFile%" >nul || set pipNeedsUpdate=1
    findstr /C:"index-url" "%pipConfigFile%" >nul || set pipNeedsUpdate=1
) else (
    set pipNeedsUpdate=1
)

if not exist "%condaConfigDir%" (
    if %DRYRUN%==1 (
        echo [DRY-RUN] Would create directory: %condaConfigDir%
    ) else (
        mkdir "%condaConfigDir%"
    )
)

set condaNeedsUpdate=0
if exist "%condaConfigFile%" (
    findstr /C:"onsart-01" "%condaConfigFile%" >nul || set condaNeedsUpdate=1
    findstr /C:"channel_alias" "%condaConfigFile%" >nul || set condaNeedsUpdate=1
) else (
    set condaNeedsUpdate=1
)

:: ------------------------------------------------------------
:: Prompt for Artifactory ENCRYPTED password only if needed and not dry-run
:: ------------------------------------------------------------
set "password="
if %DRYRUN%==0 (
    if "!pipNeedsUpdate!"=="1"  goto :MaybePromptPwd
    if "!condaNeedsUpdate!"=="1" goto :MaybePromptPwd
    goto :SkipPromptPwd
) else (
    echo [DRY-RUN] Skipping Artifactory password prompt.
    goto :SkipPromptPwd
)

:MaybePromptPwd
call :PromptEncryptedPassword
if errorlevel 1 (
    echo [ERROR] A non-empty Artifactory Encrypted Password is required.
    pause & exit /b 1
)
goto :SkipPromptPwd

:SkipPromptPwd

:: ------------------------------------------------------------
:: Configure pip.ini
:: ------------------------------------------------------------
if !pipNeedsUpdate!==1 (
    if %DRYRUN%==1 (
        echo [DRY-RUN] Would update pip.ini at %pipConfigFile%
    ) else (
        (
        echo [global]
        echo timeout = 60
        echo trusted-host = onsart-01
        echo index-url = https://%USER_NAME%:!password!@onsart-01/artifactory/api/pypi/pypi.python.org/simple
        echo extra-index-url = https://%USER_NAME%:!password!@onsart-01/artifactory/api/pypi/yr-python/simple
        ) > "%pipConfigFile%"
        echo pip.ini updated at %pipConfigFile%.
    )
) else (
    echo pip.ini already configured correctly. Skipping.
)

:: ------------------------------------------------------------
:: Configure .condarc
:: ------------------------------------------------------------
if !condaNeedsUpdate!==1 (
    if %DRYRUN%==1 (
        echo [DRY-RUN] Would update .condarc at %condaConfigFile%
    ) else (
        (
        echo channel_alias: https://%USER_NAME%:!password!@onsart-01/artifactory/Anaconda-virtual/
        echo channels:
        echo  - https://%USER_NAME%:!password!@onsart-01/artifactory/api/conda/Anaconda-virtual
        echo default_channels:
        echo  - https://%USER_NAME%:!password!@onsart-01/artifactory/Anaconda-virtual/
        echo ssl_verify: truststore
        ) > "%condaConfigFile%"
        echo .condarc updated at %condaConfigFile%.
    )
) else (
    echo .condarc already configured correctly. Skipping.
)

:: ============================================================
:: Check if target conda environment exists (EXACT match)
:: ============================================================
call :CheckEnvExists "%ENV_NAME%"
if "!ENV_EXISTS!"=="1" (
    echo Environment "%ENV_NAME%" exists.
) else (
    echo Environment "%ENV_NAME%" does not exist.
)

:: ============================================================
:: Dry-run behavior for env + install
:: ============================================================
if %DRYRUN%==1 (
    if "!ENV_EXISTS!"=="1" (
        echo [DRY-RUN] Would validate Python version in "%ENV_NAME%".
        echo [DRY-RUN] Would run: conda run -n %ENV_NAME% python -m pip install --no-input -e .
    ) else (
        echo [DRY-RUN] Would create conda environment "%ENV_NAME%" with Python 3.12.
        echo [DRY-RUN] Would run: conda run -n %ENV_NAME% python -m pip install --no-input -e .
    )
    goto :Finish
)

:: ============================================================
:: If env exists, validate Python; else create it
:: ============================================================
if "!ENV_EXISTS!"=="1" (
    echo Environment "%ENV_NAME%" exists. Checking Python version...
    for /f "tokens=2 delims= " %%i in ('call conda run -n %ENV_NAME% python --version 2^>nul') do set "PYTHON_VER=%%i"
    if not defined PYTHON_VER (
        echo [WARN] Could not detect Python version in "%ENV_NAME%". Will attempt to recreate.
        goto :CreateEnv
    )
    echo Detected Python version in %ENV_NAME%: !PYTHON_VER!
    for /f "tokens=1,2 delims=." %%a in ("!PYTHON_VER!") do (
        if %%a lss 3 (
            echo Python version is less than 3.12. Needs recreation.
            goto :CreateEnv
        ) else if %%a equ 3 (
            if %%b lss 12 (
                echo Python version is less than 3.12. Needs recreation.
                goto :CreateEnv
            )
        )
    )
    echo Environment is valid.
    goto :InstallPackages
) else (
    goto :CreateEnv
)

:CreateEnv
echo Creating conda environment "%ENV_NAME%" with Python 3.12...
call conda create -y -n %ENV_NAME% python=3.12
if %errorlevel% neq 0 (
    echo Failed to create conda environment.
    pause & exit /b 1
)
goto :InstallPackages

:InstallPackages
echo Installing current project in editable mode...
call conda run --no-capture-output -n %ENV_NAME% python -u -m pip install -e . || (
    echo Error installing project
    pause & exit /b 1
)

echo Installing pre-commit hooks...
call conda run --no-capture-output -n %ENV_NAME% pre-commit install || (
    echo Error installing pre-commit hooks
    pause & exit /b 1
)

echo Project installed successfully.
echo Setup complete.
goto :Finish

:: ============================================================
:: Subroutine: CheckEnvExists
:: Sets ENV_EXISTS=1 if env name matches EXACTLY; else 0
:: ============================================================
:CheckEnvExists
set "TARGET_ENV=%~1"
set "ENV_EXISTS=0"

for /f "tokens=1,2,* delims= " %%A in ('call conda env list 2^>nul') do (
    if NOT "%%A"=="" (
        if NOT "%%A"=="#" (
            set "FIRST=%%A"
            set "SECOND=%%B"
            if "!FIRST!"=="*" (
                if /I "!SECOND!"=="%TARGET_ENV%" set "ENV_EXISTS=1"
            ) else (
                if /I "!FIRST!"=="%TARGET_ENV%" set "ENV_EXISTS=1"
            )
        )
    )
)
exit /b 0

:: ============================================================
:: Subroutine: PromptEncryptedPassword
:: Prompts until a non-empty, non-whitespace Artifactory ENCRYPTED password is entered.
:: Returns ERRORLEVEL 0 if provided, 1 otherwise.
:: ============================================================
:PromptEncryptedPassword
echo.
echo Log in to https://onsart-01/artifactory/webapp/#/home with your Windows login,
echo then go to "Edit Profile" and copy your **Artifactory ENCRYPTED Password**.
echo Do NOT use your normal Windows password.
echo.

:ReadPwdLoop
set "password="
set /p "password=Enter Artifactory ENCRYPTED password for %USERNAME%: "

:: Trim leading/trailing spaces
for /f "tokens=* delims= " %%P in ("!password!") do set "password=%%P"
set "password=!password: =!"

if not defined password (
    echo [WARN] Password cannot be empty. Please paste your ENCRYPTED password.
    goto :ReadPwdLoop
)

exit /b 0

:Finish
echo Press any key to exit...
pause
