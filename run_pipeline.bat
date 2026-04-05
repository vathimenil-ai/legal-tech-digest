@echo off
cd /d "%~dp0"

:: Activate virtual environment if one exists
if exist "venv\Scripts\activate.bat" (
    echo Activating virtual environment: venv
    call "venv\Scripts\activate.bat"
) else if exist ".venv\Scripts\activate.bat" (
    echo Activating virtual environment: .venv
    call ".venv\Scripts\activate.bat"
) else if exist "env\Scripts\activate.bat" (
    echo Activating virtual environment: env
    call "env\Scripts\activate.bat"
) else (
    echo No virtual environment found - using system Python
)

echo.
python pipeline.py --mode daily
echo.
echo Pipeline exited with code %ERRORLEVEL%
echo.
pause
