@echo off
cd /d "%~dp0"
echo ===================================================
echo  Starting CCDAK Interactive Practice Test Server...
echo ===================================================
python server.py
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo Python server failed to start. Opening index.html directly...
    start "" "index.html"
)
pause
