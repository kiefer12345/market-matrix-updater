@echo off
rem Double-click to refresh court availability right now, then open the dashboard
cd /d "%~dp0..\.."
if exist ".venv\Scripts\python.exe" (
    ".venv\Scripts\python.exe" tennis_court_finder.py
) else (
    py -3 tennis_court_finder.py
)
start "" "tennis.html"
echo.
pause
