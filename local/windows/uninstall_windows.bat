@echo off
rem Double-click to remove the scheduled task
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0install_windows.ps1" -Uninstall
echo.
pause
