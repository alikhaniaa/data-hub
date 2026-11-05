@echo off
rem Opens the Trading Data Hub window (no console).
cd /d "%~dp0"
set PYTHONPATH=%~dp0src
if not exist ".venv\Scripts\pythonw.exe" (
  echo Run setup.bat first.
  pause
  exit /b 1
)
start "" ".venv\Scripts\pythonw.exe" -m tdata gui
