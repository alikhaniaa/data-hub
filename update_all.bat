@echo off
rem Update every default collector. The first run takes a while, later runs are incremental.
cd /d "%~dp0"
set PYTHONPATH=%~dp0src
".venv\Scripts\python.exe" -m tdata run all --parallel 5
echo.
".venv\Scripts\python.exe" -m tdata briefing
pause
