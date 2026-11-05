@echo off
rem Print the snapshot from already-collected data (no network).
cd /d "%~dp0"
set PYTHONPATH=%~dp0src
if not exist logs mkdir logs
".venv\Scripts\python.exe" -m tdata briefing > "logs\briefing_latest.txt"
type "logs\briefing_latest.txt"
pause
