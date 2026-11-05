@echo off
rem Scheduled job (daily): every default collector (incremental), then refresh the catalogue.
cd /d "%~dp0"
set PYTHONPATH=%~dp0src
if not exist logs mkdir logs
".venv\Scripts\python.exe" -m tdata run all --parallel 5 >> logs\scheduled_daily.log 2>&1
".venv\Scripts\python.exe" -m tdata catalog >> logs\scheduled_daily.log 2>&1
