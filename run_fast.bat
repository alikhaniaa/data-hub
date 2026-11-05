@echo off
rem Scheduled job (every 15 min): sources that have no free history and must be snapshotted.
cd /d "%~dp0"
set PYTHONPATH=%~dp0src
if not exist logs mkdir logs
".venv\Scripts\python.exe" -m tdata run crypto_snapshots crypto_options --parallel 4 >> logs\scheduled_fast.log 2>&1
