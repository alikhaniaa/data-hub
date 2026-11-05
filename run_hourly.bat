@echo off
rem Scheduled job (hourly): faster-moving sources.
cd /d "%~dp0"
set PYTHONPATH=%~dp0src
if not exist logs mkdir logs
".venv\Scripts\python.exe" -m tdata run crypto_hyperliquid crypto_sentiment markets_macro --parallel 4 >> logs\scheduled_hourly.log 2>&1
