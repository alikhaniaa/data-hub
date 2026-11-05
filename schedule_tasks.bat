@echo off
rem OPTIONAL: creates Windows Task Scheduler jobs so data keeps accumulating without opening the app.
rem   TDH_Fast   every 15 min : exchange snapshots (tickers, order-book depth), options snapshots
rem   TDH_Hourly every 60 min : Hyperliquid, sentiment/news, Yahoo markets
rem   TDH_Daily  06:00 daily  : everything (incremental)
rem Remove them with  schedule_remove.bat . Jobs run only while you are logged in; the PC must be awake.
cd /d "%~dp0"
set ROOT=%~dp0
schtasks /create /f /tn "TDH_Fast" /sc minute /mo 15 /tr "\"%ROOT%run_fast.bat\""
schtasks /create /f /tn "TDH_Hourly" /sc hourly /mo 1 /tr "\"%ROOT%run_hourly.bat\""
schtasks /create /f /tn "TDH_Daily" /sc daily /st 06:00 /tr "\"%ROOT%run_daily.bat\""
echo.
schtasks /query /tn "TDH_Fast" /fo list | findstr /i "TaskName Next"
pause
