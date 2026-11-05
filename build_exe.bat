@echo off
rem Builds TradingDataHub\TradingDataHub.exe (a double-clickable Windows app). Needs setup.bat first.
cd /d "%~dp0"
".venv\Scripts\python.exe" -m pip install pyinstaller
".venv\Scripts\python.exe" -m PyInstaller --noconfirm --clean --windowed --name TradingDataHub ^
  --paths src --collect-submodules tdata ^
  --distpath build_out --workpath build_tmp app_entry.py
if exist TradingDataHub rmdir /s /q TradingDataHub
move build_out\TradingDataHub TradingDataHub
rmdir /s /q build_out build_tmp 2>nul
del TradingDataHub.spec 2>nul
echo.
echo Built: %~dp0TradingDataHub\TradingDataHub.exe  (keep it inside this folder so it finds config\ and data\)
pause
