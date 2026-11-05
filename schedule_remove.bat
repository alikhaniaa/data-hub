@echo off
schtasks /delete /f /tn "TDH_Fast"
schtasks /delete /f /tn "TDH_Hourly"
schtasks /delete /f /tn "TDH_Daily"
pause
