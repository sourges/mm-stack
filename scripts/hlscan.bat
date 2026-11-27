@echo off
rem Hyperliquid maker scanner: records trades (both accounts on each) into a
rem fresh file every midnight, New York time. Stop with Ctrl+C (answer Y if
rem Windows asks "Terminate batch job").
cd /d "%~dp0.."
call venv\Scripts\activate.bat
:loop
python -m mmstack.hlscan record --data-dir E:\hldata --until-midnight
if %errorlevel%==3 goto end
echo hlscan stopped (code %errorlevel%) - starting the next file in 3 seconds...
timeout /t 3 /nobreak >nul
goto loop
:end
echo hlscan stopped by you.
pause
