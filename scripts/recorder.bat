@echo off
rem Recorder with a fresh diary every midnight, New York time.
rem Stop it with Ctrl+C (answer Y if Windows asks "Terminate batch job").
cd /d "%~dp0.."
call venv\Scripts\activate.bat
:loop
python -m mmstack.main live --paper-mm --data-dir E:\mmdata --until-midnight
if %errorlevel%==3 goto end
echo recorder stopped (code %errorlevel%) - starting the next diary in 3 seconds...
timeout /t 3 /nobreak >nul
goto loop
:end
echo recorder stopped by you.
pause
