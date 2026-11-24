@echo off
rem Nightly: scan new diaries, run every experiment, compress. Started by Task Scheduler at 1:00 am.
cd /d "%~dp0.."
call venv\Scripts\activate.bat
python -m mmstack.main nightly --workers 3
