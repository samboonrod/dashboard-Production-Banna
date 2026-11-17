@echo off
chcp 65001 >nul
title Egg Dashboard Server - Banna Plant
cd /d "%~dp0"
where py >nul 2>nul && (py -3 server.py %*) || (python server.py %*)
if errorlevel 1 (
  echo.
  echo [!] Start failed. Please install Python 3 from https://www.python.org/downloads/
  echo     and tick "Add python.exe to PATH" during install.
)
pause
