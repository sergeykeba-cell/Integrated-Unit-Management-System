@echo off
chcp 65001 > nul 2>&1
cd /d "%~dp0"

:: ============================================================
::  В/Ч А7020 — Запуск сервера
::
::  За замовчуванням: тільки localhost (127.0.0.1)
::  Для мережевого доступу (Tailscale/LAN):
::    START.bat --network
:: ============================================================

set ARGS=%*
echo  Starting server...
python server.py %ARGS%
echo.
echo  Exit code: %errorlevel%
pause
