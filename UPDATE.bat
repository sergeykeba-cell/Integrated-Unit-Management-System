:: ================== ЄДИНИЙ ІНТЕРАКТИВНИЙ ВИКЛИК ==================
echo ---------------------------------------------------
echo  Running analysis and update (interactive)
echo ---------------------------------------------------
echo Starting interactive run at %DATE% %TIME% >> "%LOGFILE%"
python "%PYTHON_SCRIPT%" --excel "!EXCEL_PATH!" --db "%DATABASE%" --interactive 2>&1
set "PY_EXIT=%errorlevel%"
echo Python exit code: %PY_EXIT% >> "%LOGFILE%"

if %PY_EXIT% neq 0 (
    echo [ERROR] Operation failed with exit code %PY_EXIT%.
    echo ERROR: operation failed >> "%LOGFILE%"
    pause >nul
    popd & exit /b 1
)

echo.
echo === DONE ===
echo === DONE === >> "%LOGFILE%"
echo.