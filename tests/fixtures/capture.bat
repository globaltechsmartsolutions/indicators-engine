@echo off
setlocal
set "VENV_PYTHON=%CD%\..\..\.venv\Scripts\python.exe"
set "TOOLS_DIR=%CD%\..\tools"
set "FIXTURES_DIR=%CD%\fixtures"

if not exist "%VENV_PYTHON%" (
    echo Error: Python venv no encontrado en %VENV_PYTHON%
    echo Asegurate de estar en indicators-engine\tests\fixtures
    pause
    exit /b 1
)

echo ================================================================
echo   Captura de Fixtures Reales
echo ================================================================
echo.
echo Asegurate de que el stack completo este corriendo:
echo   - Fake Extractor
echo   - Indicators Engine
echo   - NATS Server
echo.
pause

set "TIMESTAMP=%date:~-4,4%%date:~-7,2%%date:~-10,2%"
set "FIXTURE_FILE=%FIXTURES_DIR%\live_session_%TIMESTAMP%.jsonl"

echo Capturando fixtures durante 30 segundos...
echo Output: %FIXTURE_FILE%
echo.

"%VENV_PYTHON%" "%TOOLS_DIR%\capture_fixtures.py" ^
    --seconds 30 ^
    --pattern "md.>" ^
    --limit 1000 ^
    --output "%FIXTURE_FILE%"

if %ERRORLEVEL% EQU 0 (
    echo.
    echo Fixtures capturados exitosamente!
    echo.
    echo Para generar golden outputs, ejecuta:
    echo   python tools\generate_golden_outputs.py %FIXTURE_FILE% tests\fixtures\golden_outputs_%TIMESTAMP%.jsonl
) else (
    echo.
    echo Error capturando fixtures. Verifica que NATS y el engine esten corriendo.
)

pause

