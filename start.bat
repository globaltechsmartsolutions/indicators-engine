@echo off
setlocal
echo ========================================
echo   Indicators Engine - Startup Script
echo ========================================
echo.

for %%i in ("%~dp0..") do set "GT_ROOT=%%~fi"
set "VENV_DIR=%GT_ROOT%\.venv"
set "ACTIVATE_SCRIPT=%VENV_DIR%\Scripts\activate.bat"
set "PYTHON_CMD=%VENV_DIR%\Scripts\python.exe"

if not exist "%PYTHON_CMD%" (
    echo ERROR: No se encontro el entorno virtual comun en:
    echo   %VENV_DIR%
    echo Ejecuta primero: ..\setup_common_env.bat
    pause
    exit /b 1
)

call "%ACTIVATE_SCRIPT%"
if errorlevel 1 (
    echo ERROR: No se pudo activar el entorno virtual comun
    pause
    exit /b 1
)

:: Verificar Rust
python -c "import indicators_core" 2>nul
if errorlevel 1 (
    echo ADVERTENCIA: Rust core no disponible
    echo Para compilar: cd rust-core ^&^& maturin develop --release
    echo Continuando con Python fallback...
    echo.
) else (
    echo [OK] Rust core instalado
)

:: Ejecutar engine
echo.
echo Iniciando Indicators Engine...
echo Presiona Ctrl+C para detener
echo.

python run_engine.py

pause
endlocal

