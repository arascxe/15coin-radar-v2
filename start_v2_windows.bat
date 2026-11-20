@echo off
setlocal
cd /d "%~dp0"
if not exist "v2\config.json" (
 echo HATA: Bu paketi 15coin-radar-v2 deposunun kok klasorune yerlestir. v2\config.json bulunamadi.
 pause
 exit /b 1
)
python -c "import websocket" >nul 2>&1
if errorlevel 1 (
 echo UCRETSIZ websocket-client gerekli. Elle kurulum: python -m pip install websocket-client
 pause
 exit /b 2
)
set "PYTHONPATH=%CD%;%CD%\v2"
python -m v2_full.monitor --config v2\config.json --output-dir v2_local_live --notify-local
pause
