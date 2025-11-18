@echo off
REM Start-Skript für Mahnroboter (Windows)

echo === Mahnroboter starten ===
echo.

REM Prüfen ob Virtual Environment existiert
if exist ".venv\Scripts\activate.bat" (
    echo Virtual Environment gefunden, aktiviere...
    call .venv\Scripts\activate.bat
    python web_app.py --port 8080
) else (
    echo Kein Virtual Environment gefunden, starte direkt...
    python web_app.py --port 8080
)
