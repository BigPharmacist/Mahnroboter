@echo off
REM Setup-Skript für Mahnroboter (Windows)

echo === Mahnroboter Setup (Windows) ===
echo.

REM 1. Python-Version prüfen
echo [1/4] Python-Version pruefen...
python --version >nul 2>&1
if errorlevel 1 (
    echo FEHLER: Python ist nicht installiert!
    echo Bitte installiere Python 3.8 oder hoeher von python.org
    pause
    exit /b 1
)

for /f "tokens=2" %%i in ('python --version 2^>^&1') do set PYTHON_VERSION=%%i
echo Python %PYTHON_VERSION% gefunden
echo.

REM 2. .env Datei prüfen
echo [2/4] Konfigurationsdatei pruefen...
if not exist ".env" (
    echo WARNUNG: .env Datei nicht gefunden!
    echo Die Anwendung benoetigt die .env Datei mit allen Zugangsdaten.
    echo Bitte stelle sicher, dass die .env Datei im Ordner vorhanden ist.
    set /p CONTINUE="Trotzdem fortfahren? (j/n) "
    if /i not "%CONTINUE%"=="j" exit /b 1
) else (
    echo .env Datei gefunden
)
echo.

REM 3. Dependencies installieren
echo [3/4] Python-Pakete installieren...
echo Moechtest du ein Virtual Environment verwenden?
echo   [1] Ja, mit Virtual Environment (empfohlen)
echo   [2] Nein, systemweit installieren
set /p CHOICE="Auswahl (1 oder 2): "

if "%CHOICE%"=="1" (
    echo Erstelle Virtual Environment...
    python -m venv .venv

    echo Installiere Pakete...
    call .venv\Scripts\activate.bat
    python -m pip install --upgrade pip
    pip install -r requirements.txt

    echo.
    echo === Installation abgeschlossen! ===
    echo.
    echo Die App wurde mit Virtual Environment installiert.
    echo.
    echo Zum Starten der App:
    echo   Doppelklick auf start.bat
    echo.
    echo Oder verwende: install_autostart.bat
    echo   um die App beim Windows-Start automatisch zu starten
) else (
    echo Installiere Pakete systemweit...
    python -m pip install --upgrade pip
    pip install -r requirements.txt

    echo.
    echo === Installation abgeschlossen! ===
    echo.
    echo Die App wurde systemweit installiert.
    echo.
    echo Zum Starten der App:
    echo   Doppelklick auf start.bat
    echo.
    echo Oder verwende: install_autostart.bat
    echo   um die App beim Windows-Start automatisch zu starten
)

echo.
echo Danach im Browser oeffnen: http://localhost:8080
echo.
pause
