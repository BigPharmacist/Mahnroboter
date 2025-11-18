@echo off
REM Autostart-Installation für Mahnroboter (Windows)
REM Muss als Administrator ausgeführt werden!

echo === Mahnroboter Autostart installieren ===
echo.
echo WICHTIG: Dieses Skript muss als Administrator ausgefuehrt werden!
echo Rechtsklick auf die Datei -^> "Als Administrator ausfuehren"
echo.

REM Administrator-Check
net session >nul 2>&1
if errorlevel 1 (
    echo FEHLER: Bitte als Administrator ausfuehren!
    echo Rechtsklick auf install_autostart.bat -^> "Als Administrator ausfuehren"
    pause
    exit /b 1
)

REM Aktuellen Pfad ermitteln
set "SCRIPT_DIR=%~dp0"
set "START_SCRIPT=%SCRIPT_DIR%start.bat"
set "VBS_SCRIPT=%SCRIPT_DIR%start_background.vbs"

echo Aktueller Ordner: %SCRIPT_DIR%
echo.

REM Task Scheduler Aufgabe erstellen
echo Erstelle Windows Task Scheduler Aufgabe...
schtasks /create /tn "Mahnroboter" /tr "\"%VBS_SCRIPT%\" \"%START_SCRIPT%\"" /sc onlogon /rl highest /f

if errorlevel 1 (
    echo FEHLER: Konnte Task nicht erstellen!
    pause
    exit /b 1
)

echo.
echo === Autostart erfolgreich installiert! ===
echo.
echo Die App wird jetzt bei jedem Windows-Start automatisch im Hintergrund gestartet.
echo.
echo Weitere Befehle:
echo   - Autostart deaktivieren: schtasks /change /tn "Mahnroboter" /disable
echo   - Autostart aktivieren:   schtasks /change /tn "Mahnroboter" /enable
echo   - Autostart entfernen:    schtasks /delete /tn "Mahnroboter" /f
echo   - Status pruefen:         schtasks /query /tn "Mahnroboter"
echo.
echo HINWEIS: Die App laeuft im Hintergrund. Du kannst sie im Browser unter
echo          http://localhost:8080 oeffnen.
echo.
pause
