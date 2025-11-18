@echo off
REM Autostart-Deinstallation für Mahnroboter (Windows)
REM Muss als Administrator ausgeführt werden!

echo === Mahnroboter Autostart entfernen ===
echo.
echo WICHTIG: Dieses Skript muss als Administrator ausgefuehrt werden!
echo.

REM Administrator-Check
net session >nul 2>&1
if errorlevel 1 (
    echo FEHLER: Bitte als Administrator ausfuehren!
    echo Rechtsklick auf uninstall_autostart.bat -^> "Als Administrator ausfuehren"
    pause
    exit /b 1
)

echo Entferne Windows Task Scheduler Aufgabe...
schtasks /delete /tn "Mahnroboter" /f

if errorlevel 1 (
    echo FEHLER: Konnte Task nicht entfernen!
    echo Eventuell ist kein Autostart installiert.
    pause
    exit /b 1
)

echo.
echo === Autostart erfolgreich entfernt! ===
echo.
echo Die App wird nicht mehr automatisch beim Windows-Start gestartet.
echo.
pause
