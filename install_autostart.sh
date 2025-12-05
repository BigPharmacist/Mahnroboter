#!/bin/bash
# Autostart-Installation für Mahnroboter (macOS)

echo "=== Mahnroboter Autostart installieren (macOS) ==="
echo ""

# Aktuellen Pfad ermitteln
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
START_SCRIPT="$SCRIPT_DIR/start.sh"
PLIST_NAME="com.mahnroboter.app"
PLIST_FILE="$HOME/Library/LaunchAgents/$PLIST_NAME.plist"

echo "Aktueller Ordner: $SCRIPT_DIR"
echo "Start-Skript: $START_SCRIPT"
echo ""

# Prüfen ob start.sh existiert
if [ ! -f "$START_SCRIPT" ]; then
    echo "FEHLER: start.sh nicht gefunden in $SCRIPT_DIR"
    exit 1
fi

# LaunchAgents Ordner erstellen falls nicht vorhanden
mkdir -p "$HOME/Library/LaunchAgents"

# Plist-Datei erstellen
echo "Erstelle Launch Agent Konfiguration..."
cat > "$PLIST_FILE" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>$PLIST_NAME</string>
    <key>ProgramArguments</key>
    <array>
        <string>$START_SCRIPT</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <false/>
    <key>StandardOutPath</key>
    <string>$SCRIPT_DIR/mahnroboter.log</string>
    <key>StandardErrorPath</key>
    <string>$SCRIPT_DIR/mahnroboter.error.log</string>
    <key>WorkingDirectory</key>
    <string>$SCRIPT_DIR</string>
</dict>
</plist>
EOF

if [ $? -ne 0 ]; then
    echo "FEHLER: Konnte Plist-Datei nicht erstellen!"
    exit 1
fi

echo "✓ Plist-Datei erstellt: $PLIST_FILE"

# Launch Agent laden
echo "Lade Launch Agent..."
launchctl load "$PLIST_FILE" 2>/dev/null

if [ $? -eq 0 ]; then
    echo "✓ Launch Agent geladen"
else
    echo "⚠ Launch Agent konnte nicht geladen werden (möglicherweise bereits geladen)"
fi

echo ""
echo "=== Autostart erfolgreich installiert! ==="
echo ""
echo "Die App wird jetzt bei jedem macOS-Start automatisch gestartet."
echo ""
echo "Weitere Befehle:"
echo "  - Autostart stoppen:    launchctl unload ~/Library/LaunchAgents/$PLIST_NAME.plist"
echo "  - Autostart starten:    launchctl load ~/Library/LaunchAgents/$PLIST_NAME.plist"
echo "  - Autostart entfernen:  ./uninstall_autostart.sh"
echo "  - Status prüfen:        launchctl list | grep mahnroboter"
echo ""
echo "HINWEIS: Die App läuft im Hintergrund und ist erreichbar unter:"
echo "         http://localhost:8080"
echo ""
echo "Logs befinden sich hier:"
echo "  - Ausgabe: $SCRIPT_DIR/mahnroboter.log"
echo "  - Fehler:  $SCRIPT_DIR/mahnroboter.error.log"
echo ""
