#!/bin/bash
# Autostart-Deinstallation für Mahnroboter (macOS)

echo "=== Mahnroboter Autostart deinstallieren (macOS) ==="
echo ""

PLIST_NAME="com.mahnroboter.app"
PLIST_FILE="$HOME/Library/LaunchAgents/$PLIST_NAME.plist"

if [ ! -f "$PLIST_FILE" ]; then
    echo "FEHLER: Autostart ist nicht installiert."
    echo "Plist-Datei nicht gefunden: $PLIST_FILE"
    exit 1
fi

# Launch Agent entladen
echo "Entlade Launch Agent..."
launchctl unload "$PLIST_FILE" 2>/dev/null

if [ $? -eq 0 ]; then
    echo "✓ Launch Agent entladen"
else
    echo "⚠ Launch Agent konnte nicht entladen werden (möglicherweise nicht geladen)"
fi

# Plist-Datei löschen
echo "Lösche Plist-Datei..."
rm "$PLIST_FILE"

if [ $? -eq 0 ]; then
    echo "✓ Plist-Datei gelöscht"
else
    echo "FEHLER: Konnte Plist-Datei nicht löschen!"
    exit 1
fi

echo ""
echo "=== Autostart erfolgreich deinstalliert! ==="
echo ""
echo "Die App wird nicht mehr automatisch beim macOS-Start gestartet."
echo ""
