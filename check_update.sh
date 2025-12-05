#!/bin/bash
# Prüft auf Updates von GitHub und installiert sie automatisch

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== Prüfe auf Updates von GitHub ==="
echo ""

# Prüfen ob git verfügbar ist
if ! command -v git &> /dev/null; then
    echo "⚠ Git nicht gefunden - Update-Prüfung übersprungen"
    exit 0
fi

# Prüfen ob wir in einem git repository sind
if [ ! -d ".git" ]; then
    echo "⚠ Kein Git-Repository - Update-Prüfung übersprungen"
    exit 0
fi

# Aktuellen Branch ermitteln
CURRENT_BRANCH=$(git branch --show-current)
echo "Aktueller Branch: $CURRENT_BRANCH"

# Prüfen ob es lokale Änderungen gibt
if ! git diff-index --quiet HEAD --; then
    echo "⚠ Lokale Änderungen gefunden - Update übersprungen"
    echo "   Bitte committe oder verwerfe deine Änderungen zuerst."
    exit 0
fi

# Remote-Updates holen (ohne zu mergen)
echo "Hole Updates von GitHub..."
git fetch origin --quiet

if [ $? -ne 0 ]; then
    echo "⚠ Konnte Updates nicht von GitHub holen"
    exit 0
fi

# Prüfen ob Updates verfügbar sind
LOCAL_COMMIT=$(git rev-parse HEAD)
REMOTE_COMMIT=$(git rev-parse origin/$CURRENT_BRANCH)

if [ "$LOCAL_COMMIT" = "$REMOTE_COMMIT" ]; then
    echo "✓ App ist auf dem neuesten Stand"
    exit 0
fi

echo ""
echo "📦 Neue Updates verfügbar!"
echo ""
echo "Änderungen:"
git log --oneline HEAD..origin/$CURRENT_BRANCH | head -5
echo ""

# Backup erstellen vor Update
if [ -f "create_backup.sh" ]; then
    echo "Erstelle Sicherungskopie..."
    ./create_backup.sh --quiet 2>/dev/null || true
fi

# Updates pullen
echo "Installiere Updates..."
git pull origin $CURRENT_BRANCH --quiet

if [ $? -ne 0 ]; then
    echo "❌ Fehler beim Installieren der Updates"
    exit 1
fi

echo "✓ Updates erfolgreich installiert"

# Prüfen ob requirements.txt geändert wurde
if git diff --name-only HEAD@{1} HEAD | grep -q "requirements.txt"; then
    echo ""
    echo "📦 requirements.txt wurde aktualisiert"
    echo "Installiere neue Abhängigkeiten..."

    if [ -d ".venv" ]; then
        source .venv/bin/activate
        pip install -r requirements.txt --quiet
        echo "✓ Abhängigkeiten aktualisiert"
    else
        echo "⚠ Virtual Environment nicht gefunden - bitte manuell installieren:"
        echo "   pip install -r requirements.txt"
    fi
fi

echo ""
echo "=== Update abgeschlossen ==="
echo ""
