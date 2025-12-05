#!/bin/bash
# Manuelles Update mit Bestätigung

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== Manuelles Update von GitHub ==="
echo ""

# Prüfen ob git verfügbar ist
if ! command -v git &> /dev/null; then
    echo "❌ Git nicht gefunden"
    exit 1
fi

# Aktuellen Branch ermitteln
CURRENT_BRANCH=$(git branch --show-current)
echo "Aktueller Branch: $CURRENT_BRANCH"

# Prüfen ob es lokale Änderungen gibt
if ! git diff-index --quiet HEAD --; then
    echo ""
    echo "⚠️  WARNUNG: Du hast lokale Änderungen!"
    echo ""
    git status --short
    echo ""
    read -p "Möchtest du diese Änderungen stashen (temporär speichern)? (j/n) " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Jj]$ ]]; then
        git stash push -m "Auto-stash vor Update $(date +%Y-%m-%d_%H-%M-%S)"
        echo "✓ Änderungen gespeichert (später mit 'git stash pop' wiederherstellen)"
    else
        echo "❌ Update abgebrochen"
        exit 1
    fi
fi

# Remote-Updates holen
echo ""
echo "Hole Updates von GitHub..."
git fetch origin

if [ $? -ne 0 ]; then
    echo "❌ Konnte Updates nicht von GitHub holen"
    exit 1
fi

# Prüfen ob Updates verfügbar sind
LOCAL_COMMIT=$(git rev-parse HEAD)
REMOTE_COMMIT=$(git rev-parse origin/$CURRENT_BRANCH)

if [ "$LOCAL_COMMIT" = "$REMOTE_COMMIT" ]; then
    echo ""
    echo "✓ App ist bereits auf dem neuesten Stand!"
    exit 0
fi

echo ""
echo "📦 Neue Updates verfügbar:"
echo ""
git log --oneline --decorate HEAD..origin/$CURRENT_BRANCH
echo ""

read -p "Möchtest du diese Updates jetzt installieren? (j/n) " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Jj]$ ]]; then
    echo "Update abgebrochen"
    exit 0
fi

# Backup erstellen
if [ -f "create_backup.sh" ]; then
    echo ""
    read -p "Möchtest du ein Backup erstellen? (empfohlen) (j/n) " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Jj]$ ]]; then
        ./create_backup.sh
    fi
fi

# Updates pullen
echo ""
echo "Installiere Updates..."
git pull origin $CURRENT_BRANCH

if [ $? -ne 0 ]; then
    echo "❌ Fehler beim Installieren der Updates"
    echo "Stelle ggf. deine Änderungen wieder her mit: git stash pop"
    exit 1
fi

echo "✓ Updates erfolgreich installiert"

# Prüfen ob requirements.txt geändert wurde
if git diff --name-only HEAD@{1} HEAD | grep -q "requirements.txt"; then
    echo ""
    echo "📦 requirements.txt wurde aktualisiert"
    read -p "Möchtest du die Abhängigkeiten neu installieren? (j/n) " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Jj]$ ]]; then
        if [ -d ".venv" ]; then
            source .venv/bin/activate
            pip install -r requirements.txt
            echo "✓ Abhängigkeiten aktualisiert"
        else
            pip install -r requirements.txt
        fi
    fi
fi

echo ""
echo "=== Update abgeschlossen ==="
echo ""
echo "Starte die App neu, damit die Änderungen wirksam werden:"
echo "  ./start.sh"
echo ""
