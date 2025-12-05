#!/bin/bash
# Start-Skript für Mahnroboter

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Prüfe auf Updates von GitHub
if [ -f "check_update.sh" ]; then
    ./check_update.sh
    echo ""
fi

echo "=== Mahnroboter starten ==="
echo ""

# Prüfen ob Virtual Environment existiert
if [ -d ".venv" ]; then
    echo "Virtual Environment gefunden, aktiviere..."
    source .venv/bin/activate
    python web_app.py --port 8080
else
    echo "Kein Virtual Environment gefunden, starte direkt..."
    python3 web_app.py --port 8080
fi
