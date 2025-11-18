#!/bin/bash
# Start-Skript für Mahnroboter

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
