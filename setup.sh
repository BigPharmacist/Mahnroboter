#!/bin/bash
# Setup-Skript für Mahnroboter
# Für macOS und Linux

set -e  # Bei Fehler abbrechen

echo "=== Mahnroboter Setup ==="
echo ""

# 1. Python-Version prüfen
echo "[1/4] Python-Version prüfen..."
if ! command -v python3 &> /dev/null; then
    echo "FEHLER: Python 3 ist nicht installiert!"
    echo "Bitte installiere Python 3.8 oder höher."
    exit 1
fi

PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
echo "Python $PYTHON_VERSION gefunden"
echo ""

# 2. .env Datei prüfen
echo "[2/4] Konfigurationsdatei prüfen..."
if [ ! -f ".env" ]; then
    echo "WARNUNG: .env Datei nicht gefunden!"
    echo "Die Anwendung benötigt die .env Datei mit allen Zugangsdaten."
    echo "Bitte stelle sicher, dass die .env Datei im Ordner vorhanden ist."
    read -p "Trotzdem fortfahren? (j/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Jj]$ ]]; then
        exit 1
    fi
else
    echo ".env Datei gefunden"
fi
echo ""

# 3. Dependencies installieren
echo "[3/4] Python-Pakete installieren..."
echo "Möchtest du ein Virtual Environment verwenden?"
echo "  [1] Ja, mit Virtual Environment (empfohlen)"
echo "  [2] Nein, systemweit installieren"
read -p "Auswahl (1 oder 2): " choice

if [ "$choice" = "1" ]; then
    echo "Erstelle Virtual Environment..."
    python3 -m venv .venv

    echo "Aktiviere Virtual Environment..."
    source .venv/bin/activate

    echo "Installiere Pakete..."
    pip install --upgrade pip
    pip install -r requirements.txt

    echo ""
    echo "=== Installation abgeschlossen! ==="
    echo ""
    echo "Die App wurde mit Virtual Environment installiert."
    echo ""
    echo "Zum Starten der App:"
    echo "  1. Virtual Environment aktivieren:"
    echo "     source .venv/bin/activate"
    echo "  2. App starten:"
    echo "     python web_app.py --port 8080"
    echo ""
    echo "Oder verwende: ./start.sh"

else
    echo "Installiere Pakete systemweit..."
    pip3 install -r requirements.txt

    echo ""
    echo "=== Installation abgeschlossen! ==="
    echo ""
    echo "Die App wurde systemweit installiert."
    echo ""
    echo "Zum Starten der App:"
    echo "  python3 web_app.py --port 8080"
    echo ""
    echo "Oder verwende: ./start.sh"
fi

echo ""
echo "Danach im Browser öffnen: http://localhost:8080"
