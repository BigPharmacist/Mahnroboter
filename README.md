# Mahnroboter - Rechnungsverwaltung

Web-Anwendung zur Verwaltung und Verarbeitung von Rechnungen mit LetterXpress-Integration.

## Funktionen

- Rechnungs-Übersicht mit Suchfunktion
- PDF-Generierung und -Verwaltung
- E-Mail-Versand (SMTP/IMAP)
- LetterXpress API-Integration für Briefversand
- SQLite-Datenbank für Rechnungsdaten

## Voraussetzungen

- Python 3.8 oder höher
- Die `.env` Datei mit allen Zugangsdaten (LetterXpress API, E-Mail, etc.)

## Schnellstart

### Windows (am einfachsten)

```cmd
REM 1. Einmalig: Installation durchführen
setup.bat

REM 2. App starten
start.bat

REM 3. OPTIONAL: Autostart beim Windows-Start einrichten
REM    (Rechtsklick -> Als Administrator ausführen)
install_autostart.bat
```

Nach der Installation des Autostarts läuft die App automatisch im Hintergrund nach jedem Windows-Neustart!

### macOS/Linux

```bash
# 1. Einmalig: Installation durchführen
./setup.sh

# 2. App starten
./start.sh
```

Das Setup-Skript fragt dich, ob du ein Virtual Environment verwenden möchtest und installiert alle Abhängigkeiten automatisch.

---

## Manuelle Installation

### Option 1: Ohne Virtual Environment (einfacher)

```bash
# Abhängigkeiten installieren
pip install -r requirements.txt

# App starten
python web_app.py --port 8080
```

### Option 2: Mit Virtual Environment (empfohlen für mehrere Python-Projekte)

```bash
# Virtual Environment erstellen
python3 -m venv .venv

# Virtual Environment aktivieren
# macOS/Linux:
source .venv/bin/activate
# Windows:
.venv\Scripts\activate

# Abhängigkeiten installieren
pip install -r requirements.txt

# App starten
python web_app.py --port 8080
```

## App öffnen

Nach dem Start öffne im Browser:
```
http://localhost:8080
```

## Wichtige Dateien

### Windows-Skripte
- **setup.bat** - Automatisches Installations-Skript
- **start.bat** - App-Start-Skript
- **install_autostart.bat** - Installiert Autostart beim Windows-Start
- **uninstall_autostart.bat** - Entfernt Autostart
- **start_background.vbs** - Hilfsskript für unsichtbaren Hintergrund-Start

### macOS/Linux-Skripte
- **setup.sh** - Automatisches Installations-Skript
- **start.sh** - App-Start-Skript

### Python-Anwendung
- **web_app.py** - Haupt-Flask-Anwendung
- **invoice_tracker.py** - Rechnungsverarbeitung
- **letterxpress_client.py** - LetterXpress API-Client
- **generate_invoices.py** - Rechnungsgenerierung
- **.env** - Konfiguration und Zugangsdaten (WICHTIG: Nicht löschen!)
- **invoice_data.db** - SQLite-Datenbank
- **requirements.txt** - Python-Abhängigkeiten

## Ordnerstruktur

```
Mahnungen/
├── Rechnungen/          # Generierte PDF-Rechnungen
├── Sammelrechnungen/    # Sammelrechnungen
├── Vorlagen/            # PDF-Vorlagen
├── templates/           # HTML-Templates für Web-UI
├── static/              # CSS/JS/Bilder für Web-UI
└── .env                 # Konfigurationsdatei (WICHTIG!)
```

## Beim Kopieren auf neuen Computer

**Mitkopieren:**
- Alle Dateien und Ordner (außer .venv und __pycache__)
- **Besonders wichtig:** Die `.env` Datei mit allen Zugangsdaten

**NICHT kopieren:**
- `.venv/` Ordner (wird neu erstellt)
- `__pycache__/` Ordner (wird automatisch generiert)

## Konfiguration (.env)

Die `.env` Datei enthält:
- LetterXpress API-Zugangsdaten
- E-Mail Server-Konfiguration (IMAP/SMTP)
- Nebius AI API-Schlüssel

Diese Datei muss vorhanden sein, damit die App funktioniert.

## Windows Autostart verwalten

Nach Installation des Autostarts kannst du diesen verwalten:

```cmd
REM Autostart deaktivieren (App startet nicht mehr automatisch)
schtasks /change /tn "Mahnroboter" /disable

REM Autostart aktivieren
schtasks /change /tn "Mahnroboter" /enable

REM Autostart komplett entfernen
uninstall_autostart.bat

REM Status prüfen
schtasks /query /tn "Mahnroboter"
```

Die App läuft nach dem Autostart unsichtbar im Hintergrund und ist unter http://localhost:8080 erreichbar.

## Sicherheitssystem für Weiterentwicklung

### Git-Versionskontrolle

Das Projekt nutzt Git für sichere Versionskontrolle und Rollback-Möglichkeiten.

**Wichtigste Befehle:**

```bash
# Vor jeder Entwicklung: Backup erstellen
./create_backup.sh

# Änderungen committen
git add .
git commit -m "Beschreibung der Änderung"
git push origin main

# Rollback durchführen (interaktiv)
./rollback.sh
```

### Backup-System

**Automatisches Backup erstellen:**
```bash
# Vollständiges Backup (Code + Datenbanken)
./create_backup.sh

# Nur Datenbanken
./create_backup.sh db-only
```

Backups werden in `../../Backups/Mahnungen/` gespeichert. Die letzten 10 Backups werden automatisch behalten.

### Rollback-Optionen

Das `rollback.sh` Script bietet mehrere Möglichkeiten:

1. **Soft Rollback** - Zu altem Stand zurück, Änderungen bleiben
2. **Hard Rollback** - Kompletter Reset (VORSICHT!)
3. **Backup wiederherstellen** - Aus gespeichertem Backup
4. **Einzelne Datei** - Nur eine spezifische Datei wiederherstellen

Starte interaktiv:
```bash
./rollback.sh
```

### Entwicklungs-Workflow

1. **Vor jeder Änderung**: Backup erstellen
2. **Während Entwicklung**: Regelmäßig committen
3. **Nach Test**: Push zu GitHub
4. **Bei Problemen**: Rollback nutzen

Detaillierte Anleitung siehe: [GIT_WORKFLOW.md](GIT_WORKFLOW.md)

### Geschützte Dateien

Die `.gitignore` schützt automatisch:
- Datenbanken (*.db)
- Credentials und API-Keys (.env)
- Temporäre Dateien
- Backup-Ordner

**WICHTIG:** Die `.env` Datei wird NICHT ins Repository committed. Sichere sie separat!
