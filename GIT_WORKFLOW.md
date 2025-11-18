# Git-Sicherheitssystem - Workflow & Anleitung

## Branching-Strategie

### Branch-Struktur
```
main          -> Produktions-Version (immer stabil!)
├── dev       -> Entwicklungs-Branch (aktuelle Entwicklung)
├── feature/* -> Feature-Branches (neue Funktionen)
└── hotfix/*  -> Hotfix-Branches (dringende Bugfixes)
```

## Entwicklungs-Workflow

### 1. Vor jeder Änderung: Backup erstellen
```bash
./create_backup.sh
```

### 2. Neues Feature entwickeln
```bash
# Aktuellen Stand sichern
git add .
git commit -m "Beschreibung der Änderungen"
git push origin main

# Feature-Branch erstellen
git checkout -b feature/mein-neues-feature

# Entwickeln und regelmäßig committen
git add .
git commit -m "Feature: Beschreibung"

# Testen!
# Wenn alles funktioniert:
git checkout main
git merge feature/mein-neues-feature
git push origin main

# Branch löschen (optional)
git branch -d feature/mein-neues-feature
```

### 3. Schnelle Änderungen (ohne Branch)
```bash
# Nur für kleine Fixes direkt auf main
git add .
git commit -m "Fix: Beschreibung"
git push origin main
```

## Wichtige Regeln

### ✅ IMMER TUN:
1. **Vor jeder Änderung**: Backup erstellen
2. **Nach jedem funktionierenden Stand**: Commit & Push
3. **Aussagekräftige Commit-Messages**:
   - ✅ "Fix: Rechnungsberechnung korrigiert"
   - ❌ "update"
4. **Testen vor dem Merge**

### ❌ NIEMALS:
1. Datenbank-Dateien (.db) committen
2. API-Keys oder Passwörter committen
3. Auf main pushen, wenn Code nicht funktioniert

## Rollback-Prozess

### Zu letztem funktionierenden Stand zurück
```bash
# 1. Letzte Commits anzeigen
git log --oneline -10

# 2. Zu bestimmtem Commit zurück (OHNE Änderungen zu löschen)
git checkout <commit-hash>

# 3. Wenn dieser Stand gut ist, daraus neuen Branch machen
git checkout -b rollback-sicher
git checkout main
git reset --hard rollback-sicher

# ODER: Nur Dateien aus altem Commit holen
git checkout <commit-hash> -- pfad/zur/datei.py
```

### Komplettes Rollback (VORSICHT!)
```bash
# Zu einem bestimmten Commit zurück, ALLE Änderungen verwerfen
git reset --hard <commit-hash>
git push origin main --force
```

## Backup-Strategie

### Automatische Backups
- Vor jeder Entwicklung: `./create_backup.sh`
- Backups werden in `../Backups/Mahnungen/` gespeichert
- Behalten: Letzte 10 Backups

### Manuelle Backups
```bash
# Vollständiges Backup
./create_backup.sh full

# Nur Datenbanken
./create_backup.sh db-only
```

## Nützliche Git-Befehle

```bash
# Status anzeigen
git status

# Änderungen anzeigen
git diff

# Commit-Historie
git log --oneline --graph --all

# Datei aus Staging entfernen
git reset HEAD datei.py

# Lokale Änderungen verwerfen
git checkout -- datei.py

# Ungespeicherte Änderungen temporär sichern
git stash
git stash pop  # Später wiederherstellen

# Letzten Commit rückgängig machen (Änderungen behalten)
git reset --soft HEAD~1
```

## Notfall-Prozeduren

### Code funktioniert plötzlich nicht mehr
```bash
# 1. Backup wiederherstellen
cd ../Backups/Mahnungen/
ls -lt  # Neueste Backups anzeigen
# Manuell Dateien zurückkopieren

# 2. ODER: Git-Rollback
git log --oneline -20
git checkout <letzter-funktionierender-commit>
```

### Versehentlich Datei gelöscht
```bash
git checkout HEAD -- datei.py
```

### Merge-Konflikt
```bash
# 1. Konflikt-Dateien anzeigen
git status

# 2. Dateien manuell bearbeiten (<<<< ==== >>>> entfernen)

# 3. Als gelöst markieren
git add konflikt-datei.py
git commit -m "Merge-Konflikt gelöst"
```

## Best Practices

1. **Täglich pushen**: Mindestens einmal pro Tag auf GitHub
2. **Kleine Commits**: Lieber viele kleine als ein großer
3. **Branch-Namen**: Aussagekräftig (feature/pdf-export, fix/calculation-bug)
4. **Testing**: Vor jedem Merge testen
5. **Dokumentation**: Änderungen im README festhalten
