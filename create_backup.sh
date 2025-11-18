#!/bin/bash

# Mahnungen Backup-Script
# Erstellt vollständige Backups von Code und Datenbanken

# Farben für Ausgabe
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Konfiguration
BACKUP_BASE_DIR="../../Backups/Mahnungen"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
BACKUP_DIR="${BACKUP_BASE_DIR}/${TIMESTAMP}"
MAX_BACKUPS=10  # Behalte nur die letzten 10 Backups

# Backup-Typ (full oder db-only)
BACKUP_TYPE=${1:-"full"}

echo -e "${BLUE}=== Mahnungen Backup ===${NC}"
echo -e "${BLUE}Zeit: $(date)${NC}"
echo -e "${BLUE}Typ: ${BACKUP_TYPE}${NC}\n"

# Backup-Verzeichnis erstellen
mkdir -p "${BACKUP_DIR}"

# Git-Status prüfen
echo -e "${YELLOW}Prüfe Git-Status...${NC}"
if git diff-index --quiet HEAD --; then
    echo -e "${GREEN}✓ Keine ungespeicherten Änderungen${NC}"
else
    echo -e "${RED}⚠ WARNUNG: Du hast ungespeicherte Änderungen!${NC}"
    echo -e "${YELLOW}Möchtest du diese jetzt committen? (Backup wird trotzdem erstellt)${NC}"
    git status --short
    echo ""
fi

# Datenbanken sichern
echo -e "\n${YELLOW}Sichere Datenbanken...${NC}"
DB_COUNT=0
for db in *.db; do
    if [ -f "$db" ]; then
        cp "$db" "${BACKUP_DIR}/"
        echo -e "${GREEN}✓${NC} $db"
        ((DB_COUNT++))
    fi
done

if [ $DB_COUNT -eq 0 ]; then
    echo -e "${YELLOW}Keine Datenbanken gefunden${NC}"
fi

# Bei Full-Backup: Kompletten Code sichern
if [ "$BACKUP_TYPE" = "full" ]; then
    echo -e "\n${YELLOW}Erstelle Code-Backup...${NC}"

    # Alle Python-Dateien
    for py in *.py; do
        if [ -f "$py" ]; then
            cp "$py" "${BACKUP_DIR}/"
        fi
    done

    # Konfigurationsdateien
    [ -f "requirements.txt" ] && cp requirements.txt "${BACKUP_DIR}/"
    [ -f "README.md" ] && cp README.md "${BACKUP_DIR}/"
    [ -f "GIT_WORKFLOW.md" ] && cp GIT_WORKFLOW.md "${BACKUP_DIR}/"
    [ -f ".gitignore" ] && cp .gitignore "${BACKUP_DIR}/"

    # Shell-Scripte
    for sh in *.sh; do
        if [ -f "$sh" ] && [ "$sh" != "create_backup.sh" ]; then
            cp "$sh" "${BACKUP_DIR}/"
        fi
    done

    # Batch-Dateien für Windows
    for bat in *.bat; do
        if [ -f "$bat" ]; then
            cp "$bat" "${BACKUP_DIR}/"
        fi
    done

    # VBS-Dateien
    for vbs in *.vbs; do
        if [ -f "$vbs" ]; then
            cp "$vbs" "${BACKUP_DIR}/"
        fi
    done

    # Logo und PDFs
    [ -f "Logo Mahnroboter.png" ] && cp "Logo Mahnroboter.png" "${BACKUP_DIR}/"
    [ -f "SEPA_Lastschriftmandat_Vorlage.pdf" ] && cp SEPA_Lastschriftmandat_Vorlage.pdf "${BACKUP_DIR}/"

    echo -e "${GREEN}✓ Code-Dateien gesichert${NC}"
fi

# Git-Commit Hash speichern
echo -e "\n${YELLOW}Speichere Git-Information...${NC}"
{
    echo "Backup erstellt: $(date)"
    echo "Git Commit: $(git rev-parse HEAD)"
    echo "Git Branch: $(git rev-parse --abbrev-ref HEAD)"
    echo "Git Status:"
    git status --short
} > "${BACKUP_DIR}/GIT_INFO.txt"
echo -e "${GREEN}✓ Git-Info gespeichert${NC}"

# Backup-Größe berechnen
BACKUP_SIZE=$(du -sh "${BACKUP_DIR}" | cut -f1)
echo -e "\n${GREEN}✓ Backup erfolgreich erstellt!${NC}"
echo -e "${BLUE}Ort: ${BACKUP_DIR}${NC}"
echo -e "${BLUE}Größe: ${BACKUP_SIZE}${NC}"

# Alte Backups löschen
echo -e "\n${YELLOW}Prüfe alte Backups...${NC}"
BACKUP_COUNT=$(ls -1d ${BACKUP_BASE_DIR}/*/ 2>/dev/null | wc -l)
if [ $BACKUP_COUNT -gt $MAX_BACKUPS ]; then
    echo -e "${YELLOW}Lösche alte Backups (behalte die letzten ${MAX_BACKUPS})...${NC}"
    ls -1dt ${BACKUP_BASE_DIR}/*/ | tail -n +$((MAX_BACKUPS + 1)) | while read old_backup; do
        rm -rf "$old_backup"
        echo -e "${GREEN}✓${NC} Gelöscht: $(basename $old_backup)"
    done
fi

echo -e "\n${GREEN}=== Backup abgeschlossen ===${NC}"
echo -e "${BLUE}Anzahl Backups: $(ls -1d ${BACKUP_BASE_DIR}/*/ 2>/dev/null | wc -l)${NC}\n"

# Erinnerung für Git-Commit
if ! git diff-index --quiet HEAD --; then
    echo -e "${YELLOW}💡 Tipp: Vergiss nicht, deine Änderungen zu committen:${NC}"
    echo -e "   git add ."
    echo -e "   git commit -m \"Beschreibung\""
    echo -e "   git push origin main\n"
fi
