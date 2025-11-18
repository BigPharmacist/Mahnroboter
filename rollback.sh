#!/bin/bash

# Mahnungen Rollback-Script
# Hilft beim sicheren Zurückkehren zu früheren Versionen

# Farben
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}=== Git Rollback-Assistent ===${NC}\n"

# Funktion: Zeige letzte Commits
show_commits() {
    echo -e "${YELLOW}Letzte 20 Commits:${NC}\n"
    git log --oneline --graph -20
    echo ""
}

# Funktion: Zeige verfügbare Backups
show_backups() {
    echo -e "${YELLOW}Verfügbare Backups:${NC}\n"
    BACKUP_BASE="../../Backups/Mahnungen"
    if [ -d "$BACKUP_BASE" ]; then
        ls -lt "$BACKUP_BASE" | grep ^d | head -n 10 | awk '{print NR". "$9" ("$6" "$7" "$8")"}'
    else
        echo -e "${RED}Keine Backups gefunden${NC}"
    fi
    echo ""
}

# Funktion: Interaktiver Rollback
interactive_rollback() {
    echo -e "${YELLOW}Wähle Rollback-Methode:${NC}"
    echo "1) Zu einem Git-Commit zurückkehren (Änderungen bleiben erhalten)"
    echo "2) Zu einem Git-Commit zurückkehren (ALLE Änderungen verwerfen)"
    echo "3) Backup wiederherstellen"
    echo "4) Nur eine Datei wiederherstellen"
    echo "5) Abbrechen"
    echo ""
    read -p "Auswahl (1-5): " choice

    case $choice in
        1)
            rollback_soft
            ;;
        2)
            rollback_hard
            ;;
        3)
            restore_backup
            ;;
        4)
            restore_file
            ;;
        5)
            echo -e "${BLUE}Abgebrochen${NC}"
            exit 0
            ;;
        *)
            echo -e "${RED}Ungültige Auswahl${NC}"
            exit 1
            ;;
    esac
}

# Soft Rollback (Änderungen bleiben)
rollback_soft() {
    show_commits
    echo -e "${YELLOW}Zu welchem Commit möchtest du zurück?${NC}"
    read -p "Commit-Hash (erste 7 Zeichen reichen): " commit_hash

    if [ -z "$commit_hash" ]; then
        echo -e "${RED}Kein Commit angegeben${NC}"
        exit 1
    fi

    echo -e "${YELLOW}Erstelle Sicherheits-Backup...${NC}"
    ./create_backup.sh full

    echo -e "${YELLOW}Kehre zu Commit $commit_hash zurück (Änderungen bleiben erhalten)...${NC}"
    git reset --soft "$commit_hash"

    echo -e "${GREEN}✓ Rollback erfolgreich!${NC}"
    echo -e "${BLUE}Deine Änderungen sind noch vorhanden (git status zum Anzeigen)${NC}"
}

# Hard Rollback (ALLES verwerfen)
rollback_hard() {
    show_commits
    echo -e "${RED}⚠ WARNUNG: Alle Änderungen seit diesem Commit werden PERMANENT gelöscht!${NC}"
    echo -e "${YELLOW}Zu welchem Commit möchtest du zurück?${NC}"
    read -p "Commit-Hash: " commit_hash

    if [ -z "$commit_hash" ]; then
        echo -e "${RED}Kein Commit angegeben${NC}"
        exit 1
    fi

    echo -e "${RED}Bist du SICHER? Dies kann nicht rückgängig gemacht werden!${NC}"
    read -p "Ja, ALLES löschen (tippe 'JA SICHER'): " confirm

    if [ "$confirm" != "JA SICHER" ]; then
        echo -e "${BLUE}Abgebrochen${NC}"
        exit 0
    fi

    echo -e "${YELLOW}Erstelle Notfall-Backup...${NC}"
    ./create_backup.sh full

    echo -e "${RED}Führe Hard Reset durch...${NC}"
    git reset --hard "$commit_hash"

    echo -e "${GREEN}✓ Hard Rollback abgeschlossen${NC}"
    echo -e "${YELLOW}Falls etwas schiefging, siehe Backup in ../../Backups/Mahnungen/${NC}"
}

# Backup wiederherstellen
restore_backup() {
    show_backups
    echo -e "${YELLOW}Welches Backup möchtest du wiederherstellen?${NC}"
    read -p "Nummer: " backup_num

    BACKUP_BASE="../../Backups/Mahnungen"
    BACKUP_DIR=$(ls -lt "$BACKUP_BASE" | grep ^d | head -n 10 | awk "NR==$backup_num {print \$9}")

    if [ -z "$BACKUP_DIR" ]; then
        echo -e "${RED}Ungültige Auswahl${NC}"
        exit 1
    fi

    BACKUP_PATH="${BACKUP_BASE}/${BACKUP_DIR}"

    echo -e "${YELLOW}Wiederherstellen von: $BACKUP_DIR${NC}"
    echo -e "${RED}Dies überschreibt aktuelle Dateien!${NC}"
    read -p "Fortfahren? (j/n): " confirm

    if [ "$confirm" != "j" ]; then
        echo -e "${BLUE}Abgebrochen${NC}"
        exit 0
    fi

    # Aktuellen Stand sichern
    echo -e "${YELLOW}Sichere aktuellen Stand...${NC}"
    ./create_backup.sh full

    # Dateien wiederherstellen
    echo -e "${YELLOW}Stelle Dateien wieder her...${NC}"
    cp -v "${BACKUP_PATH}"/*.py . 2>/dev/null
    cp -v "${BACKUP_PATH}"/*.db . 2>/dev/null
    cp -v "${BACKUP_PATH}"/*.sh . 2>/dev/null
    cp -v "${BACKUP_PATH}"/*.bat . 2>/dev/null

    echo -e "${GREEN}✓ Backup wiederhergestellt${NC}"
    echo -e "${BLUE}Git-Info aus diesem Backup:${NC}"
    cat "${BACKUP_PATH}/GIT_INFO.txt"
}

# Einzelne Datei wiederherstellen
restore_file() {
    show_commits
    echo -e "${YELLOW}Welche Datei möchtest du wiederherstellen?${NC}"
    read -p "Dateiname: " filename

    if [ -z "$filename" ]; then
        echo -e "${RED}Kein Dateiname angegeben${NC}"
        exit 1
    fi

    echo -e "${YELLOW}Von welchem Commit? (leer = letzter Commit)${NC}"
    read -p "Commit-Hash (oder Enter): " commit_hash

    if [ -z "$commit_hash" ]; then
        commit_hash="HEAD"
    fi

    echo -e "${YELLOW}Stelle $filename von $commit_hash wieder her...${NC}"
    git checkout "$commit_hash" -- "$filename"

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Datei wiederhergestellt: $filename${NC}"
    else
        echo -e "${RED}✗ Fehler beim Wiederherstellen${NC}"
    fi
}

# Hauptmenü
echo -e "${YELLOW}Was möchtest du tun?${NC}"
echo "1) Letzte Commits anzeigen"
echo "2) Verfügbare Backups anzeigen"
echo "3) Rollback durchführen"
echo "4) Abbrechen"
echo ""
read -p "Auswahl (1-4): " main_choice

case $main_choice in
    1)
        show_commits
        ;;
    2)
        show_backups
        ;;
    3)
        interactive_rollback
        ;;
    4)
        echo -e "${BLUE}Abgebrochen${NC}"
        exit 0
        ;;
    *)
        echo -e "${RED}Ungültige Auswahl${NC}"
        exit 1
        ;;
esac

echo ""
echo -e "${GREEN}Fertig!${NC}"
