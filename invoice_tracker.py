#!/usr/bin/env python3
"""
Initial scanner for the ``Rechnungen`` directory.

The script reads every PDF invoice, extracts the key metadata (name, address,
invoice date and total), and stores the result in a local SQLite database. On
subsequent runs, only new files are parsed.
"""

from __future__ import annotations

import argparse
import logging
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
import time
from pathlib import Path
from typing import Iterable, Optional
import os
import unicodedata

from pypdf import PdfReader
import pdfplumber
from dotenv import load_dotenv
import requests
from thefuzz import fuzz
import json

# Load environment variables
load_dotenv()
try:
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers import Observer
    from watchdog.observers.polling import PollingObserver
except ImportError:  # pragma: no cover - optional dependency for --watch mode
    FileSystemEventHandler = None
    Observer = None
    PollingObserver = None

BASE_DIR = Path(__file__).resolve().parent


def get_data_dir() -> Path:
    """
    Get the data directory from environment variable DATA_DIR.
    If not set, defaults to BASE_DIR (application directory).
    Expands ~ to user home directory.
    """
    data_dir_env = os.getenv('DATA_DIR')
    if data_dir_env:
        # Expand ~ to home directory
        expanded_path = Path(data_dir_env).expanduser()
        return expanded_path
    return BASE_DIR


DEFAULT_INVOICE_DIR = get_data_dir() / "Rechnungen"
DEFAULT_DB_PATH = get_data_dir() / "invoice_data.db"

CUSTOMER_MARKERS = {"Herr", "Herrn", "Frau", "Familie"}
DATE_PATTERN = re.compile(r"(?:Datum:\s*(\d{2}\.\d{2}\.\d{4})|(\d{2}\.\d{2}\.\d{4})\s*Datum:)")
INVOICE_NO_PATTERN = re.compile(r"(?:Rechnungs-Nr|Deckblatt-Nr):\s*([\w\-\/]+)")
TOTAL_PATTERN = re.compile(r"(?:Gesamtsumme|Zwischensumme|Rechnungsbetrag).*?([0-9]+(?:\.[0-9]{3})*,[0-9]{2})\s*€")


@dataclass
class InvoiceRecord:
    file_path: str
    invoice_number: Optional[str]
    invoice_date: str  # ISO 8601 (YYYY-MM-DD)
    customer_name: str
    customer_address: str  # Deprecated: use customer_street and customer_city
    amount_cents: int
    customer_street: Optional[str] = None
    customer_city: Optional[str] = None
    address_incomplete: bool = False  # True if address was auto-completed
    name_needs_review: bool = False  # True if customer name failed AI validation


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scan invoice PDFs once and store their metadata.")
    parser.add_argument(
        "--root",
        type=Path,
        default=DEFAULT_INVOICE_DIR,
        help="Root directory that contains the Rechnungen tree (default: %(default)s).",
    )
    parser.add_argument(
        "--database",
        type=Path,
        default=DEFAULT_DB_PATH,
        help="SQLite database file to create/use for storing invoice metadata (default: %(default)s).",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Keep watching the directory after the initial scan and ingest new PDFs automatically.",
    )
    parser.add_argument(
        "--settle-seconds",
        type=float,
        default=0.5,
        help="Delay (in seconds) before reading a freshly created file when --watch is enabled.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging.",
    )
    return parser.parse_args()


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    root_logger.addHandler(console_handler)

    # File handler for errors
    file_handler = logging.FileHandler("import_errors.log", encoding="utf-8")
    file_handler.setLevel(logging.ERROR)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    )
    root_logger.addHandler(file_handler)


def init_db(conn: sqlite3.Connection) -> None:
    # Create snapshots table
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_date TEXT NOT NULL UNIQUE,
            folder_name TEXT NOT NULL,
            scanned_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
            import_complete INTEGER DEFAULT 0
        )
        """
    )

    # Add import_complete column if it doesn't exist (migration for existing databases)
    try:
        conn.execute("ALTER TABLE snapshots ADD COLUMN import_complete INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Create invoices table
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS invoices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_number TEXT,
            customer_name TEXT NOT NULL,
            customer_address TEXT NOT NULL,
            customer_street TEXT,
            customer_city TEXT,
            invoice_date TEXT NOT NULL,
            amount_cents INTEGER NOT NULL,
            currency TEXT NOT NULL DEFAULT 'EUR',
            address_incomplete INTEGER DEFAULT 0,
            name_needs_review INTEGER DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
            UNIQUE(invoice_number, customer_name, amount_cents)
        )
        """
    )

    # Create invoice_snapshots junction table
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS invoice_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_id INTEGER NOT NULL,
            snapshot_id INTEGER NOT NULL,
            file_path TEXT NOT NULL,
            FOREIGN KEY (invoice_id) REFERENCES invoices(id) ON DELETE CASCADE,
            FOREIGN KEY (snapshot_id) REFERENCES snapshots(id) ON DELETE CASCADE,
            UNIQUE(invoice_id, snapshot_id)
        )
        """
    )

    # Create reminders table for tracking Mahnungen
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS reminders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_id INTEGER NOT NULL,
            reminder_level INTEGER NOT NULL CHECK(reminder_level IN (0, 1, 2)),
            created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
            sent_date TEXT,
            letterexpress_status TEXT CHECK(letterexpress_status IN ('pending', 'sent', 'delivered')),
            letterexpress_id TEXT,
            pdf_path TEXT,
            notes TEXT,
            FOREIGN KEY (invoice_id) REFERENCES invoices(id) ON DELETE CASCADE
        )
        """
    )

    # Create customer_details table for managing customer information
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS customer_details (
            customer_name TEXT PRIMARY KEY,
            salutation TEXT,
            email TEXT,
            notes TEXT,
            never_remind INTEGER DEFAULT 0,
            bank_debit INTEGER DEFAULT 0,
            print_only INTEGER DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
        )
        """
    )

    # Add salutation column if it doesn't exist (migration for existing databases)
    try:
        conn.execute("ALTER TABLE customer_details ADD COLUMN salutation TEXT")
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Add never_remind column if it doesn't exist (for existing databases)
    try:
        conn.execute("ALTER TABLE customer_details ADD COLUMN never_remind INTEGER DEFAULT 0")
        conn.commit()
    except sqlite3.OperationalError:
        # Column already exists, that's fine
        pass

    # Add bank_debit column if it doesn't exist (for existing databases)
    try:
        conn.execute("ALTER TABLE customer_details ADD COLUMN bank_debit INTEGER DEFAULT 0")
        conn.commit()
    except sqlite3.OperationalError:
        # Column already exists, that's fine
        pass

    # Add print_only column if it doesn't exist (for existing databases)
    try:
        conn.execute("ALTER TABLE customer_details ADD COLUMN print_only INTEGER DEFAULT 0")
        conn.commit()
    except sqlite3.OperationalError:
        # Column already exists, that's fine
        pass

    # Add custom_name column if it doesn't exist (for overriding customer name)
    try:
        conn.execute("ALTER TABLE customer_details ADD COLUMN custom_name TEXT")
        conn.commit()
    except sqlite3.OperationalError:
        # Column already exists, that's fine
        pass

    # Add custom_street column if it doesn't exist (for overriding customer street)
    try:
        conn.execute("ALTER TABLE customer_details ADD COLUMN custom_street TEXT")
        conn.commit()
    except sqlite3.OperationalError:
        # Column already exists, that's fine
        pass

    # Add custom_city column if it doesn't exist (for overriding customer city)
    try:
        conn.execute("ALTER TABLE customer_details ADD COLUMN custom_city TEXT")
        conn.commit()
    except sqlite3.OperationalError:
        # Column already exists, that's fine
        pass

    # Add uncollectible column to invoices if it doesn't exist (for existing databases)
    try:
        conn.execute("ALTER TABLE invoices ADD COLUMN uncollectible INTEGER DEFAULT 0")
        conn.commit()
    except sqlite3.OperationalError:
        # Column already exists, that's fine
        pass

    # Add customer_street and customer_city columns to invoices if they don't exist
    try:
        conn.execute("ALTER TABLE invoices ADD COLUMN customer_street TEXT")
        conn.commit()
    except sqlite3.OperationalError:
        pass  # Column already exists

    try:
        conn.execute("ALTER TABLE invoices ADD COLUMN customer_city TEXT")
        conn.commit()
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Migrate existing customer_address data to customer_street and customer_city
    # Only migrate rows where customer_street is NULL (not yet migrated)
    conn.execute("""
        UPDATE invoices
        SET
            customer_street = CASE
                WHEN customer_address LIKE '%,%'
                THEN TRIM(SUBSTR(customer_address, 1, INSTR(customer_address, ',') - 1))
                ELSE customer_address
            END,
            customer_city = CASE
                WHEN customer_address LIKE '%,%'
                THEN TRIM(SUBSTR(customer_address, INSTR(customer_address, ',') + 1))
                ELSE ''
            END
        WHERE customer_street IS NULL
    """)

    # Add name_needs_review column to invoices if it doesn't exist (for existing databases)
    try:
        conn.execute("ALTER TABLE invoices ADD COLUMN name_needs_review INTEGER DEFAULT 0")
        conn.commit()
    except sqlite3.OperationalError:
        pass  # Column already exists

    # Create sammelrechnungen_letterxpress table for tracking Letterxpress submissions
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sammelrechnungen_letterxpress (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL UNIQUE,
            letterxpress_job_id INTEGER NOT NULL,
            submitted_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
            mode TEXT NOT NULL CHECK(mode IN ('test', 'live')),
            price REAL,
            status TEXT,
            customer_name TEXT,
            month TEXT
        )
        """
    )

    # Create mahnungen_letterxpress table for tracking Letterxpress submissions of reminders
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS mahnungen_letterxpress (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL,
            pdf_path TEXT NOT NULL UNIQUE,
            letterxpress_job_id INTEGER NOT NULL,
            submitted_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
            mode TEXT NOT NULL CHECK(mode IN ('test', 'live')),
            price REAL,
            status TEXT,
            customer_name TEXT
        )
        """
    )

    # Create collective_invoice_items table to track which invoices are in which collective invoices
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS collective_invoice_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_id INTEGER NOT NULL,
            collective_invoice_filename TEXT NOT NULL,
            collective_invoice_month TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
            FOREIGN KEY (invoice_id) REFERENCES invoices(id) ON DELETE CASCADE,
            UNIQUE(invoice_id, collective_invoice_filename)
        )
        """
    )

    # Create sammelrechnungen_rx table for tracking rX selections per month
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sammelrechnungen_rx (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL,
            month TEXT NOT NULL,
            selected INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
            UNIQUE(filename, month)
        )
        """
    )

    # Create invoice_history table for tracking all events related to an invoice
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS invoice_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            event_timestamp TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
            metadata TEXT,
            FOREIGN KEY (invoice_id) REFERENCES invoices(id) ON DELETE CASCADE
        )
        """
    )

    # Create pending_imports table for imports that need user review due to similar customers
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pending_imports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_path TEXT NOT NULL UNIQUE,
            invoice_number TEXT,
            invoice_date TEXT NOT NULL,
            customer_name TEXT NOT NULL,
            customer_street TEXT,
            customer_city TEXT,
            amount_cents INTEGER NOT NULL,
            snapshot_date TEXT,
            snapshot_id INTEGER,
            similar_customers TEXT,
            status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending', 'resolved', 'rejected')),
            created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
            resolved_at TEXT,
            FOREIGN KEY (snapshot_id) REFERENCES snapshots(id) ON DELETE CASCADE
        )
        """
    )

    # Create form_usage_history table for tracking when forms were added to collective invoices
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS form_usage_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            form_type TEXT NOT NULL CHECK(form_type IN ('email_consent', 'sepa_mandate')),
            usage_month TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
        )
        """
    )

    # Insert initial data for email_consent form (11-2025) if table is empty
    existing_email = conn.execute(
        "SELECT COUNT(*) FROM form_usage_history WHERE form_type = 'email_consent'"
    ).fetchone()[0]
    if existing_email == 0:
        conn.execute(
            """
            INSERT INTO form_usage_history (form_type, usage_month, created_at)
            VALUES ('email_consent', '2025-11', '2025-11-01 00:00:00')
            """
        )

    conn.commit()


def extract_snapshot_from_path(pdf_path: Path, root: Path) -> Optional[tuple[str, str]]:
    """
    Extract snapshot date and folder name from PDF path.
    Returns (snapshot_date, folder_name) or None if not in a monthly folder.

    Example:
      Rechnungen/2024-01-Januar/invoice.pdf -> ("2024-01", "2024-01-Januar")
      Rechnungen/2024-02/invoice.pdf -> ("2024-02", "2024-02")
    """
    try:
        relative_path = pdf_path.relative_to(root)
        parts = relative_path.parts

        if len(parts) < 2:
            return None

        folder_name = parts[0]

        # Try to extract YYYY-MM pattern from folder name
        match = re.match(r'^(\d{4}-\d{2})', folder_name)
        if match:
            snapshot_date = match.group(1)
            return (snapshot_date, folder_name)

        return None
    except (ValueError, IndexError):
        return None


def find_pdfs(root: Path) -> Iterable[Path]:
    return sorted(path for path in root.rglob("*.pdf") if path.is_file())


def get_completed_folders(conn: sqlite3.Connection) -> set[str]:
    """Get set of folder names that have been marked as import_complete."""
    cursor = conn.execute(
        "SELECT folder_name FROM snapshots WHERE import_complete = 1"
    )
    return {row[0] for row in cursor.fetchall()}


def find_pdfs_for_import(root: Path, conn: sqlite3.Connection) -> Iterable[Path]:
    """
    Find PDFs that need to be imported, skipping folders already marked as complete.
    This is much faster than find_pdfs() for subsequent scans.
    """
    completed_folders = get_completed_folders(conn)

    if completed_folders:
        logging.info(
            "Überspringe %d bereits importierte Ordner: %s",
            len(completed_folders),
            ", ".join(sorted(completed_folders))
        )

    for path in sorted(root.rglob("*.pdf")):
        if not path.is_file():
            continue

        # Get the folder name (first part of relative path)
        try:
            relative_path = path.relative_to(root)
            if len(relative_path.parts) >= 1:
                folder_name = relative_path.parts[0]
                if folder_name in completed_folders:
                    continue  # Skip files in completed folders
        except ValueError:
            pass

        yield path


def mark_folder_complete(conn: sqlite3.Connection, folder_name: str) -> bool:
    """Mark a folder as completely imported."""
    cursor = conn.execute(
        "UPDATE snapshots SET import_complete = 1 WHERE folder_name = ?",
        (folder_name,)
    )
    conn.commit()
    if cursor.rowcount > 0:
        logging.info("Ordner '%s' als vollständig importiert markiert", folder_name)
        return True
    return False


def mark_folder_incomplete(conn: sqlite3.Connection, folder_name: str) -> bool:
    """Mark a folder as incomplete (for re-scanning)."""
    cursor = conn.execute(
        "UPDATE snapshots SET import_complete = 0 WHERE folder_name = ?",
        (folder_name,)
    )
    conn.commit()
    return cursor.rowcount > 0


def extract_text(pdf_path: Path) -> str:
    """Extract text from PDF using pdfplumber for better structure recognition."""
    with pdfplumber.open(pdf_path) as pdf:
        return "\n".join(page.extract_text() or "" for page in pdf.pages)


def is_storno_document(text: str) -> bool:
    """
    Check if the document is a Stornobeleg (cancellation document).
    Returns True if it's a cancellation, False if it's a regular invoice.
    """
    # Keywords that indicate a cancellation document
    storno_keywords = [
        "Stornobeleg für",
        "Stornierung für",
        "Stornodatum:",
        "Stornogrund:",
    ]

    text_lower = text.lower()

    for keyword in storno_keywords:
        if keyword.lower() in text_lower:
            return True

    return False


def extract_customer(lines: list[str]) -> tuple[str, str, str, bool]:
    """Extract customer name, street, and city separately from invoice lines.

    Returns:
        tuple[str, str, str, bool]: (customer_name, customer_street, customer_city, address_incomplete)
    """
    # Try to find customer info by looking for address pattern with name followed by street and city
    # Format: Name, Street + Number, PLZ + City
    for idx in range(len(lines) - 2):
        name = lines[idx].strip()
        addr_line1 = lines[idx + 1].strip()
        addr_line2 = lines[idx + 2].strip() if idx + 2 < len(lines) else ""

        # Skip sender address and metadata lines
        skip_keywords = ["Datum:", "DECKBLATT", "Tel", "Fax", "Rechnung", "Kunden-Nr"]
        if any(keyword in name for keyword in skip_keywords):
            continue

        # Check if this looks like a valid address with PLZ + Stadt
        if (addr_line2 and len(addr_line2) >= 5 and addr_line2[:5].isdigit()
            and name and "Apotheke am Damm" not in name):
            street = addr_line1
            city = addr_line2
            return name, street, city, False  # Complete address

    # Second pass: Look for addresses WITHOUT "Apotheke am Damm" restriction
    # This handles B2B invoices (Apotheke, Praxis, etc.)
    for idx in range(len(lines) - 2):
        name = lines[idx].strip()
        addr_line1 = lines[idx + 1].strip()
        addr_line2 = lines[idx + 2].strip() if idx + 2 < len(lines) else ""

        skip_keywords = ["Datum:", "DECKBLATT", "Tel", "Fax", "Rechnung", "Kunden-Nr", "Apotheke am Damm"]
        if any(keyword in name for keyword in skip_keywords):
            continue

        # Valid address with PLZ + Stadt
        if addr_line2 and len(addr_line2) >= 5 and addr_line2[:5].isdigit() and name:
            street = addr_line1
            city = addr_line2
            return name, street, city, False  # Complete address (B2B)

    # Third pass: Look for incomplete addresses (only street, missing PLZ+Stadt)
    # This is more restrictive - only matches if the pattern really looks like a customer address
    for idx in range(len(lines) - 1):
        name = lines[idx].strip()
        addr_line1 = lines[idx + 1].strip()
        addr_line2 = lines[idx + 2].strip() if idx + 2 < len(lines) else ""

        skip_keywords = ["Datum:", "DECKBLATT", "Tel", "Fax", "Rechnung", "Kunden-Nr", "Apotheke am Damm", "Medikation", "Am Damm"]
        if any(keyword in name for keyword in skip_keywords):
            continue

        # Skip if addr_line1 looks like sender address
        if any(keyword in addr_line1 for keyword in skip_keywords):
            continue

        # Additional check: Skip if name looks like a street (e.g., "Am Damm 17")
        # Street names typically start with prepositions or have numbers
        if (name.startswith(("Am ", "An ", "Auf ", "In ", "Zur ", "Zum ")) or
            any(c.isdigit() for c in name)):
            continue

        # Check if this looks like a street address (contains letters and numbers)
        # and the name doesn't look like metadata
        if (name and len(name) > 3 and addr_line1 and
            # Street should have at least one letter and possibly a number
            any(c.isalpha() for c in addr_line1) and
            # But addr_line2 either doesn't exist or doesn't start with PLZ
            (not addr_line2 or (len(addr_line2) < 5 or not addr_line2[:5].isdigit()))):

            # Additional check: name should look like a person or business name
            # Avoid matching random text like "Menge PZN" etc.
            if not any(kw in name for kw in ["Menge", "PZN", "Artikel", "Pack", "MwSt", "Netto", "Summe"]):
                logging.warning(f"⚠️ Unvollständige Adresse für '{name}' (nur Straße '{addr_line1}') - verwende Standard-Stadt 'Alzey'")
                return name, addr_line1, "55232 Alzey", True  # Incomplete address!

    # Fallback: try old method with markers
    for idx, raw in enumerate(lines):
        token = raw.strip()
        if token in CUSTOMER_MARKERS:
            try:
                name = lines[idx + 1].strip()
                # Skip if next line looks like a date
                if "Datum:" in name or re.match(r'\d{2}\.\d{2}\.\d{4}', name):
                    continue
                addr_line1 = lines[idx + 2].strip()
                addr_line2 = lines[idx + 3].strip() if idx + 3 < len(lines) else ""
            except IndexError as exc:
                raise ValueError("Unvollständige Adresse im PDF") from exc
            if not name or not addr_line1:
                raise ValueError("Adresse enthält leere Zeilen")

            # If addr_line2 is missing or incomplete, use default
            address_incomplete = False
            if not addr_line2 or (len(addr_line2) >= 5 and not addr_line2[:5].isdigit()):
                logging.warning(f"⚠️ Unvollständige Adresse für '{name}' - verwende Standard-Stadt 'Alzey'")
                addr_line2 = "55232 Alzey"
                address_incomplete = True

            street = addr_line1
            city = addr_line2
            return name, street, city, address_incomplete
    raise ValueError("Keine Empfängeradresse gefunden")


def extract_total_amount_robust(text: str) -> int:
    """
    Extract total amount with fallback for table-style format.
    Tries the old inline pattern first, then table format.
    """
    # Try the old pattern first
    total_match = TOTAL_PATTERN.search(text)
    if total_match:
        amount_raw = total_match.group(1).replace(".", "").replace(",", ".")
        try:
            amount = Decimal(amount_raw)
            return int((amount * 100).to_integral_value())
        except InvalidOperation:
            pass

    # Try table-style format where headers and values are on separate lines
    lines = text.split('\n')
    for i, line in enumerate(lines):
        if 'Rechnungsbetrag' in line and i + 1 < len(lines):
            next_line = lines[i + 1]
            # Extract the last amount (should be the total)
            amounts = re.findall(r'([0-9]+(?:\.[0-9]{3})*,[0-9]{2})\s*€', next_line)
            if amounts:
                amount_raw = amounts[-1].replace(".", "").replace(",", ".")
                try:
                    amount = Decimal(amount_raw)
                    return int((amount * 100).to_integral_value())
                except InvalidOperation:
                    pass

    raise ValueError("Gesamtsumme nicht gefunden")


def parse_invoice(pdf_path: Path, storage_path: str) -> InvoiceRecord:
    text = extract_text(pdf_path)

    # Check if this is a Stornobeleg
    if is_storno_document(text):
        raise ValueError("Stornobeleg - wird nicht importiert")

    lines = [line for line in text.splitlines() if line.strip()]

    date_match = DATE_PATTERN.search(text)
    if not date_match:
        # Fallback: Try to extract date from filename
        # Format: "Beleg gespeichert X-Re 'XXXXX' vom 'DD.MM.YYYY' für Name.pdf"
        filename_date_pattern = re.compile(r"vom\s+'(\d{2}\.\d{2}\.\d{4})'")
        filename_match = filename_date_pattern.search(pdf_path.name)
        if filename_match:
            invoice_date = datetime.strptime(filename_match.group(1), "%d.%m.%Y").date().isoformat()
        else:
            raise ValueError("Rechnungsdatum nicht gefunden")
    else:
        # Get the date from whichever capture group matched
        invoice_date_str = date_match.group(1) or date_match.group(2)
        invoice_date = datetime.strptime(invoice_date_str, "%d.%m.%Y").date().isoformat()

    invoice_match = INVOICE_NO_PATTERN.search(text)
    invoice_number = invoice_match.group(1).strip() if invoice_match else None

    # Use robust extraction method
    amount_cents = extract_total_amount_robust(text)

    customer_name, customer_street, customer_city, address_incomplete = extract_customer(lines)

    # For backward compatibility, also set customer_address
    customer_address = f"{customer_street}, {customer_city}"

    return InvoiceRecord(
        file_path=storage_path,
        invoice_number=invoice_number,
        invoice_date=invoice_date,
        customer_name=customer_name,
        customer_address=customer_address,
        amount_cents=amount_cents,
        customer_street=customer_street,
        customer_city=customer_city,
        address_incomplete=address_incomplete,
    )


def get_or_create_snapshot(conn: sqlite3.Connection, snapshot_date: str, folder_name: str) -> int:
    """Get or create a snapshot and return its ID."""
    cursor = conn.execute(
        "SELECT id FROM snapshots WHERE snapshot_date = ?",
        (snapshot_date,)
    )
    row = cursor.fetchone()
    if row:
        return row[0]

    cursor = conn.execute(
        "INSERT INTO snapshots (snapshot_date, folder_name) VALUES (?, ?)",
        (snapshot_date, folder_name)
    )
    return cursor.lastrowid


def get_or_create_invoice(conn: sqlite3.Connection, record: InvoiceRecord) -> int:
    """Get or create an invoice and return its ID."""
    cursor = conn.execute(
        """
        SELECT id FROM invoices
        WHERE invoice_number = ? AND customer_name = ? AND amount_cents = ?
        """,
        (record.invoice_number, record.customer_name, record.amount_cents)
    )
    row = cursor.fetchone()
    if row:
        return row[0]

    cursor = conn.execute(
        """
        INSERT INTO invoices (
            invoice_number,
            customer_name,
            customer_address,
            invoice_date,
            amount_cents,
            customer_street,
            customer_city,
            address_incomplete,
            name_needs_review
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            record.invoice_number,
            record.customer_name,
            record.customer_address,
            record.invoice_date,
            record.amount_cents,
            record.customer_street,
            record.customer_city,
            1 if record.address_incomplete else 0,
            1 if record.name_needs_review else 0,
        )
    )
    return cursor.lastrowid


def link_invoice_to_snapshot(
    conn: sqlite3.Connection,
    invoice_id: int,
    snapshot_id: int,
    file_path: str
) -> bool:
    """Link an invoice to a snapshot. Returns True if new link created."""
    try:
        conn.execute(
            """
            INSERT INTO invoice_snapshots (invoice_id, snapshot_id, file_path)
            VALUES (?, ?, ?)
            """,
            (invoice_id, snapshot_id, file_path)
        )
        return True
    except sqlite3.IntegrityError:
        # Link already exists
        return False


# Known names that AI might not recognize correctly
# Maps lowercase first name -> "Herr" or "Frau"
KNOWN_NAMES_GENDER = {
    "hedemi": "Frau",
    "martun": "Herr",
    "ruqiyo": "Frau",
}


def lookup_known_gender(first_name: str) -> Optional[str]:
    """
    Check if a first name is in the known names lookup table.
    Returns "Herr" or "Frau" if found, None otherwise.
    """
    if not first_name:
        return None
    return KNOWN_NAMES_GENDER.get(first_name.lower().strip())


def extract_first_name(customer_name: str) -> Optional[str]:
    """
    Extract the first name from a customer name.
    """
    if not customer_name:
        return None

    name_clean = customer_name.strip()
    for title in ["Dr.", "Prof.", "Dipl.-Ing.", "Ing."]:
        name_clean = name_clean.replace(title, "").strip()

    parts = name_clean.split()
    if len(parts) >= 2:
        return parts[0].strip()

    if "," in name_clean:
        parts = name_clean.split(",")
        if len(parts) >= 2:
            return parts[1].strip().split()[0] if parts[1].strip() else None

    if len(parts) == 1:
        return parts[0].strip()

    return None


def determine_gender_via_ai(first_name: str) -> Optional[str]:
    """
    Use Nebius AI (Meta Llama 70B) to determine the gender based on first name.
    First checks the known names lookup table.
    """
    # First check known names lookup
    known = lookup_known_gender(first_name)
    if known:
        return known

    try:
        api_key = os.getenv('NEBIUS_API_KEY')
        if not api_key:
            return None

        url = "https://api.studio.nebius.com/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

        prompt = f"""Bestimme das Geschlecht des Vornamens "{first_name}".
Antworte NUR mit einem dieser Wörter:
- "männlich" wenn der Name typischerweise männlich ist
- "weiblich" wenn der Name typischerweise weiblich ist
- "unbekannt" wenn du dir nicht sicher bist

Antwort:"""

        payload = {
            "model": "meta-llama/Llama-3.3-70B-Instruct",
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1,
            "max_tokens": 10
        }

        response = requests.post(url, headers=headers, json=payload, timeout=10)
        response.raise_for_status()

        data = response.json()
        ai_response = data.get("choices", [{}])[0].get("message", {}).get("content", "").strip().lower()

        if "männlich" in ai_response or "male" in ai_response:
            return "Herr"
        elif "weiblich" in ai_response or "female" in ai_response:
            return "Frau"
        else:
            return None

    except Exception:
        return None


def determine_genders_batch_via_ai(first_names: list[str]) -> dict[str, Optional[str]]:
    """
    Use Nebius AI to determine genders for multiple first names in a single API call.
    First checks the known names lookup table for each name.
    Returns a dict mapping first_name -> "Herr"/"Frau"/None
    """
    if not first_names:
        return {}

    # First check known names lookup for all names
    result = {}
    names_to_query = []
    for name in first_names:
        known = lookup_known_gender(name)
        if known:
            result[name] = known
        else:
            names_to_query.append(name)

    # If all names were known, return early
    if not names_to_query:
        return result

    try:
        api_key = os.getenv('NEBIUS_API_KEY')
        if not api_key:
            for name in names_to_query:
                result[name] = None
            return result

        url = "https://api.studio.nebius.com/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

        names_list = ", ".join(f'"{name}"' for name in names_to_query)
        prompt = f"""Bestimme das Geschlecht für folgende Vornamen: {names_list}

Antworte im JSON-Format als Array, für jeden Namen in der gleichen Reihenfolge:
- "m" für männlich
- "w" für weiblich
- "u" für unbekannt

Beispiel für ["Hans", "Maria", "Kim"]:
["m", "w", "u"]

Antwort (nur das JSON-Array, keine Erklärung):"""

        payload = {
            "model": "meta-llama/Llama-3.3-70B-Instruct",
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1,
            "max_tokens": len(names_to_query) * 5 + 20
        }

        response = requests.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()

        data = response.json()
        ai_response = data.get("choices", [{}])[0].get("message", {}).get("content", "").strip()

        # Parse JSON response
        try:
            # Find JSON array in response
            start = ai_response.find('[')
            end = ai_response.rfind(']') + 1
            if start >= 0 and end > start:
                genders = json.loads(ai_response[start:end])
            else:
                for name in names_to_query:
                    result[name] = None
                return result
        except json.JSONDecodeError:
            for name in names_to_query:
                result[name] = None
            return result

        # Map AI results to names_to_query
        for i, name in enumerate(names_to_query):
            if i < len(genders):
                g = genders[i].lower() if isinstance(genders[i], str) else ""
                if g == "m" or "männlich" in g or "male" in g:
                    result[name] = "Herr"
                elif g == "w" or "weiblich" in g or "female" in g:
                    result[name] = "Frau"
                else:
                    result[name] = None
            else:
                result[name] = None

        return result

    except Exception as e:
        logging.error(f"Batch gender determination failed: {e}")
        for name in names_to_query:
            result[name] = None
        return result


def determine_salutation_for_customer(customer_name: str) -> Optional[str]:
    """
    Determine salutation for a customer by extracting first name and using AI.
    """
    first_name = extract_first_name(customer_name)
    if not first_name:
        return None
    return determine_gender_via_ai(first_name)


def validate_customer_names_batch_via_ai(customer_names: list[str]) -> dict[str, bool]:
    """
    Use Nebius AI to validate whether customer names are plausible person names.
    Returns a dict mapping customer_name -> True (valid) / False (invalid/suspicious)

    Examples of invalid names:
    - "Herr" (just a title)
    - "Bischheim", "Marnheim" (place names)
    - "b7 Dialyse" (institution/company name)
    """
    if not customer_names:
        return {}

    result = {}

    # Pre-filter: Single words (no spaces) are automatically invalid
    # A valid customer name should have at least first + last name
    names_for_ai = []
    for name in customer_names:
        name_stripped = name.strip()
        if ' ' not in name_stripped:
            # Single word = invalid (could be place name, title, etc.)
            result[name] = False
        else:
            names_for_ai.append(name)

    # If all names were single words, return early
    if not names_for_ai:
        return result

    try:
        api_key = os.getenv('NEBIUS_API_KEY')
        if not api_key:
            # If no API key, assume remaining names are valid (don't flag them)
            for name in names_for_ai:
                result[name] = True
            return result

        url = "https://api.studio.nebius.com/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

        names_list = ", ".join(f'"{name}"' for name in names_for_ai)
        prompt = f"""Prüfe ob folgende Einträge gültige Kunden-Bezeichnungen für eine deutsche Apotheken-Rechnungssoftware sind: {names_list}

GÜLTIG (true):
- Personennamen mit Vor- UND Nachname (z.B. "Hans Müller", "Christel Bader")
- Ehepaare (z.B. "Ulrike u. Rainer Ewald")
- Arztpraxen (z.B. "Praxis Dr. Müller", "Praxis Orthopädie Dr. Reiners")
- Medizinische Einrichtungen (z.B. "MVZ DaVita Alzey GmbH")
- Praxisbedarf (z.B. "Praxisbedarf Dr. Filippi")

IMMER UNGÜLTIG (false) - diese Fälle sind NIEMALS gültig:
1. NUR "Herr" oder NUR "Frau" ohne weiteren Namen → IMMER false
2. Einzelne Ortsnamen (-heim, -berg, -dorf): "Bischheim", "Marnheim" → IMMER false
3. Kryptische Codes: "Dialyse B7", "b7 Dialyse", "Station 3" → IMMER false

Beispiel: ["Hans Müller", "Herr", "Bischheim", "Praxis Dr. Müller", "Dialyse B7"]
Antwort: [true, false, false, true, false]

"Herr" alleine = false! "Frau" alleine = false!

Antworte NUR mit JSON-Array [true/false, ...]:"""

        payload = {
            "model": "meta-llama/Llama-3.3-70B-Instruct",
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1,
            "max_tokens": len(names_for_ai) * 10 + 20
        }

        response = requests.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()

        data = response.json()
        ai_response = data.get("choices", [{}])[0].get("message", {}).get("content", "").strip()

        # Parse JSON response
        try:
            # Find JSON array in response
            start = ai_response.find('[')
            end = ai_response.rfind(']') + 1
            if start >= 0 and end > start:
                validities = json.loads(ai_response[start:end])
            else:
                # If parsing fails, assume remaining names are valid
                for name in names_for_ai:
                    result[name] = True
                return result
        except json.JSONDecodeError:
            for name in names_for_ai:
                result[name] = True
            return result

        # Map AI results to names_for_ai
        for i, name in enumerate(names_for_ai):
            if i < len(validities):
                result[name] = bool(validities[i])
            else:
                result[name] = True  # Assume valid if missing

        return result

    except Exception as e:
        logging.error(f"Batch name validation failed: {e}")
        # On error, assume remaining names are valid (don't flag them)
        for name in names_for_ai:
            result[name] = True
        return result


def normalize_string(text: str) -> str:
    """
    Normalize a string for comparison by removing common variations.
    """
    if not text:
        return ""

    # Convert to lowercase
    normalized = text.lower().strip()

    # Remove common punctuation
    normalized = normalized.replace(".", "").replace(",", "").replace("-", " ")

    # Replace common abbreviations
    replacements = {
        "str": "strasse",
        "straße": "strasse",
        "strasse": "strasse",
        "str.": "strasse",
    }

    for old, new in replacements.items():
        normalized = normalized.replace(old, new)

    # Remove extra whitespace
    normalized = " ".join(normalized.split())

    return normalized


def highlight_diff(text1: str, text2: str) -> tuple[str, str]:
    """
    Compare two strings and return HTML with highlighted differences.

    Returns:
        Tuple of (text1_highlighted, text2_highlighted)
    """
    import difflib
    import html

    if not text1 or not text2:
        return (html.escape(text1 or ""), html.escape(text2 or ""))

    # Use SequenceMatcher to find differences
    matcher = difflib.SequenceMatcher(None, text1, text2)

    result1 = []
    result2 = []

    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        text1_part = html.escape(text1[i1:i2])
        text2_part = html.escape(text2[j1:j2])

        if tag == 'equal':
            result1.append(text1_part)
            result2.append(text2_part)
        elif tag == 'replace':
            result1.append(f'<mark class="diff-change">{text1_part}</mark>')
            result2.append(f'<mark class="diff-change">{text2_part}</mark>')
        elif tag == 'delete':
            result1.append(f'<mark class="diff-delete">{text1_part}</mark>')
        elif tag == 'insert':
            result2.append(f'<mark class="diff-insert">{text2_part}</mark>')

    return (''.join(result1), ''.join(result2))


def find_similar_customers(
    conn: sqlite3.Connection,
    customer_name: str,
    customer_street: str,
    customer_city: str,
    similarity_threshold: int = 80
) -> list[dict]:
    """
    Find similar customers in the database using fuzzy string matching.

    Args:
        conn: Database connection
        customer_name: Name to search for
        customer_street: Street to search for
        customer_city: City to search for
        similarity_threshold: Minimum similarity score (0-100) to consider a match

    Returns:
        List of dictionaries with similar customer data and similarity scores
    """
    # Get all existing customers from invoices
    cursor = conn.execute("""
        SELECT DISTINCT customer_name, customer_street, customer_city
        FROM invoices
        WHERE customer_name IS NOT NULL
    """)

    existing_customers = cursor.fetchall()
    similar_customers = []

    # Normalize input
    norm_name = normalize_string(customer_name)
    norm_street = normalize_string(customer_street or "")
    norm_city = normalize_string(customer_city or "")

    for existing_name, existing_street, existing_city in existing_customers:
        # Normalize existing data
        norm_existing_name = normalize_string(existing_name)
        norm_existing_street = normalize_string(existing_street or "")
        norm_existing_city = normalize_string(existing_city or "")

        # Calculate similarity scores for each field
        name_score = fuzz.ratio(norm_name, norm_existing_name)
        street_score = fuzz.ratio(norm_street, norm_existing_street) if norm_street and norm_existing_street else 0
        city_score = fuzz.ratio(norm_city, norm_existing_city) if norm_city and norm_existing_city else 0

        # Calculate weighted average (name is most important)
        # Name: 50%, Street: 30%, City: 20%
        if norm_street and norm_city:
            overall_score = (name_score * 0.5) + (street_score * 0.3) + (city_score * 0.2)
        elif norm_street:
            overall_score = (name_score * 0.7) + (street_score * 0.3)
        else:
            overall_score = name_score

        # If overall similarity is above threshold, add to results
        if overall_score >= similarity_threshold:
            # Count how many invoices this customer has
            invoice_count = conn.execute(
                "SELECT COUNT(*) FROM invoices WHERE customer_name = ?",
                (existing_name,)
            ).fetchone()[0]

            # Calculate diff highlights for name, street, and city
            name_new_hl, name_old_hl = highlight_diff(customer_name, existing_name)
            street_new_hl, street_old_hl = highlight_diff(customer_street or "", existing_street or "")
            city_new_hl, city_old_hl = highlight_diff(customer_city or "", existing_city or "")

            similar_customers.append({
                "customer_name": existing_name,
                "customer_street": existing_street,
                "customer_city": existing_city,
                "customer_name_highlighted": name_old_hl,
                "customer_street_highlighted": street_old_hl,
                "customer_city_highlighted": city_old_hl,
                "new_name_highlighted": name_new_hl,
                "new_street_highlighted": street_new_hl,
                "new_city_highlighted": city_new_hl,
                "similarity_score": round(overall_score, 1),
                "name_score": round(name_score, 1),
                "street_score": round(street_score, 1),
                "city_score": round(city_score, 1),
                "invoice_count": invoice_count
            })

    # Sort by similarity score (highest first)
    similar_customers.sort(key=lambda x: x["similarity_score"], reverse=True)

    return similar_customers


def merge_customers(
    conn: sqlite3.Connection,
    old_customer_name: str,
    old_customer_street: str,
    old_customer_city: str,
    new_customer_name: str,
    new_customer_street: str,
    new_customer_city: str
) -> int:
    """
    Merge customer data by updating all invoices from old customer to new customer.

    Args:
        conn: Database connection
        old_customer_name: Name of the customer to be replaced
        old_customer_street: Street of the customer to be replaced
        old_customer_city: City of the customer to be replaced
        new_customer_name: Name to use for all invoices
        new_customer_street: Street to use for all invoices
        new_customer_city: City to use for all invoices

    Returns:
        Number of invoices updated
    """
    # Count invoices to be updated
    count = conn.execute(
        "SELECT COUNT(*) FROM invoices WHERE customer_name = ?",
        (old_customer_name,)
    ).fetchone()[0]

    if count == 0:
        return 0

    # Update all invoices with the old customer name to use the new customer data
    conn.execute(
        """
        UPDATE invoices
        SET customer_name = ?,
            customer_street = ?,
            customer_city = ?,
            customer_address = ?
        WHERE customer_name = ?
        """,
        (
            new_customer_name,
            new_customer_street,
            new_customer_city,
            f"{new_customer_street}, {new_customer_city}",
            old_customer_name,
        )
    )

    # Check if customer_details exists for old customer
    old_details = conn.execute(
        "SELECT * FROM customer_details WHERE customer_name = ?",
        (old_customer_name,)
    ).fetchone()

    # Check if customer_details exists for new customer
    new_details = conn.execute(
        "SELECT * FROM customer_details WHERE customer_name = ?",
        (new_customer_name,)
    ).fetchone()

    # Merge customer_details
    if old_details and not new_details:
        # If only old customer has details, rename them to new customer
        conn.execute(
            """
            UPDATE customer_details
            SET customer_name = ?,
                updated_at = datetime('now', 'localtime')
            WHERE customer_name = ?
            """,
            (new_customer_name, old_customer_name)
        )
    elif old_details and new_details:
        # Both have details - keep new customer's details, delete old
        conn.execute(
            "DELETE FROM customer_details WHERE customer_name = ?",
            (old_customer_name,)
        )

    # Log the merge operation for all affected invoices
    invoice_ids = conn.execute(
        "SELECT id FROM invoices WHERE customer_name = ?",
        (new_customer_name,)
    ).fetchall()

    for (invoice_id,) in invoice_ids:
        log_invoice_event(
            conn,
            invoice_id,
            "CUSTOMER_MERGED",
            {
                "old_customer_name": old_customer_name,
                "old_customer_street": old_customer_street,
                "old_customer_city": old_customer_city,
                "new_customer_name": new_customer_name,
                "new_customer_street": new_customer_street,
                "new_customer_city": new_customer_city,
            }
        )

    conn.commit()
    logging.info(
        "Kunden zusammengeführt: '%s' -> '%s' (%d Rechnungen aktualisiert)",
        old_customer_name,
        new_customer_name,
        count
    )

    return count


def update_customer_data_for_all_invoices(
    conn: sqlite3.Connection,
    old_name: str,
    old_street: str,
    old_city: str,
    new_name: str,
    new_street: str,
    new_city: str
) -> int:
    """
    Update customer data for all invoices matching the old data.

    Args:
        conn: Database connection
        old_name: Old customer name
        old_street: Old customer street
        old_city: Old customer city
        new_name: New customer name
        new_street: New customer street
        new_city: New customer city

    Returns:
        Number of invoices updated
    """
    new_address = f"{new_street}, {new_city}"

    cursor = conn.execute(
        """
        UPDATE invoices
        SET customer_name = ?,
            customer_street = ?,
            customer_city = ?,
            customer_address = ?
        WHERE customer_name = ?
          AND customer_street = ?
          AND customer_city = ?
        """,
        (new_name, new_street, new_city, new_address,
         old_name, old_street, old_city)
    )

    updated_count = cursor.rowcount
    conn.commit()

    logging.info(
        "Kundendaten aktualisiert: '%s' -> '%s' (%d Rechnungen)",
        old_name, new_name, updated_count
    )

    return updated_count


def resolve_pending_import(
    conn: sqlite3.Connection,
    pending_import_id: int,
    action: str,
    selected_customer: Optional[dict] = None,
    use_new_data: bool = False
) -> bool:
    """
    Resolve a pending import by either creating a new customer or merging with existing.

    Args:
        conn: Database connection
        pending_import_id: ID of the pending import to resolve
        action: Either 'create_new' or 'merge_with_existing'
        selected_customer: If action is 'merge_with_existing', the customer to merge with
        use_new_data: If True and action is 'merge_with_existing', update all existing
                     invoices with the new customer data from pending import

    Returns:
        True if resolved successfully, False otherwise
    """
    # Get pending import data
    pending = conn.execute(
        """
        SELECT file_path, invoice_number, invoice_date, customer_name,
               customer_street, customer_city, amount_cents, snapshot_date, snapshot_id
        FROM pending_imports
        WHERE id = ? AND status = 'pending'
        """,
        (pending_import_id,)
    ).fetchone()

    if not pending:
        logging.error("Pending import %d nicht gefunden oder bereits resolved", pending_import_id)
        return False

    (file_path, invoice_number, invoice_date, customer_name, customer_street,
     customer_city, amount_cents, snapshot_date, snapshot_id) = pending

    if action == 'create_new':
        # Create new invoice with the data from pending import
        customer_address = f"{customer_street}, {customer_city}"
        record = InvoiceRecord(
            file_path=file_path,
            invoice_number=invoice_number,
            invoice_date=invoice_date,
            customer_name=customer_name,
            customer_address=customer_address,
            amount_cents=amount_cents,
            customer_street=customer_street,
            customer_city=customer_city
        )

        invoice_id = get_or_create_invoice(conn, record)
        link_invoice_to_snapshot(conn, invoice_id, snapshot_id, file_path)

        # Log import event
        log_invoice_event(
            conn,
            invoice_id,
            "IMPORT",
            {
                "snapshot_date": snapshot_date,
                "file_path": file_path,
                "amount": amount_cents / 100,
                "resolved_from_pending": True
            }
        )

        logging.info(
            "Pending import resolved - Neuer Kunde erstellt: %s",
            customer_name
        )

    elif action == 'merge_with_existing':
        if not selected_customer:
            logging.error("Kein Kunde für Merge ausgewählt")
            return False

        # Determine which customer data to use
        if use_new_data:
            # Use new data from pending import and update all existing invoices
            final_name = customer_name
            final_street = customer_street
            final_city = customer_city

            # Get old customer data
            old_name = selected_customer.get('customer_name')
            old_street = selected_customer.get('customer_street')
            old_city = selected_customer.get('customer_city')

            # Update all existing invoices with new data
            updated_count = update_customer_data_for_all_invoices(
                conn,
                old_name, old_street, old_city,
                final_name, final_street, final_city
            )

            logging.info(
                "Kundendaten-Version NEU verwendet: %d Rechnungen aktualisiert",
                updated_count
            )
        else:
            # Use existing customer's data
            final_name = selected_customer.get('customer_name')
            final_street = selected_customer.get('customer_street')
            final_city = selected_customer.get('customer_city')

        customer_address = f"{final_street}, {final_city}"
        record = InvoiceRecord(
            file_path=file_path,
            invoice_number=invoice_number,
            invoice_date=invoice_date,
            customer_name=final_name,
            customer_address=customer_address,
            amount_cents=amount_cents,
            customer_street=final_street,
            customer_city=final_city
        )

        invoice_id = get_or_create_invoice(conn, record)
        link_invoice_to_snapshot(conn, invoice_id, snapshot_id, file_path)

        # Log import event with merge info
        log_invoice_event(
            conn,
            invoice_id,
            "IMPORT",
            {
                "snapshot_date": snapshot_date,
                "file_path": file_path,
                "amount": amount_cents / 100,
                "resolved_from_pending": True,
                "merged_with_existing": final_name,
                "original_name": customer_name,
                "used_new_data": use_new_data
            }
        )

        logging.info(
            "Pending import resolved - Zusammengeführt: '%s' -> '%s' (Neue Daten: %s)",
            customer_name,
            final_name,
            "Ja" if use_new_data else "Nein"
        )

    else:
        logging.error("Unbekannte Aktion: %s", action)
        return False

    # Mark pending import as resolved
    conn.execute(
        """
        UPDATE pending_imports
        SET status = 'resolved',
            resolved_at = datetime('now', 'localtime')
        WHERE id = ?
        """,
        (pending_import_id,)
    )

    conn.commit()
    return True


def log_invoice_event(
    conn: sqlite3.Connection,
    invoice_id: int,
    event_type: str,
    metadata: Optional[dict] = None
) -> None:
    """
    Log an event for an invoice in the invoice_history table.

    Args:
        conn: Database connection
        invoice_id: ID of the invoice
        event_type: Type of event (e.g., 'IMPORT', 'EMAIL_SENT', 'REMINDER_CREATED', 'PAYMENT_RECEIVED')
        metadata: Optional dictionary with additional event details (will be stored as JSON)
    """
    import json
    metadata_json = json.dumps(metadata) if metadata else None
    conn.execute(
        """
        INSERT INTO invoice_history (invoice_id, event_type, metadata)
        VALUES (?, ?, ?)
        """,
        (invoice_id, event_type, metadata_json)
    )


def detect_and_log_payments(conn: sqlite3.Connection, current_snapshot_date: str) -> int:
    """
    Detect invoices that were paid (disappeared from the latest snapshot)
    and log PAYMENT_RECEIVED events in the invoice history.

    Compares the current snapshot with the previous one to find invoices
    that are no longer present (indicating payment).

    Args:
        conn: Database connection
        current_snapshot_date: The snapshot date just processed (YYYY-MM format)

    Returns:
        Number of newly detected payments
    """
    # Get current snapshot ID
    current_snapshot = conn.execute(
        "SELECT id FROM snapshots WHERE snapshot_date = ? ORDER BY id DESC LIMIT 1",
        (current_snapshot_date,)
    ).fetchone()

    if not current_snapshot:
        return 0

    current_snapshot_id = current_snapshot[0]

    # Get previous snapshot
    previous_snapshot = conn.execute(
        """
        SELECT id, snapshot_date
        FROM snapshots
        WHERE snapshot_date < ?
        ORDER BY snapshot_date DESC
        LIMIT 1
        """,
        (current_snapshot_date,)
    ).fetchone()

    if not previous_snapshot:
        # No previous snapshot to compare with
        return 0

    previous_snapshot_id = previous_snapshot[0]
    previous_snapshot_date = previous_snapshot[1]

    # Find invoices that were in previous snapshot but NOT in current snapshot
    # and haven't been logged as paid yet
    paid_invoices = conn.execute(
        """
        SELECT DISTINCT i.id, i.invoice_number, i.customer_name, i.amount_cents
        FROM invoices i
        JOIN invoice_snapshots isnap_prev ON i.id = isnap_prev.invoice_id
        WHERE isnap_prev.snapshot_id = ?
          AND NOT EXISTS (
              SELECT 1 FROM invoice_snapshots isnap_curr
              WHERE isnap_curr.invoice_id = i.id
                AND isnap_curr.snapshot_id = ?
          )
          AND NOT EXISTS (
              SELECT 1 FROM invoice_history ih
              WHERE ih.invoice_id = i.id
                AND ih.event_type = 'PAYMENT_RECEIVED'
          )
        """,
        (previous_snapshot_id, current_snapshot_id)
    ).fetchall()

    # Log payment event for each paid invoice
    count = 0
    for invoice in paid_invoices:
        invoice_id, invoice_number, customer_name, amount_cents = invoice
        log_invoice_event(
            conn,
            invoice_id,
            "PAYMENT_RECEIVED",
            {
                "amount": amount_cents / 100,
                "last_seen_snapshot": previous_snapshot_date,
                "paid_detected_at_snapshot": current_snapshot_date,
                "invoice_number": invoice_number or "ohne Nummer",
                "customer_name": customer_name
            }
        )
        count += 1
        logging.info(
            "Zahlung erkannt: %s (%s) – %.2f € (zuletzt gesehen: %s)",
            customer_name,
            invoice_number or "ohne Nr.",
            amount_cents / 100,
            previous_snapshot_date
        )

    return count


def storage_key(pdf_path: Path, root: Path) -> str:
    # Store paths relative to DATA_DIR (get_data_dir()) for consistency with serve_pdf route
    # Normalize Unicode to NFC for cross-platform compatibility (macOS uses NFD, Windows uses NFC)
    data_dir = get_data_dir()
    try:
        relative = str(pdf_path.relative_to(data_dir))
    except ValueError:
        # Fallback: if not under DATA_DIR, try relative to root
        try:
            relative = str(pdf_path.relative_to(root))
        except ValueError:
            relative = str(pdf_path.resolve())
    return unicodedata.normalize('NFC', relative)


def process_pdf_file(conn: sqlite3.Connection, pdf_path: Path, root: Path) -> bool:
    """
    Process a PDF file and add it to the database with snapshot tracking.
    Returns True if a new invoice-snapshot link was created.

    If similar customers are found, the import is saved to pending_imports
    for user review and False is returned.
    """
    # Extract snapshot info from path
    snapshot_info = extract_snapshot_from_path(pdf_path, root)
    if not snapshot_info:
        logging.warning("Überspringe %s - nicht in einem Monatsordner (Format: YYYY-MM-...)", pdf_path)
        return False

    snapshot_date, folder_name = snapshot_info

    # Get storage key for file path
    key = storage_key(pdf_path, root)

    # Check if this file is already in pending_imports
    existing_pending = conn.execute(
        "SELECT id FROM pending_imports WHERE file_path = ?",
        (key,)
    ).fetchone()

    if existing_pending:
        logging.debug("PDF bereits in pending_imports: %s", pdf_path)
        return False

    # Parse invoice data
    record = parse_invoice(pdf_path, key)

    # Get or create snapshot
    snapshot_id = get_or_create_snapshot(conn, snapshot_date, folder_name)

    # Check if this customer_name was previously corrected (has custom_name set)
    # If so, we know this "bad" name belongs to an existing customer and skip fuzzy matching
    name_was_corrected = conn.execute(
        """
        SELECT custom_name FROM customer_details
        WHERE customer_name = ? AND custom_name IS NOT NULL AND custom_name != ''
        """,
        (record.customer_name,)
    ).fetchone()

    if name_was_corrected:
        logging.info(
            "Name '%s' wurde früher zu '%s' korrigiert - automatische Zuordnung",
            record.customer_name,
            name_was_corrected[0]
        )

    # First, check if customer already exists EXACTLY (fast DB query)
    exact_match = conn.execute(
        """
        SELECT COUNT(*) FROM invoices
        WHERE customer_name = ? AND customer_street = ? AND customer_city = ?
        """,
        (record.customer_name, record.customer_street, record.customer_city)
    ).fetchone()[0]

    # Only do expensive fuzzy matching if no exact match exists AND name wasn't corrected before
    similar_customers = []
    if exact_match == 0 and not name_was_corrected:
        # Check for similar customers BEFORE creating invoice
        similar_customers = find_similar_customers(
            conn,
            record.customer_name,
            record.customer_street,
            record.customer_city,
            similarity_threshold=80
        )

    # If similar customers found, save to pending_imports for user review
    if similar_customers:
        try:
            conn.execute(
                """
                INSERT INTO pending_imports (
                    file_path,
                    invoice_number,
                    invoice_date,
                    customer_name,
                    customer_street,
                    customer_city,
                    amount_cents,
                    snapshot_date,
                    snapshot_id,
                    similar_customers,
                    status
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
                """,
                (
                    key,
                    record.invoice_number,
                    record.invoice_date,
                    record.customer_name,
                    record.customer_street,
                    record.customer_city,
                    record.amount_cents,
                    snapshot_date,
                    snapshot_id,
                    json.dumps(similar_customers),
                )
            )
            conn.commit()
            logging.info(
                "Ähnlicher Kunde gefunden für '%s' - Import wartet auf Review (Score: %.1f%%)",
                record.customer_name,
                similar_customers[0]["similarity_score"]
            )
            return False
        except sqlite3.IntegrityError:
            # File already in pending_imports
            logging.debug("PDF bereits in pending_imports: %s", pdf_path)
            return False

    # No similar customers found - proceed with normal import
    # Get or create invoice
    invoice_id = get_or_create_invoice(conn, record)

    # Link invoice to snapshot
    is_new_link = link_invoice_to_snapshot(conn, invoice_id, snapshot_id, key)

    # Check if customer has salutation in customer_details
    customer_check = conn.execute(
        "SELECT salutation FROM customer_details WHERE customer_name = ?",
        (record.customer_name,)
    ).fetchone()

    # If customer doesn't exist or has no salutation, try to determine it via AI
    if not customer_check or not customer_check[0]:
        salutation = determine_salutation_for_customer(record.customer_name)
        if salutation:
            conn.execute(
                """
                INSERT INTO customer_details (customer_name, salutation, updated_at)
                VALUES (?, ?, datetime('now', 'localtime'))
                ON CONFLICT(customer_name) DO UPDATE SET
                    salutation = excluded.salutation,
                    updated_at = datetime('now', 'localtime')
                """,
                (record.customer_name, salutation)
            )
            logging.info("Anrede für %s automatisch ermittelt: %s", record.customer_name, salutation)

    # Log import event for new invoices
    if is_new_link:
        log_invoice_event(
            conn,
            invoice_id,
            "IMPORT",
            {
                "snapshot_date": snapshot_date,
                "file_path": key,
                "amount": record.amount_cents / 100
            }
        )

    conn.commit()

    if is_new_link:
        logging.info(
            "Rechnung verknüpft: %s (%s) – %.2f € [%s]",
            record.customer_name,
            record.invoice_date,
            record.amount_cents / 100,
            snapshot_date,
        )
    else:
        logging.debug("Rechnung bereits in Snapshot %s: %s", snapshot_date, key)

    return is_new_link


class InvoiceEventHandler(FileSystemEventHandler):
    def __init__(self, root: Path, db_path: Path, settle_seconds: float) -> None:
        super().__init__()
        self.root = root
        self.db_path = db_path
        self.settle_seconds = max(0.0, settle_seconds)

    def on_created(self, event) -> None:  # type: ignore[override]
        self._handle_event(event, getattr(event, "src_path", None), event.is_directory)

    def on_moved(self, event) -> None:  # type: ignore[override]
        self._handle_event(event, getattr(event, "dest_path", None), event.is_directory)

    def _handle_event(self, event, path_str: Optional[str], is_directory: bool) -> None:
        if is_directory or not path_str:
            return
        path = Path(path_str)
        if path.suffix.lower() != ".pdf":
            return
        logging.info("Neue Datei erkannt: %s", path)
        if self.settle_seconds:
            time.sleep(self.settle_seconds)
        with sqlite3.connect(self.db_path) as conn:
            init_db(conn)
            try:
                process_pdf_file(conn, path, self.root)
            except Exception as exc:  # pragma: no cover - watcher runtime
                logging.error("Fehler beim Verarbeiten von %s: %s", path, exc)


def run_watcher(root: Path, db_path: Path, settle_seconds: float) -> None:
    if FileSystemEventHandler is None:
        raise SystemExit("watchdog ist nicht installiert – bitte 'pip install watchdog' ausführen.")
    handler = InvoiceEventHandler(root, db_path, settle_seconds)
    observer = start_observer(handler, root)
    logging.info("Watcher gestartet. Mit Strg+C beenden.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Beende Watcher...")
    finally:
        observer.stop()
        observer.join()


def start_observer(handler, root: Path):
    observer_classes = []
    if PollingObserver is not None:
        observer_classes.append(PollingObserver)
    if Observer is not None:
        observer_classes.append(Observer)
    if not observer_classes:
        raise SystemExit("Kein verfügbarer Watchdog-Observer gefunden.")

    last_error: Optional[Exception] = None
    for cls in observer_classes:
        observer = cls()
        try:
            observer.schedule(handler, str(root), recursive=True)
            observer.start()
            if cls.__name__ != "Observer":
                logging.info("Nutze %s für Dateibeobachtung.", cls.__name__)
            return observer
        except Exception as exc:  # pragma: no cover - platform-specific
            last_error = exc
            logging.warning("Konnte %s nicht starten: %s", cls.__name__, exc)
    raise SystemExit(f"Watcher konnte nicht gestartet werden: {last_error}")


def main() -> None:
    args = parse_args()
    configure_logging(args.verbose)

    root = args.root.resolve()
    if not root.exists():
        raise SystemExit(f"Das Verzeichnis {root} wurde nicht gefunden.")

    db_path = args.database.resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    pdf_files = list(find_pdfs(root))
    if not pdf_files:
        logging.warning("Keine PDF-Dateien unter %s gefunden.", root)
        return

    new_entries = 0
    skipped_storno = 0
    with sqlite3.connect(db_path) as conn:
        init_db(conn)
        for pdf_path in pdf_files:
            try:
                if process_pdf_file(conn, pdf_path, root):
                    new_entries += 1
            except ValueError as exc:
                # Check if this is a Stornobeleg
                if "Stornobeleg" in str(exc):
                    logging.debug("Überspringe Stornobeleg: %s", pdf_path.name)
                    skipped_storno += 1
                else:
                    logging.error("Kann %s nicht verarbeiten: %s", pdf_path, exc)
                continue
            except Exception as exc:
                logging.error("Kann %s nicht verarbeiten: %s", pdf_path, exc)
                continue
        conn.commit()

    logging.info("Fertig. %s neue Rechnungen gespeichert.", new_entries)
    if skipped_storno > 0:
        logging.info("%s Stornobelege übersprungen.", skipped_storno)

    if args.watch:
        run_watcher(root, db_path, args.settle_seconds)


if __name__ == "__main__":
    main()
