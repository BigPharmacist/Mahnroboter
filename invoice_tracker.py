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

from pypdf import PdfReader
from dotenv import load_dotenv
import requests

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
DEFAULT_INVOICE_DIR = BASE_DIR / "Rechnungen"
DEFAULT_DB_PATH = BASE_DIR / "invoice_data.db"

CUSTOMER_MARKERS = {"Herr", "Herrn", "Frau", "Familie"}
DATE_PATTERN = re.compile(r"(?:Datum:\s*(\d{2}\.\d{2}\.\d{4})|(\d{2}\.\d{2}\.\d{4})\s*Datum:)")
INVOICE_NO_PATTERN = re.compile(r"(?:Rechnungs-Nr|Deckblatt-Nr):\s*([\w\-\/]+)")
TOTAL_PATTERN = re.compile(r"(?:Gesamtsumme|Zwischensumme|Rechnungsbetrag).*?([0-9]{1,3}(?:\.[0-9]{3})*,[0-9]{2})\s*€")


@dataclass
class InvoiceRecord:
    file_path: str
    invoice_number: Optional[str]
    invoice_date: str  # ISO 8601 (YYYY-MM-DD)
    customer_name: str
    customer_address: str
    amount_cents: int


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
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")


def init_db(conn: sqlite3.Connection) -> None:
    # Create snapshots table
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_date TEXT NOT NULL UNIQUE,
            folder_name TEXT NOT NULL,
            scanned_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
        )
        """
    )

    # Create invoices table
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS invoices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_number TEXT,
            customer_name TEXT NOT NULL,
            customer_address TEXT NOT NULL,
            invoice_date TEXT NOT NULL,
            amount_cents INTEGER NOT NULL,
            currency TEXT NOT NULL DEFAULT 'EUR',
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

    # Add uncollectible column to invoices if it doesn't exist (for existing databases)
    try:
        conn.execute("ALTER TABLE invoices ADD COLUMN uncollectible INTEGER DEFAULT 0")
        conn.commit()
    except sqlite3.OperationalError:
        # Column already exists, that's fine
        pass

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


def extract_text(pdf_path: Path) -> str:
    reader = PdfReader(pdf_path)
    return "\n".join(page.extract_text() or "" for page in reader.pages)


def extract_customer(lines: list[str]) -> tuple[str, str]:
    # Try to find customer info by looking for address pattern with name followed by street and city
    # Format: Name, Street + Number, PLZ + City
    for idx in range(len(lines) - 2):
        name = lines[idx].strip()
        addr_line1 = lines[idx + 1].strip()
        addr_line2 = lines[idx + 2].strip()

        # Check if this looks like a valid address:
        # - addr_line2 should start with 5 digits (PLZ)
        # - name should not contain "Apotheke" (sender address)
        # - Skip if name contains "Datum:" or other keywords
        if (addr_line2 and len(addr_line2) >= 5 and addr_line2[:5].isdigit()
            and name and "Apotheke" not in name and "Datum:" not in name
            and "DECKBLATT" not in name and "Tel" not in name and "Fax" not in name):
            address = f"{addr_line1}, {addr_line2}"
            return name, address

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
                addr_line2 = lines[idx + 3].strip()
            except IndexError as exc:
                raise ValueError("Unvollständige Adresse im PDF") from exc
            if not name or not addr_line1 or not addr_line2:
                raise ValueError("Adresse enthält leere Zeilen")
            address = f"{addr_line1}, {addr_line2}"
            return name, address
    raise ValueError("Keine Empfängeradresse gefunden")


def parse_invoice(pdf_path: Path, storage_path: str) -> InvoiceRecord:
    text = extract_text(pdf_path)
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

    total_match = TOTAL_PATTERN.search(text)
    if not total_match:
        raise ValueError("Gesamtsumme nicht gefunden")
    amount_raw = total_match.group(1).replace(".", "").replace(",", ".")
    try:
        amount = Decimal(amount_raw)
    except InvalidOperation as exc:
        raise ValueError(f"Ungültiger Rechnungsbetrag: {amount_raw}") from exc
    amount_cents = int((amount * 100).to_integral_value())

    customer_name, customer_address = extract_customer(lines)

    return InvoiceRecord(
        file_path=storage_path,
        invoice_number=invoice_number,
        invoice_date=invoice_date,
        customer_name=customer_name,
        customer_address=customer_address,
        amount_cents=amount_cents,
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
            amount_cents
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            record.invoice_number,
            record.customer_name,
            record.customer_address,
            record.invoice_date,
            record.amount_cents,
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
    """
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


def determine_salutation_for_customer(customer_name: str) -> Optional[str]:
    """
    Determine salutation for a customer by extracting first name and using AI.
    """
    first_name = extract_first_name(customer_name)
    if not first_name:
        return None
    return determine_gender_via_ai(first_name)


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
    try:
        return str(pdf_path.relative_to(root))
    except ValueError:
        try:
            return str(pdf_path.relative_to(BASE_DIR))
        except ValueError:
            return str(pdf_path.resolve())


def process_pdf_file(conn: sqlite3.Connection, pdf_path: Path, root: Path) -> bool:
    """
    Process a PDF file and add it to the database with snapshot tracking.
    Returns True if a new invoice-snapshot link was created.
    """
    # Extract snapshot info from path
    snapshot_info = extract_snapshot_from_path(pdf_path, root)
    if not snapshot_info:
        logging.warning("Überspringe %s - nicht in einem Monatsordner (Format: YYYY-MM-...)", pdf_path)
        return False

    snapshot_date, folder_name = snapshot_info

    # Get storage key for file path
    key = storage_key(pdf_path, root)

    # Parse invoice data
    record = parse_invoice(pdf_path, key)

    # Get or create snapshot
    snapshot_id = get_or_create_snapshot(conn, snapshot_date, folder_name)

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
    with sqlite3.connect(db_path) as conn:
        init_db(conn)
        for pdf_path in pdf_files:
            try:
                if process_pdf_file(conn, pdf_path, root):
                    new_entries += 1
            except Exception as exc:
                logging.error("Kann %s nicht verarbeiten: %s", pdf_path, exc)
                continue
        conn.commit()

    logging.info("Fertig. %s neue Rechnungen gespeichert.", new_entries)

    if args.watch:
        run_watcher(root, db_path, args.settle_seconds)


if __name__ == "__main__":
    main()
