"""Shared configuration: data directory, constants, sort helpers and
SMTP/IMAP settings.

Extracted verbatim from web_app.py to keep the Flask app slim. No behaviour
change — only the location of these definitions moved.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

from dotenv import load_dotenv

# Ensure environment variables are available when the module-level paths below
# are computed (DATA_DIR etc.). Idempotent if web_app already called it.
load_dotenv()

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


DEFAULT_DB_PATH = get_data_dir() / "invoice_data.db"
DEFAULT_INVOICE_ROOT = get_data_dir() / "Rechnungen"
DEFAULT_LIMIT = 1000

ASCII_FALLBACK_MAP = str.maketrans({
    "ä": "ae",
    "Ä": "Ae",
    "ö": "oe",
    "Ö": "Oe",
    "ü": "ue",
    "Ü": "Ue",
    "ß": "ss",
})

SORT_COLUMN_MAP = {
    "date": "ist.invoice_date",
    # Sort by last name (actual last word of the name, using custom_name if available)
    # Implemented via a small SQLite UDF registered in fetch_invoices: LAST_WORD(text)
    # NULLIF converts empty strings to NULL so COALESCE falls back to customer_name
    "name": "LOWER(LAST_WORD(COALESCE(NULLIF(cd.custom_name, ''), ist.customer_name)))",
    "address": "LOWER(ist.customer_address)",
    "number": "COALESCE(ist.invoice_number, '')",
    "amount": "ist.amount_cents",
    "status": "ist.status",
}


def normalize_sort_params(sort_by: Optional[str], sort_direction: Optional[str]) -> Tuple[str, str]:
    """Return safe sort key/direction for invoice listings."""
    sort_key = (sort_by or "date").lower()
    if sort_key not in SORT_COLUMN_MAP:
        sort_key = "date"

    direction = (sort_direction or "desc").lower()
    if direction not in ("asc", "desc"):
        direction = "desc"

    return sort_key, direction


def sql_last_word(value: Optional[str]) -> str:
    """SQLite UDF: return the last whitespace-separated word of a string.

    - Treats None/empty as empty string
    - Collapses multiple spaces
    - Keeps hyphenated surnames intact (e.g., "Meyer-Lüdenscheidt")
    """
    if not value:
        return ""
    # Split on any whitespace and take the last non-empty token
    parts = str(value).strip().split()
    if not parts:
        return ""
    last = parts[-1]
    # Normalize German umlauts for predictable ordering
    try:
        return last.translate(ASCII_FALLBACK_MAP)
    except Exception:
        return last


@dataclass
class SMTPConfig:
    server: str
    port: int
    user: str
    password: str
    use_tls: bool
    from_name: str


def load_smtp_config() -> SMTPConfig:
    """Read SMTP settings from the environment."""
    return SMTPConfig(
        server=os.getenv('SMTP_SERVER', 'mail.kaeee.de'),
        port=int(os.getenv('SMTP_PORT', '587')),
        user=os.getenv('SMTP_USER', 'info@apothekeamdamm.de'),
        password=os.getenv('SMTP_PASSWORD', ''),
        use_tls=os.getenv('SMTP_USE_TLS', 'True').lower() == 'true',
        from_name=os.getenv('SMTP_FROM_NAME', 'Apotheke am Damm'),
    )


@dataclass
class IMAPConfig:
    server: str
    port: int
    user: str
    password: str


def load_imap_config() -> IMAPConfig:
    """Read IMAP settings from the environment."""
    return IMAPConfig(
        server=os.getenv('IMAP_SERVER', 'mail.kaeee.de'),
        port=int(os.getenv('IMAP_PORT', '993')),
        user=os.getenv('IMAP_USER', 'info@apothekeamdamm.de'),
        password=os.getenv('IMAP_PASSWORD', ''),
    )
