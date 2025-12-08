#!/usr/bin/env python3
"""
Minimal web UI for browsing invoices stored in ``invoice_data.db``.

The app exposes two routes:
* ``/`` – HTML table with optional search
* ``/pdf/<path>`` – streams the underlying PDF from the Rechnungen tree
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, date
from pathlib import Path
from typing import Iterable, List, Optional, Dict, Tuple, Any
from collections import defaultdict
import os
import smtplib
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from email.utils import encode_rfc2231
from dotenv import load_dotenv
import requests
import re as regex_module

from flask import (
    Flask,
    Response,
    abort,
    jsonify,
    redirect,
    render_template,
    request,
    send_from_directory,
    send_file,
    stream_with_context,
    url_for,
)

# Import scan functionality
import logging
import io
import tempfile
import unicodedata
from pypdf import PdfWriter, PdfReader
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib.utils import ImageReader
from reportlab.platypus import Paragraph
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_JUSTIFY
from invoice_tracker import (
    find_pdfs,
    find_pdfs_for_import,
    get_completed_folders,
    mark_folder_complete,
    mark_folder_incomplete,
    init_db,
    process_pdf_file,
    log_invoice_event,
    resolve_pending_import,
    extract_first_name,
    determine_genders_batch_via_ai,
    validate_customer_names_batch_via_ai,
)
from letterxpress_client import LetterXpressClient

# Load environment variables
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DB_PATH = BASE_DIR / "invoice_data.db"
DEFAULT_INVOICE_ROOT = BASE_DIR / "Rechnungen"
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


def create_smtp_connection(config: SMTPConfig) -> smtplib.SMTP:
    """Establish and return an authenticated SMTP connection."""
    if config.use_tls:
        server = smtplib.SMTP(config.server, config.port, timeout=30)
        server.ehlo()
        server.starttls()
        server.ehlo()
    else:
        server = smtplib.SMTP_SSL(config.server, config.port, timeout=30)

    if config.user:
        server.login(config.user, config.password)

    return server


def _ascii_safe_filename(filename: str) -> str:
    """Return a best-effort ASCII representation of a filename."""
    translated = filename.translate(ASCII_FALLBACK_MAP)
    normalized = unicodedata.normalize('NFKD', translated)
    ascii_name = normalized.encode('ascii', 'ignore').decode('ascii').strip()
    return ascii_name or "rechnung.pdf"


def create_pdf_attachment(invoice_pdf_path: Path) -> Optional[MIMEBase]:
    """Create a MIME attachment for a PDF with proper filename fallbacks."""
    if not invoice_pdf_path.exists():
        logging.warning(f"Invoice PDF not found (skipping): {invoice_pdf_path}")
        return None

    with open(invoice_pdf_path, 'rb') as f:
        pdf_attachment = MIMEBase('application', 'pdf')
        pdf_attachment.set_payload(f.read())

    encoders.encode_base64(pdf_attachment)

    filename = invoice_pdf_path.name
    ascii_filename = _ascii_safe_filename(filename)

    pdf_attachment.add_header('Content-Disposition', 'attachment', filename=ascii_filename)

    if ascii_filename != filename:
        encoded_filename = encode_rfc2231(filename, 'utf-8')
        pdf_attachment.set_param('filename*', encoded_filename, header='Content-Disposition')

    return pdf_attachment


def send_invoice_email(to_email: str, customer_name: str, invoice_pdf_path: Path, salutation: str = None) -> bool:
    """
    Send an invoice via email with a nice message from the pharmacy.

    Args:
        to_email: Recipient email address
        customer_name: Name of the customer
        invoice_pdf_path: Path to the invoice PDF file
        salutation: Salutation for the customer (e.g., "Herr", "Frau")

    Returns:
        True if email was sent successfully, False otherwise
    """
    try:
        smtp_config = load_smtp_config()

        # Create message
        msg = MIMEMultipart()
        msg['From'] = f"{smtp_config.from_name} <{smtp_config.user}>"
        msg['To'] = to_email
        msg['Subject'] = "Ihre Monatsrechnung"

        # Create email body with a nice message
        # Determine the greeting based on salutation
        if salutation and salutation.lower() in ['herr', 'herrn']:
            greeting = f"Sehr geehrter Herr {customer_name}"
        elif salutation and salutation.lower() == 'frau':
            greeting = f"Sehr geehrte Frau {customer_name}"
        elif salutation and salutation.lower() == 'familie':
            greeting = f"Sehr geehrte Familie {customer_name}"
        else:
            greeting = "Sehr geehrte Damen und Herren"

        email_body = f"""{greeting},

anbei senden wir Ihnen Ihre aktuelle Monatsrechnung.

Wir bedanken uns herzlich für Ihr Vertrauen und Ihre Treue. Sollten Sie Fragen zu Ihrer Rechnung haben, stehen wir Ihnen selbstverständlich gerne zur Verfügung.

Hinweis: Falls Sie einen bequemen Bankeinzug wünschen, sprechen Sie uns gerne an. Wir richten Ihnen gerne ein SEPA-Lastschriftmandat ein.

Mit freundlichen Grüßen
Ihr Team der Apotheke am Damm

---
Apotheke am Damm
Matthias Blüm, e.K.
Am Damm 17, 55232 Alzey
Tel. : 06731 / 548846
Fax: 06731 / 548847
www.apothekeamdamm.de

Der Inhalt dieser Nachricht ist vertraulich. Sollte diese Nachricht nicht für Sie bestimmt sein, löschen Sie diese bitte umgehend. This message was sent confidential. If you are not the recipient, please delete immediately.
"""

        msg.attach(MIMEText(email_body, 'plain', 'utf-8'))

        # Attach PDF invoice
        attachment = create_pdf_attachment(invoice_pdf_path)
        if not attachment:
            logging.error(f"Invoice PDF not found: {invoice_pdf_path}")
            return False
        msg.attach(attachment)

        server = create_smtp_connection(smtp_config)
        try:
            server.send_message(msg)
        finally:
            try:
                server.quit()
            except Exception:
                logging.warning("Failed to close SMTP connection cleanly")

        logging.info(f"Email sent successfully to {to_email}")
        return True

    except Exception as e:
        logging.error(f"Failed to send email to {to_email}: {e}")
        return False


def send_invoices_batch_email(
    to_email: str,
    customer_name: str,
    invoice_pdf_paths: List[Path],
    month_year: str,
    salutation: str = None,
    smtp_connection: Optional[smtplib.SMTP] = None,
    smtp_config: Optional[SMTPConfig] = None,
    invoice_list: Optional[List] = None,
    other_open_invoices: Optional[List] = None,
) -> bool:
    """
    Send multiple invoices via email with a nice message from the pharmacy.

    Args:
        to_email: Recipient email address
        customer_name: Name of the customer
        invoice_pdf_paths: List of paths to invoice PDF files
        month_year: Month and year string (e.g., "2024-01")
        salutation: Salutation for the customer (e.g., "Herr", "Frau")

    Returns:
        True if email was sent successfully, False otherwise
    """
    try:
        config = smtp_config or load_smtp_config()

        # Create message
        msg = MIMEMultipart()
        msg['From'] = f"{config.from_name} <{config.user}>"
        msg['To'] = to_email

        # Format subject - always the same
        subject = "💊 Ihre aktuelle Monatsrechnung"
        msg['Subject'] = subject

        invoice_count = len(invoice_pdf_paths)

        # Create email body with a nice message
        # Determine the greeting based on salutation
        if salutation and salutation.lower() in ['herr', 'herrn']:
            greeting = f"Sehr geehrter Herr {customer_name}"
        elif salutation and salutation.lower() == 'frau':
            greeting = f"Sehr geehrte Frau {customer_name}"
        elif salutation and salutation.lower() == 'familie':
            greeting = f"Sehr geehrte Familie {customer_name}"
        else:
            greeting = "Sehr geehrte Damen und Herren"

        # Build invoice list if provided
        invoice_details = ""
        if invoice_list and len(invoice_list) > 0:
            invoice_details = "\n\nFolgende Rechnungen sind im Anhang:\n"
            for inv in invoice_list:
                # Format date
                invoice_date_str = inv.invoice_date if inv.invoice_date else "Unbekannt"
                if invoice_date_str and len(invoice_date_str) >= 10:
                    # Convert from ISO format (YYYY-MM-DD) to German format (DD.MM.YYYY)
                    try:
                        from datetime import datetime
                        date_obj = datetime.fromisoformat(invoice_date_str)
                        invoice_date_str = date_obj.strftime("%d.%m.%Y")
                    except:
                        pass

                # Format amount
                amount_str = f"{inv.amount_cents / 100:.2f} €"

                # Format invoice number
                inv_number = inv.invoice_number if inv.invoice_number else "ohne Nummer"

                invoice_details += f"  - Rechnung Nr. {inv_number} vom {invoice_date_str}: {amount_str}\n"

        # Build list of other open invoices (not attached)
        other_open_details = ""
        if other_open_invoices and len(other_open_invoices) > 0:
            other_open_details = "\nBitte beachten Sie, dass folgende Rechnungen noch offen sind:\n"
            total_other_open = 0
            for inv in other_open_invoices:
                # Format date
                inv_date_str = inv.invoice_date if inv.invoice_date else "Unbekannt"
                if inv_date_str and len(inv_date_str) >= 10:
                    try:
                        from datetime import datetime
                        date_obj = datetime.fromisoformat(inv_date_str)
                        inv_date_str = date_obj.strftime("%d.%m.%Y")
                    except:
                        pass
                # Format amount
                inv_amount = inv.amount_cents / 100
                total_other_open += inv_amount
                amount_str = f"{inv_amount:.2f} EUR"
                inv_number = inv.invoice_number if inv.invoice_number else "ohne Nummer"
                other_open_details += f"  - Rechnung Nr. {inv_number} vom {inv_date_str}: {amount_str}\n"
            other_open_details += f"\nGesamtbetrag offene Rechnungen: {total_other_open:.2f} EUR\n"

        # Adjust message based on number of invoices
        if invoice_count == 1:
            invoice_text = "anbei senden wir Ihnen Ihre aktuelle Rechnung."
        else:
            invoice_text = "anbei senden wir Ihnen Ihre aktuellen Rechnungen."

        email_body = f"""{greeting},

{invoice_text}{invoice_details}{other_open_details}
Wir bedanken uns herzlich für Ihr Vertrauen und Ihre Treue. ✨
Sollten Sie Fragen zu Ihrer Rechnung haben, stehen wir Ihnen selbstverständlich gerne zur Verfügung.

💬 Nutzen Sie bei Fragen zu Ihren Rechnungen WhatsApp unter: 06731-548846

💡 Hinweis: Falls Sie einen bequemen Bankeinzug wünschen, sprechen Sie uns gerne an.
Wir richten Ihnen gerne ein SEPA-Lastschriftmandat ein.

Mit freundlichen Grüßen
Ihr Team der Apotheke am Damm

---
Apotheke am Damm
Matthias Blüm, e.K.
Am Damm 17, 55232 Alzey
Tel. : 06731 / 548846
Fax: 06731 / 548847
www.apothekeamdamm.de

Der Inhalt dieser Nachricht ist vertraulich. Sollte diese Nachricht nicht für Sie bestimmt sein, löschen Sie diese bitte umgehend. This message was sent confidential. If you are not the recipient, please delete immediately.
"""

        msg.attach(MIMEText(email_body, 'plain', 'utf-8'))

        # Attach all PDF invoices
        for invoice_pdf_path in invoice_pdf_paths:
            attachment = create_pdf_attachment(invoice_pdf_path)
            if attachment:
                msg.attach(attachment)

        connection = smtp_connection
        owns_connection = False
        if connection is None:
            connection = create_smtp_connection(config)
            owns_connection = True

        try:
            connection.send_message(msg)
        finally:
            if owns_connection and connection:
                try:
                    connection.quit()
                except Exception:
                    logging.warning("Failed to close SMTP connection cleanly")

        logging.info(f"Batch email sent successfully to {to_email} with {invoice_count} invoices")
        return True

    except Exception as e:
        logging.error(f"Failed to send batch email to {to_email}: {e}")
        return False


def extract_first_name(customer_name: str) -> Optional[str]:
    """
    Extract the first name from a customer name.
    Handles various formats like "Max Mustermann", "Mustermann, Max", etc.

    Args:
        customer_name: Full customer name

    Returns:
        First name or None if extraction fails
    """
    if not customer_name:
        return None

    # Remove common titles
    name_clean = customer_name.strip()
    for title in ["Dr.", "Prof.", "Dipl.-Ing.", "Ing."]:
        name_clean = name_clean.replace(title, "").strip()

    # Try different patterns
    # Pattern 1: "Vorname Nachname" or "Vorname Mittelname Nachname"
    parts = name_clean.split()
    if len(parts) >= 2:
        # First part is likely the first name
        return parts[0].strip()

    # Pattern 2: "Nachname, Vorname"
    if "," in name_clean:
        parts = name_clean.split(",")
        if len(parts) >= 2:
            return parts[1].strip().split()[0] if parts[1].strip() else None

    # If only one part, return it
    if len(parts) == 1:
        return parts[0].strip()

    return None


def determine_gender_via_ai(first_name: str) -> Optional[str]:
    """
    Use Nebius AI (Meta Llama 70B) to determine the gender based on first name.

    Args:
        first_name: The first name to analyze

    Returns:
        "Herr" for male, "Frau" for female, or None if uncertain
    """
    try:
        api_key = os.getenv('NEBIUS_API_KEY')
        if not api_key:
            logging.error("NEBIUS_API_KEY not found in environment")
            return None

        # Nebius Studio API endpoint (OpenAI-compatible)
        url = "https://api.studio.nebius.com/v1/chat/completions"

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

        # Prompt for the AI
        prompt = f"""Bestimme das Geschlecht des Vornamens "{first_name}".
Antworte NUR mit einem dieser Wörter:
- "männlich" wenn der Name typischerweise männlich ist
- "weiblich" wenn der Name typischerweise weiblich ist
- "unbekannt" wenn du dir nicht sicher bist

Antwort:"""

        payload = {
            "model": "meta-llama/Llama-3.3-70B-Instruct",
            "messages": [
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": 0.1,
            "max_tokens": 10
        }

        response = requests.post(url, headers=headers, json=payload, timeout=10)
        response.raise_for_status()

        data = response.json()
        ai_response = data.get("choices", [{}])[0].get("message", {}).get("content", "").strip().lower()

        logging.info(f"AI response for '{first_name}': {ai_response}")

        # Parse response
        if "männlich" in ai_response or "male" in ai_response:
            return "Herr"
        elif "weiblich" in ai_response or "female" in ai_response:
            return "Frau"
        else:
            return None

    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to call Nebius AI for '{first_name}': {e}")
        return None
    except Exception as e:
        logging.error(f"Error determining gender for '{first_name}': {e}")
        return None


def determine_salutation_for_customer(customer_name: str) -> Optional[str]:
    """
    Determine salutation for a customer by extracting first name and using AI.

    Args:
        customer_name: Full customer name

    Returns:
        "Herr", "Frau", or None
    """
    first_name = extract_first_name(customer_name)
    if not first_name:
        logging.warning(f"Could not extract first name from: {customer_name}")
        return None

    return determine_gender_via_ai(first_name)


def get_customer_custom_address(conn: sqlite3.Connection, customer_name: str) -> Optional[Tuple[str, str, str]]:
    """
    Get custom address for a customer from customer_details table.

    Args:
        conn: Database connection
        customer_name: Customer name to lookup

    Returns:
        Tuple of (custom_name, custom_street, custom_city) if custom address exists, None otherwise
    """
    cursor = conn.execute(
        "SELECT custom_name, custom_street, custom_city FROM customer_details WHERE customer_name = ?",
        (customer_name,)
    )
    row = cursor.fetchone()

    if row and row[0] and row[1] and row[2]:
        # All custom fields must be present
        return (row[0], row[1], row[2])

    return None


def draw_justified_paragraph(c, text, x, y, width, font_size=10, font_name='Helvetica'):
    """
    Draw a justified paragraph at given position.
    Returns the new y position after the paragraph.
    """
    style = ParagraphStyle(
        'Justified',
        fontName=font_name,
        fontSize=font_size,
        alignment=TA_JUSTIFY,
        leading=font_size * 1.2
    )

    p = Paragraph(text, style)
    w, h = p.wrap(width, 1000)  # wrap to given width
    p.drawOn(c, x, y - h)
    return y - h  # return new y position


def draw_modern_footer(c, left_margin, right_margin, footer_y, include_bank_details=True):
    """
    Draw a modern, 3-column footer with balanced layout.

    Args:
        c: Canvas object
        left_margin: Left margin
        right_margin: Right margin
        footer_y: Y position for footer
        include_bank_details: If True, include bank details (for Sammelrechnung)
    """
    from reportlab.lib.colors import HexColor

    primary_color = HexColor("#123C69")
    black = HexColor("#000000")
    gray = HexColor("#666666")

    # Trennlinie (gestrichelt, elegant)
    c.setStrokeColor(HexColor("#cccccc"))
    c.setDash(2, 2)
    c.line(left_margin, footer_y + 15*mm, right_margin, footer_y + 15*mm)
    c.setDash()

    y_start = footer_y + 2*mm

    # === 3-SPALTEN-LAYOUT (gleichmäßig verteilt) ===
    page_width = right_margin - left_margin
    col_width = page_width / 3

    # Spalte 1: LINKS (Adresse)
    col1_x = left_margin

    # Spalte 2: MITTE (Kontakt)
    col2_x = left_margin + col_width

    # Spalte 3: RECHTS (Bank oder Rechtliches)
    col3_x = left_margin + (col_width * 2)

    # === SPALTE 1: ADRESSE (LINKS) ===
    y = y_start
    c.setFillColor(primary_color)
    c.setFont("Helvetica-Bold", 9)
    c.drawString(col1_x, y, "Apotheke am Damm")

    y -= 4*mm
    c.setFillColor(gray)
    c.setFont("Helvetica", 7)
    c.drawString(col1_x, y, "Inh. Matthias Blüm, e.K.")

    y -= 3.5*mm
    c.drawString(col1_x, y, "Am Damm 17")

    y -= 3.5*mm
    c.drawString(col1_x, y, "55232 Alzey")

    # === SPALTE 2: KONTAKT (MITTE) ===
    y = y_start
    c.setFillColor(primary_color)
    c.setFont("Helvetica-Bold", 9)
    c.drawString(col2_x, y, "Kontakt")

    y -= 4*mm
    c.setFillColor(gray)
    c.setFont("Helvetica", 7)
    c.drawString(col2_x, y, "Tel: 06731-548846")

    y -= 3.5*mm
    c.drawString(col2_x, y, "Fax: 06731-548847")

    y -= 3.5*mm
    c.drawString(col2_x, y, "info@apothekeamdamm.de")

    y -= 3.5*mm
    c.drawString(col2_x, y, "WhatsApp: 06731-548846")

    # === SPALTE 3: BANK ODER RECHTLICHES (RECHTS) ===
    y = y_start

    if include_bank_details:
        # Bei Sammelrechnung: Bankverbindung
        c.setFillColor(primary_color)
        c.setFont("Helvetica-Bold", 9)
        c.drawString(col3_x, y, "Bankverbindung")

        y -= 4*mm
        c.setFillColor(gray)
        c.setFont("Helvetica", 7)
        c.drawString(col3_x, y, "Sparkasse Worms-Alzey-Ried")

        y -= 3.5*mm
        c.setFont("Helvetica", 6.5)
        c.drawString(col3_x, y, "IBAN: DE51 5535 0010")
        y -= 2.5*mm
        c.drawString(col3_x, y, "0033 7173 83")

        y -= 3.5*mm
        c.drawString(col3_x, y, "BIC: MALADE51WOR")
    else:
        # Bei Mahnungen: Rechtliches
        c.setFillColor(primary_color)
        c.setFont("Helvetica-Bold", 9)
        c.drawString(col3_x, y, "Rechtliches")

        y -= 4*mm
        c.setFillColor(gray)
        c.setFont("Helvetica", 7)
        c.drawString(col3_x, y, "HRA 31710")

        y -= 3.5*mm
        c.drawString(col3_x, y, "Amtsgericht Mainz")

        y -= 3.5*mm
        c.drawString(col3_x, y, "USt-IdNr. DE814983365")


# Legacy function names for backwards compatibility
def draw_footer(c, left_margin, width, footer_y):
    """Legacy wrapper for draw_modern_footer (Sammelrechnungen)."""
    right_margin = width - 25*mm
    draw_modern_footer(c, left_margin, right_margin, footer_y, include_bank_details=True)


def draw_reminder_footer(c, left_margin, width, footer_y):
    """Legacy wrapper for draw_modern_footer (Mahnungen)."""
    right_margin = width - 25*mm
    draw_modern_footer(c, left_margin, right_margin, footer_y, include_bank_details=False)


def check_page_break(c, current_y, needed_space, left_margin, width, height, is_reminder=False):
    """
    Check if page break is needed and handle it.

    Args:
        c: ReportLab canvas
        current_y: Current Y position
        needed_space: Space needed for the next content block
        left_margin: Left margin
        width: Page width
        height: Page height
        is_reminder: If True, use reminder footer style

    Returns:
        New Y position (either current or reset to top of new page)
    """
    FOOTER_SPACE = 120  # Space reserved for footer (footer at 80, needs ~40 pixels)
    MIN_Y = FOOTER_SPACE

    if current_y - needed_space < MIN_Y:
        # Draw footer on current page
        if is_reminder:
            draw_reminder_footer(c, left_margin, width, 80)
        else:
            draw_footer(c, left_margin, width, 80)

        # Start new page
        c.showPage()

        # Reset to top of page (leaving space for header if needed)
        return height - 100

    return current_y


def create_cover_letter_pdf(
    customer_name: str,
    customer_address: str,
    current_month_invoices: List[Dict],
    older_open_invoices: List[Dict],
    salutation: Optional[str] = None
) -> bytes:
    """
    Create a modern cover letter PDF for Sammelrechnungen.

    Args:
        customer_name: Name of the customer
        customer_address: Full address of the customer
        current_month_invoices: List of invoices from the latest month
        older_open_invoices: List of older open invoices
        salutation: Salutation for the customer

    Returns:
        PDF bytes
    """
    from reportlab.lib.colors import HexColor

    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4

    # Colors
    primary_color = HexColor("#123C69")
    black = HexColor("#000000")
    box_bg = HexColor("#f0f4f8")

    # Margins (DIN 5008 konform - ADJUSTED)
    left_margin = 25 * mm  # ADJUSTED: +5mm nach rechts (war 20mm)
    right_margin = width - 25 * mm

    # === KOPFBEREICH (MODERN) ===
    y_pos = height - 25*mm

    c.setFillColor(primary_color)
    c.setFont("Helvetica-Bold", 16)
    c.drawString(left_margin, y_pos, "Apotheke am Damm")

    y_pos -= 6*mm
    c.setFillColor(black)
    c.setFont("Helvetica", 9)
    c.drawString(left_margin, y_pos, "Am Damm 17 | 55232 Alzey")

    # Rücksendeadresse (klein, DIN 5008)
    # DIN 5008: 44mm von oben - ADJUSTED: +4mm nach unten
    return_address_y = height - (48 * mm)
    c.setFont("Helvetica", 8)
    c.drawString(left_margin, return_address_y, "Apotheke am Damm, Am Damm 17, 55232 Alzey")

    # === EMPFÄNGERADRESSE (DIN 5008: 66-88mm) ===
    # DIN 5008: 66-88mm von oben - ADJUSTED: +10mm nach unten (76mm)
    recipient_y_start = height - (76 * mm)

    # Parse address
    address_lines = customer_address.split('\n') if '\n' in customer_address else customer_address.split(',')
    address_lines = [line.strip() for line in address_lines if line.strip()]

    # Greeting line
    if salutation and salutation.lower() in ['herr', 'herrn']:
        greeting_line = f"Herr {customer_name}"
    elif salutation and salutation.lower() == 'frau':
        greeting_line = f"Frau {customer_name}"
    elif salutation and salutation.lower() == 'familie':
        greeting_line = f"Familie {customer_name}"
    else:
        greeting_line = customer_name

    c.setFont("Helvetica-Bold", 11)
    c.drawString(left_margin, recipient_y_start, greeting_line)

    # Address lines
    c.setFont("Helvetica", 11)
    line_height = 4.5 * mm
    for i, line in enumerate(address_lines):
        c.drawString(left_margin, recipient_y_start - ((i + 1) * line_height), line)

    # === DATUM ===
    date_y = height - (106 * mm)
    today = datetime.now().strftime("%d.%m.%Y")
    c.setFont("Helvetica", 10)
    c.drawRightString(right_margin, date_y, f"Alzey, {today}")

    # === BETREFFZEILE (MIT FARBE) ===
    subject_y = date_y - 20
    c.setFillColor(primary_color)
    c.setFont("Helvetica-Bold", 14)

    # Month/year from first invoice
    if current_month_invoices:
        first_date = datetime.strptime(current_month_invoices[0]['date'], '%Y-%m-%d')
        month_year = first_date.strftime("%m.%Y")
        if len(current_month_invoices) == 1:
            subject_text = f"Ihre Monatsrechnung {month_year}"
        else:
            subject_text = f"Ihre Monatsrechnungen {month_year}"
    else:
        subject_text = "Ihre Monatsrechnungen"

    c.drawString(left_margin, subject_y, subject_text)

    # === ANREDE ===
    content_y = subject_y - 25
    c.setFillColor(black)
    c.setFont("Helvetica", 10)

    if salutation and salutation.lower() in ['herr', 'herrn']:
        salutation_text = f"Sehr geehrter Herr {customer_name.split()[-1]},"
    elif salutation and salutation.lower() == 'frau':
        salutation_text = f"Sehr geehrte Frau {customer_name.split()[-1]},"
    elif salutation and salutation.lower() == 'familie':
        salutation_text = f"Sehr geehrte Familie {customer_name.split()[-1]},"
    else:
        salutation_text = "Sehr geehrte Damen und Herren,"

    c.drawString(left_margin, content_y, salutation_text)

    # === HAUPTTEXT ===
    content_y -= 20
    if len(current_month_invoices) == 1:
        c.drawString(left_margin, content_y, "anbei erhalten Sie Ihre aktuelle Rechnung:")
    else:
        c.drawString(left_margin, content_y, "anbei erhalten Sie Ihre aktuellen Rechnungen:")

    # === TABELLE MIT MODERNEM STYLING ===
    content_y -= 25

    # Table header (mit Farbe)
    c.setFillColor(primary_color)
    c.setFont("Helvetica-Bold", 10)

    col1_x = left_margin + 10
    col2_x = left_margin + 110
    col3_x = right_margin - 70

    c.drawString(col1_x, content_y, "Rechnungs-Nr.")
    c.drawString(col2_x, content_y, "Datum")
    c.drawRightString(col3_x, content_y, "Betrag")

    content_y -= 3
    c.setStrokeColor(primary_color)
    c.setLineWidth(1.5)
    c.line(left_margin, content_y, right_margin - 60, content_y)
    content_y -= 12

    # Table rows
    c.setFillColor(black)
    c.setFont("Helvetica", 10)
    c.setStrokeColor(black)
    c.setLineWidth(1)

    total_current = 0.0
    for inv in current_month_invoices:
        inv_date_str = datetime.strptime(inv['date'], '%Y-%m-%d').strftime('%d.%m.%Y')
        c.drawString(col1_x, content_y, inv['number'])
        c.drawString(col2_x, content_y, inv_date_str)
        c.drawRightString(col3_x, content_y, f"{inv['amount']:.2f} €")
        total_current += inv['amount']
        content_y -= 14

    # === GESAMTSUMME IN BOX ===
    content_y -= 5
    box_height = 15
    c.setFillColor(box_bg)
    c.rect(left_margin, content_y - box_height, right_margin - left_margin - 60, box_height, stroke=0, fill=1)

    c.setFillColor(primary_color)
    c.setFont("Helvetica-Bold", 11)
    c.drawString(col1_x, content_y - 10, "Gesamtsumme:")
    c.drawRightString(col3_x, content_y - 10, f"{total_current:.2f} €")

    content_y -= box_height + 10

    # === ÄLTERE RECHNUNGEN (FALLS VORHANDEN) ===
    if older_open_invoices:
        content_y -= 20
        c.setFillColor(black)
        c.setFont("Helvetica", 10)
        c.drawString(left_margin, content_y, "Bitte beachten Sie außerdem folgende noch offenen Rechnungen:")

        content_y -= 20

        # Table header
        c.setFillColor(primary_color)
        c.setFont("Helvetica-Bold", 10)
        c.drawString(col1_x, content_y, "Rechnungs-Nr.")
        c.drawString(col2_x, content_y, "Datum")
        c.drawRightString(col3_x, content_y, "Betrag")

        content_y -= 3
        c.setStrokeColor(primary_color)
        c.line(left_margin, content_y, right_margin - 60, content_y)
        content_y -= 12

        # Rows
        c.setFillColor(black)
        c.setFont("Helvetica", 10)
        c.setStrokeColor(black)

        total_older = 0.0
        for inv in older_open_invoices:
            inv_date_str = datetime.strptime(inv['date'], '%Y-%m-%d').strftime('%d.%m.%Y')
            c.drawString(col1_x, content_y, inv['number'])
            c.drawString(col2_x, content_y, inv_date_str)
            c.drawRightString(col3_x, content_y, f"{inv['amount']:.2f} €")
            total_older += inv['amount']
            content_y -= 14

        # Sum box
        content_y -= 5
        c.setFillColor(box_bg)
        c.rect(left_margin, content_y - box_height, right_margin - left_margin - 60, box_height, stroke=0, fill=1)

        c.setFillColor(primary_color)
        c.setFont("Helvetica-Bold", 11)
        c.drawString(col1_x, content_y - 10, "Summe offener Rechnungen:")
        c.drawRightString(col3_x, content_y - 10, f"{total_older:.2f} €")

        content_y -= box_height + 10

    # === HINWEIS ZUZAHLUNGSBEFREIUNG ===
    content_y -= 25
    c.setFillColor(primary_color)
    c.setFont("Helvetica-Bold", 11)
    c.drawString(left_margin, content_y, "Hinweis bei Zuzahlungsbefreiung:")

    content_y -= 15
    text_width = right_margin - left_margin
    c.setFillColor(black)
    text = ("Trotz Befreiung von der Rezeptgebühr ist der Rechnungsbetrag fällig, da das Rezept/die Rezepte vom "
            "Arzt als \"gebührenpflichtig\" gekennzeichnet wurde(n). Mit dieser Rechnung und einem "
            "Zahlungsnachweis erhalten Sie den Betrag von Ihrer Krankenkasse erstattet. Bitte reichen Sie uns "
            "ebenfalls eine Kopie des Befreiungsausweises ein. Für Rückfragen helfen wir Ihnen natürlich gerne "
            "weiter.")
    content_y = draw_justified_paragraph(c, text, left_margin, content_y, text_width, font_size=9)

    # === SCHLUSS ===
    content_y -= 20
    c.setFont("Helvetica", 10)
    c.drawString(left_margin, content_y, "Wir bedanken uns herzlich für Ihr Vertrauen und Ihre Treue.")

    content_y -= 20
    c.drawString(left_margin, content_y, "Mit freundlichen Grüßen")
    content_y -= 10
    c.setFillColor(primary_color)
    c.setFont("Helvetica-Bold", 11)
    c.drawString(left_margin, content_y, "Ihr Team der Apotheke am Damm")

    # === FOOTER ===
    footer_y = 20*mm
    draw_modern_footer(c, left_margin, right_margin, footer_y, include_bank_details=True)

    c.save()
    buffer.seek(0)
    return buffer.getvalue()


def create_reminder_pdf(
    customer_name: str,
    customer_address: str,
    invoices: List[Dict],
    reminder_level: int,
    salutation: Optional[str] = None
) -> bytes:
    """
    Create a modern payment reminder or dunning letter PDF.

    Args:
        customer_name: Name of the customer
        customer_address: Full address of the customer
        invoices: List of invoices with date, number, and amount
        reminder_level: 0 = Zahlungserinnerung, 1 = 1. Mahnung, 2 = 2. Mahnung
        salutation: Salutation for the customer

    Returns:
        PDF bytes
    """
    from reportlab.lib.colors import HexColor

    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4

    # Colors
    primary_color = HexColor("#123C69")
    black = HexColor("#000000")
    box_bg = HexColor("#f0f4f8")
    warning_color = HexColor("#dc3545")  # Red for level 2
    warning_bg = HexColor("#fff3cd")  # Yellow warning box

    # Margins (DIN 5008 konform - ADJUSTED)
    left_margin = 25 * mm  # ADJUSTED: +5mm nach rechts (war 20mm)
    right_margin = width - 25 * mm

    # === KOPFBEREICH (MODERN) ===
    y_pos = height - 25*mm

    c.setFillColor(primary_color)
    c.setFont("Helvetica-Bold", 16)
    c.drawString(left_margin, y_pos, "Apotheke am Damm")

    y_pos -= 6*mm
    c.setFillColor(black)
    c.setFont("Helvetica", 9)
    c.drawString(left_margin, y_pos, "Am Damm 17 | 55232 Alzey | Tel: 06731-548846")

    # Rücksendeadresse (klein, DIN 5008)
    # DIN 5008: 44mm von oben - ADJUSTED: +4mm nach unten
    return_address_y = height - (48 * mm)
    c.setFont("Helvetica", 8)
    c.drawString(left_margin, return_address_y, "Apotheke am Damm, Am Damm 17, 55232 Alzey")

    # === EMPFÄNGERADRESSE (DIN 5008: 66-88mm) ===
    # DIN 5008: 66-88mm von oben - ADJUSTED: +10mm nach unten (76mm)
    recipient_y_start = height - (76 * mm)

    # Parse address
    address_lines = customer_address.split('\n') if '\n' in customer_address else customer_address.split(',')
    address_lines = [line.strip() for line in address_lines if line.strip()]

    # Greeting line
    if salutation and salutation.lower() in ['herr', 'herrn']:
        greeting_line = f"Herr {customer_name}"
    elif salutation and salutation.lower() == 'frau':
        greeting_line = f"Frau {customer_name}"
    elif salutation and salutation.lower() == 'familie':
        greeting_line = f"Familie {customer_name}"
    else:
        greeting_line = customer_name

    c.setFont("Helvetica-Bold", 11)
    c.drawString(left_margin, recipient_y_start, greeting_line)

    # Address lines
    c.setFont("Helvetica", 11)
    line_height = 4.5 * mm
    for i, line in enumerate(address_lines):
        c.drawString(left_margin, recipient_y_start - ((i + 1) * line_height), line)

    # === DATUM ===
    date_y = height - (106 * mm)
    today = datetime.now().strftime("%d.%m.%Y")
    c.setFont("Helvetica", 10)
    c.drawRightString(right_margin, date_y, f"Alzey, {today}")

    # === BETREFFZEILE (MIT FARBE - ROT FÜR LETZTE MAHNUNG) ===
    subject_y = date_y - 20

    level_names = {
        0: "Zahlungserinnerung",
        1: "1. Mahnung",
        2: "2. Mahnung - LETZTE ZAHLUNGSAUFFORDERUNG"
    }
    subject_text = level_names.get(reminder_level, "Zahlungserinnerung")

    # Color based on level
    if reminder_level == 2:
        c.setFillColor(warning_color)
    else:
        c.setFillColor(primary_color)

    c.setFont("Helvetica-Bold", 14)
    c.drawString(left_margin, subject_y, subject_text)

    # === ANREDE ===
    content_y = subject_y - 25
    c.setFillColor(black)
    c.setFont("Helvetica", 10)

    if salutation and salutation.lower() in ['herr', 'herrn']:
        salutation_text = f"Sehr geehrter Herr {customer_name.split()[-1]},"
    elif salutation and salutation.lower() == 'frau':
        salutation_text = f"Sehr geehrte Frau {customer_name.split()[-1]},"
    elif salutation and salutation.lower() == 'familie':
        salutation_text = f"Sehr geehrte Familie {customer_name.split()[-1]},"
    else:
        salutation_text = "Sehr geehrte Damen und Herren,"

    c.drawString(left_margin, content_y, salutation_text)

    # === HAUPTTEXT (ABHÄNGIG VON MAHNSTUFE) ===
    content_y -= 20

    if reminder_level == 0:
        text_lines = [
            "bei der Durchsicht unserer Buchhaltung ist uns aufgefallen, dass der",
            "Rechnungsbetrag für die unten aufgeführten Rechnungen noch nicht bei uns",
            "eingegangen ist. Wir bitten Sie, die offenen Beträge innerhalb von 14 Tagen",
            "auf unser Konto zu überweisen."
        ]
    elif reminder_level == 1:
        text_lines = [
            "trotz unserer Zahlungserinnerung haben wir bisher keinen Zahlungseingang",
            "für die unten aufgeführten Rechnungen feststellen können. Wir fordern Sie",
            "hiermit auf, den ausstehenden Betrag innerhalb von 10 Tagen nach Erhalt",
            "dieses Schreibens zu überweisen."
        ]
    else:  # Level 2
        # Warning box for level 2
        box_height = 45
        box_width = right_margin - left_margin
        c.setFillColor(warning_bg)
        c.rect(left_margin, content_y - box_height, box_width, box_height, stroke=0, fill=1)

        c.setFillColor(warning_color)
        c.setFont("Helvetica-Bold", 11)
        c.drawString(left_margin + 5, content_y - 10, "⚠ LETZTE ZAHLUNGSAUFFORDERUNG")

        c.setFillColor(black)
        c.setFont("Helvetica", 10)
        c.drawString(left_margin + 5, content_y - 25, "Trotz mehrmaliger Zahlungsaufforderungen ist der ausstehende")
        c.drawString(left_margin + 5, content_y - 37, "Rechnungsbetrag bis heute nicht bei uns eingegangen.")

        content_y -= box_height + 5
        text_lines = []

    for line in text_lines:
        c.drawString(left_margin, content_y, line)
        content_y -= 12

    # === TABELLE MIT MODERNEM STYLING ===
    content_y -= 15

    # Table header (mit Farbe)
    c.setFillColor(primary_color)
    c.setFont("Helvetica-Bold", 10)

    col1_x = left_margin + 10
    col2_x = left_margin + 110
    col3_x = right_margin - 70

    c.drawString(col1_x, content_y, "Rechnungs-Nr.")
    c.drawString(col2_x, content_y, "Datum")
    c.drawRightString(col3_x, content_y, "Betrag")

    content_y -= 3
    c.setStrokeColor(primary_color)
    c.setLineWidth(1.5)
    c.line(left_margin, content_y, right_margin - 60, content_y)
    content_y -= 12

    # Table rows
    c.setFillColor(black)
    c.setFont("Helvetica", 10)
    c.setStrokeColor(black)
    c.setLineWidth(1)

    total_amount = 0.0
    for inv in invoices:
        inv_date_str = datetime.strptime(inv['date'], '%Y-%m-%d').strftime('%d.%m.%Y')
        c.drawString(col1_x, content_y, inv['number'])
        c.drawString(col2_x, content_y, inv_date_str)
        c.drawRightString(col3_x, content_y, f"{inv['amount']:.2f} €")
        total_amount += inv['amount']
        content_y -= 14

    # Add reminder fees (Mahngebühren) if applicable
    reminder_fee = 0.0
    if reminder_level == 1:
        reminder_fee = 5.0  # 5€ for 1. Mahnung
    elif reminder_level == 2:
        reminder_fee = 10.0  # 10€ for 2. Mahnung

    if reminder_fee > 0:
        c.drawString(col1_x, content_y, "Mahngebühren")
        c.drawString(col2_x, content_y, "")
        c.drawRightString(col3_x, content_y, f"{reminder_fee:.2f} €")
        total_amount += reminder_fee
        content_y -= 14

    # === GESAMTSUMME IN BOX ===
    content_y -= 5
    box_height = 15

    # Use warning color for level 2
    if reminder_level == 2:
        c.setFillColor(warning_bg)
    else:
        c.setFillColor(box_bg)

    c.rect(left_margin, content_y - box_height, right_margin - left_margin - 60, box_height, stroke=0, fill=1)

    if reminder_level == 2:
        c.setFillColor(warning_color)
    else:
        c.setFillColor(primary_color)

    c.setFont("Helvetica-Bold", 12)
    c.drawString(col1_x, content_y - 10, "Offener Gesamtbetrag:")
    c.drawRightString(col3_x, content_y - 10, f"{total_amount:.2f} €")

    content_y -= box_height + 15

    # === BANKVERBINDUNG IN INFO-BOX ===
    c.setFillColor(box_bg)
    box_height = 35
    c.rect(left_margin, content_y - box_height, right_margin - left_margin, box_height, stroke=0, fill=1)

    c.setFillColor(primary_color)
    c.setFont("Helvetica-Bold", 10)
    c.drawString(left_margin + 5, content_y - 10, "Unsere Bankverbindung:")

    c.setFillColor(black)
    c.setFont("Helvetica", 9)
    c.drawString(left_margin + 5, content_y - 22, "Sparkasse Worms-Alzey-Ried")
    c.drawString(left_margin + 5, content_y - 32, "IBAN: DE51 5535 0010 0033 7173 83  •  BIC: MALADE51WOR")

    content_y -= box_height + 15

    # === SCHLUSS ===
    c.setFont("Helvetica", 10)
    if reminder_level < 2:
        c.drawString(left_margin, content_y, "Für Rückfragen stehen wir Ihnen gerne zur Verfügung.")
        content_y -= 15
        c.drawString(left_margin, content_y, "Mit freundlichen Grüßen")
    else:
        c.setFont("Helvetica-Bold", 10)
        c.drawString(left_margin, content_y, "Bitte überweisen Sie den Betrag umgehend, um weitere Maßnahmen zu vermeiden.")
        content_y -= 15
        c.setFont("Helvetica", 10)
        c.drawString(left_margin, content_y, "Mit freundlichen Grüßen")

    content_y -= 10
    c.setFillColor(primary_color)
    c.setFont("Helvetica-Bold", 11)
    c.drawString(left_margin, content_y, "Ihr Team der Apotheke am Damm")

    # === FOOTER ===
    footer_y = 20*mm
    draw_modern_footer(c, left_margin, right_margin, footer_y, include_bank_details=False)

    # === SEITE 2: ZUSÄTZLICHE INFORMATIONEN ===
    c.showPage()

    # Co-payment exemption notice (at top of page 2)
    info_y = height - 150
    c.setFillColor(primary_color)
    c.setFont("Helvetica-Bold", 11)
    c.drawString(left_margin, info_y, "Hinweis bei Zuzahlungsbefreiung:")

    info_y -= 15
    text_width = right_margin - left_margin
    c.setFillColor(black)
    text = ("Trotz Befreiung von der Rezeptgebühr ist der Rechnungsbetrag fällig, da das Rezept/die Rezepte vom Arzt als "
            "\"gebührenpflichtig\" gekennzeichnet wurde(n). Mit dieser Rechnung und einem Zahlungsnachweis erhalten Sie den "
            "Betrag von Ihrer Krankenkasse erstattet. Bitte reichen Sie uns ebenfalls eine Kopie des Befreiungsausweises ein. "
            "Für Rückfragen helfen wir Ihnen natürlich gerne weiter.")
    info_y = draw_justified_paragraph(c, text, left_margin, info_y, text_width, font_size=9)

    # Title
    info_y -= 30
    c.setFillColor(primary_color)
    c.setFont("Helvetica-Bold", 12)
    c.drawString(left_margin, info_y, "Weitere Informationen und Hinweise")

    info_y -= 25

    # Paragraph 1
    c.setFillColor(black)
    text = "Sollten Sie den Betrag bereits überwiesen haben, betrachten Sie dieses Schreiben bitte als gegenstandslos. In diesem Fall bitten wir um Entschuldigung für die Unannehmlichkeiten."
    info_y = draw_justified_paragraph(c, text, left_margin, info_y, text_width, font_size=9)
    info_y -= 12

    # Paragraph 2
    text = "Falls Sie Fragen zu den Rechnungspositionen haben oder in einer finanziellen Notlage sind, bitten wir Sie, sich umgehend mit uns in Verbindung zu setzen. Wir sind gerne bereit, mit Ihnen eine Ratenzahlungsvereinbarung zu treffen."
    info_y = draw_justified_paragraph(c, text, left_margin, info_y, text_width, font_size=9)
    info_y -= 12

    # Paragraph 3
    text = "Bitte beachten Sie, dass bei Nichtzahlung weitere Kosten auf Sie zukommen können, einschließlich Zinsen, Anwaltskosten und Gerichtsgebühren. Diese können den ursprünglichen Rechnungsbetrag erheblich erhöhen."
    info_y = draw_justified_paragraph(c, text, left_margin, info_y, text_width, font_size=9)
    info_y -= 12

    # Paragraph 4
    text = "Wir möchten Sie darauf hinweisen, dass ein gerichtliches Mahnverfahren auch negative Auswirkungen auf Ihre Bonität haben kann. Dies kann zukünftige Geschäftsbeziehungen und Kreditwürdigkeitsprüfungen beeinflussen."
    info_y = draw_justified_paragraph(c, text, left_margin, info_y, text_width, font_size=9)
    info_y -= 12

    # Paragraph 5
    text = "Ihre Gesundheit liegt uns am Herzen, und wir möchten unsere gute Geschäftsbeziehung fortführen. Daher bitten wir Sie eindringlich, den offenen Betrag zu begleichen oder sich mit uns in Verbindung zu setzen, um eine Lösung zu finden."
    info_y = draw_justified_paragraph(c, text, left_margin, info_y, text_width, font_size=9)

    # Footer on page 2
    draw_modern_footer(c, left_margin, right_margin, 20*mm, include_bank_details=False)

    c.save()
    buffer.seek(0)
    return buffer.getvalue()


def create_sepa_mandate_pdf(
    customer_name: str,
    customer_address: str
) -> bytes:
    """
    Create a SEPA-Lastschriftmandat PDF with customer data filled in.

    Args:
        customer_name: Name of the customer
        customer_address: Full address of the customer

    Returns:
        PDF bytes
    """
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4

    # Parse customer address
    address_lines = customer_address.split('\n') if '\n' in customer_address else customer_address.split(',')
    address_lines = [line.strip() for line in address_lines if line.strip()]

    # Extract street and city from address
    street = address_lines[0] if len(address_lines) > 0 else ""
    city = address_lines[1] if len(address_lines) > 1 else ""

    # Startposition oben
    y_pos = height - 40*mm

    # Überschrift - SEPA-Basis-Lastschriftmandat
    c.setFont("Helvetica-Bold", 12)
    c.rect(20*mm, y_pos - 8*mm, 170*mm, 10*mm, stroke=1, fill=0)
    c.drawString(22*mm, y_pos - 5*mm, "SEPA-Basis-Lastschriftmandat")

    y_pos -= 15*mm

    # Zahlungsempfänger Box
    c.setFont("Helvetica", 7)
    c.drawString(20*mm, y_pos, "Name und Anschrift des Zahlungsempfängers (Gläubiger)")

    y_pos -= 7*mm
    c.rect(20*mm, y_pos - 20*mm, 90*mm, 25*mm, stroke=1, fill=0)

    c.setFont("Helvetica-Bold", 10)
    c.drawString(22*mm, y_pos - 5*mm, "Apotheke am Damm")
    c.setFont("Helvetica", 10)
    c.drawString(22*mm, y_pos - 10*mm, "Am Damm 17")
    c.drawString(22*mm, y_pos - 15*mm, "55232 Alzey")

    y_pos -= 28*mm

    # Gläubiger-ID und Mandatsreferenz
    c.rect(20*mm, y_pos - 8*mm, 90*mm, 10*mm, stroke=1, fill=0)
    c.setFont("Helvetica-Bold", 10)
    c.drawString(22*mm, y_pos - 5*mm, "DE45ZZZ00002778112")

    c.rect(112*mm, y_pos - 8*mm, 78*mm, 10*mm, stroke=1, fill=0)
    c.setFont("Helvetica-Bold", 9)
    c.drawString(114*mm, y_pos - 5*mm, "Wird separat mitgeteilt!")

    c.setFont("Helvetica", 6)
    c.drawString(22*mm, y_pos - 11*mm, "Gläubiger-Identifikationsnummer")
    c.drawString(114*mm, y_pos - 11*mm, "Mandatsreferenz")

    y_pos -= 18*mm

    # Ermächtigungstext
    c.setFont("Helvetica", 7)
    text_de = [
        "Ich ermächtige (Wir ermächtigen) die Apotheke am Damm,",
        "Zahlungen von meinem (unserem) Konto mittels Lastschrift",
        "einzuziehen. Zugleich weise ich mein (weisen wir unser)",
        "Kreditinstitut an, die von der Apotheke am Damm auf mein",
        "(unser) Konto gezogenen Lastschriften einzulösen.",
        "",
        "Hinweis: Ich kann (wir können) innerhalb von acht",
        "Wochen, beginnend mit dem Belastungsdatum, die",
        "Erstattung des belasteten Betrages verlangen. Es gelten",
        "dabei die mit meinem (unserem) Kreditinstitut vereinbarten",
        "Bedingungen."
    ]

    y_text = y_pos
    for line in text_de:
        c.drawString(22*mm, y_text, line)
        y_text -= 3.5*mm

    y_pos -= 45*mm

    # Checkboxen
    c.setFont("Helvetica", 8)
    # Wiederkehrende Zahlung
    c.rect(22*mm, y_pos - 3*mm, 4*mm, 4*mm, stroke=1, fill=0)
    c.drawString(28*mm, y_pos - 2*mm, "Wiederkehrende Zahlung")

    # Einmalige Zahlung
    c.rect(112*mm, y_pos - 3*mm, 4*mm, 4*mm, stroke=1, fill=0)
    c.drawString(118*mm, y_pos - 2*mm, "Einmalige Zahlung")

    y_pos -= 10*mm

    # Zahlungspflichtiger Felder (mit Daten gefüllt)
    c.setFont("Helvetica", 7)

    # Name
    c.drawString(20*mm, y_pos, "Zahlungspflichtiger")
    y_pos -= 3*mm
    c.rect(20*mm, y_pos - 5*mm, 170*mm, 7*mm, stroke=1, fill=0)
    c.setFont("Helvetica", 10)
    c.drawString(22*mm, y_pos - 3.5*mm, customer_name)
    c.setFont("Helvetica", 7)
    y_pos -= 10*mm

    # Straße und Hausnummer
    c.drawString(20*mm, y_pos, "Straße und Hausnummer")
    y_pos -= 3*mm
    c.rect(20*mm, y_pos - 5*mm, 170*mm, 7*mm, stroke=1, fill=0)
    c.setFont("Helvetica", 10)
    c.drawString(22*mm, y_pos - 3.5*mm, street)
    c.setFont("Helvetica", 7)
    y_pos -= 10*mm

    # PLZ und Ort
    c.drawString(20*mm, y_pos, "PLZ und Ort")
    y_pos -= 3*mm
    c.rect(20*mm, y_pos - 5*mm, 170*mm, 7*mm, stroke=1, fill=0)
    c.setFont("Helvetica", 10)
    c.drawString(22*mm, y_pos - 3.5*mm, city)
    c.setFont("Helvetica", 7)
    y_pos -= 10*mm

    # Land
    c.drawString(20*mm, y_pos, "Land")
    y_pos -= 3*mm
    c.rect(20*mm, y_pos - 5*mm, 170*mm, 7*mm, stroke=1, fill=0)
    y_pos -= 10*mm

    # IBAN
    c.drawString(20*mm, y_pos, "IBAN")
    y_pos -= 3*mm
    c.rect(20*mm, y_pos - 5*mm, 170*mm, 7*mm, stroke=1, fill=0)
    y_pos -= 10*mm

    # SWIFT BIC
    c.drawString(20*mm, y_pos, "SWIFT BIC")
    y_pos -= 3*mm
    c.rect(20*mm, y_pos - 5*mm, 170*mm, 7*mm, stroke=1, fill=0)
    y_pos -= 20*mm

    # Unterschriftenbereich
    c.setFont("Helvetica", 7)

    # Ort
    c.line(20*mm, y_pos, 70*mm, y_pos)
    c.drawString(20*mm, y_pos - 3*mm, "Ort")

    # Datum
    c.line(90*mm, y_pos, 125*mm, y_pos)
    c.drawString(90*mm, y_pos - 3*mm, "Datum")

    # Unterschrift
    c.line(140*mm, y_pos, 190*mm, y_pos)
    c.drawString(140*mm, y_pos - 3*mm, "Unterschrift(en)")

    c.save()
    buffer.seek(0)
    return buffer.getvalue()


def create_email_consent_form_pdf(customer_name: str) -> bytes:
    """
    Create an email consent form PDF with customer name filled in.

    Args:
        customer_name: Name of the customer

    Returns:
        PDF bytes
    """
    from reportlab.lib.colors import HexColor

    # Apotheken-Daten
    APOTHEKE_NAME = "Apotheke am Damm"
    APOTHEKE_STRASSE = "Am Damm 17"
    APOTHEKE_PLZ_ORT = "55232 Alzey"
    APOTHEKE_TELEFON = "06731-548846"
    APOTHEKE_EMAIL = "info@apothekeamdamm.de"

    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4

    # Farben
    primary_color = HexColor("#123C69")
    black = HexColor("#000000")

    # Startposition oben
    y_pos = height - 25*mm

    # ===== KOPFBEREICH =====
    c.setFillColor(primary_color)
    c.setFont("Helvetica-Bold", 16)
    c.drawString(20*mm, y_pos, APOTHEKE_NAME)

    y_pos -= 6*mm
    c.setFillColor(black)
    c.setFont("Helvetica", 9)
    c.drawString(20*mm, y_pos, f"{APOTHEKE_STRASSE} | {APOTHEKE_PLZ_ORT}")

    y_pos -= 18*mm

    # ===== ÜBERSCHRIFT =====
    c.setFillColor(primary_color)
    c.setFont("Helvetica", 11)
    c.drawCentredString(width/2, y_pos, "Ihre Rechnungen direkt per E-Mail")
    y_pos -= 8*mm
    c.setFont("Helvetica-Bold", 14)
    c.drawCentredString(width/2, y_pos, "Einwilligung zum elektronischen Rechnungsversand")

    y_pos -= 15*mm

    # ===== EINLEITUNGSTEXT =====
    c.setFillColor(black)
    c.setFont("Helvetica", 10)

    intro_text = [
        "Um Ressourcen zu schonen und Ihnen Ihre Rechnungen schneller zukommen zu lassen,",
        "bieten wir Ihnen gerne die Möglichkeit an, Ihre Rechnungen per E-Mail zu erhalten."
    ]

    for line in intro_text:
        c.drawString(20*mm, y_pos, line)
        y_pos -= 5*mm

    y_pos -= 8*mm

    # ===== VORTEILE-BOX =====
    box_height = 30*mm
    c.setFillColor(HexColor("#f0f4f8"))
    c.rect(20*mm, y_pos - box_height + 5*mm, 170*mm, box_height, stroke=0, fill=1)

    c.setFillColor(primary_color)
    c.setFont("Helvetica-Bold", 11)
    c.drawString(25*mm, y_pos, "Ihre Vorteile:")

    y_pos -= 7*mm
    c.setFillColor(black)
    c.setFont("Helvetica", 10)

    vorteile = [
        "Schnellerer Erhalt Ihrer Rechnungen",
        "Umweltfreundlich durch Papiereinsparung",
        "Übersichtliche digitale Ablage möglich"
    ]

    for vorteil in vorteile:
        c.drawString(30*mm, y_pos, f"• {vorteil}")
        y_pos -= 5.5*mm

    y_pos -= 10*mm

    # ===== DATENSCHUTZINFORMATION =====
    c.setFillColor(primary_color)
    c.setFont("Helvetica-Bold", 11)
    c.drawString(20*mm, y_pos, "Datenschutzinformation")

    y_pos -= 7*mm
    c.setFillColor(black)
    c.setFont("Helvetica", 9)

    datenschutz_text = [
        "Mit Ihrer Einwilligung verarbeiten wir Ihre E-Mail-Adresse zum Zweck des Versands",
        "von Rechnungen. Die Rechtsgrundlage für diese Verarbeitung ist Ihre Einwilligung",
        "gemäß Art. 6 Abs. 1 lit. a DSGVO.",
        "",
        "Diese Einwilligung ist freiwillig. Sie können sie jederzeit ohne Angabe von Gründen",
        f"widerrufen, z.B. per E-Mail an {APOTHEKE_EMAIL} oder schriftlich an unsere",
        "Adresse. Der Widerruf berührt nicht die Rechtmäßigkeit der bis dahin erfolgten",
        "Verarbeitung. Nach einem Widerruf erhalten Sie Ihre Rechnungen wieder per Post."
    ]

    for line in datenschutz_text:
        c.drawString(20*mm, y_pos, line)
        y_pos -= 4.5*mm

    y_pos -= 12*mm

    # ===== EINWILLIGUNGSERKLÄRUNG =====
    c.setStrokeColor(black)
    c.rect(20*mm, y_pos - 3*mm, 5*mm, 5*mm, stroke=1, fill=0)

    c.setFont("Helvetica-Bold", 10)
    einwilligung_text = f"Ja, ich willige ein, dass die {APOTHEKE_NAME} mir Rechnungen per E-Mail zusendet."
    c.drawString(28*mm, y_pos, einwilligung_text)

    y_pos -= 18*mm

    # ===== EINGABEFELDER =====
    c.setFont("Helvetica", 9)
    field_width = 120*mm

    # Name, Vorname
    c.drawString(20*mm, y_pos, "Name, Vorname:")
    c.line(55*mm, y_pos - 1*mm, 55*mm + field_width, y_pos - 1*mm)
    # Kundenname vorausfüllen
    if customer_name:
        c.setFont("Helvetica", 10)
        c.drawString(56*mm, y_pos, customer_name)
        c.setFont("Helvetica", 9)
    y_pos -= 12*mm

    # E-Mail-Adresse
    c.drawString(20*mm, y_pos, "E-Mail-Adresse:")
    c.line(55*mm, y_pos - 1*mm, 55*mm + field_width, y_pos - 1*mm)
    y_pos -= 12*mm

    # Ort, Datum
    c.drawString(20*mm, y_pos, "Ort, Datum:")
    c.line(45*mm, y_pos - 1*mm, 45*mm + 60*mm, y_pos - 1*mm)
    y_pos -= 18*mm

    # Unterschrift
    c.drawString(20*mm, y_pos, "Unterschrift:")
    c.line(45*mm, y_pos - 1*mm, 45*mm + 80*mm, y_pos - 1*mm)

    y_pos -= 30*mm

    # ===== FUSSBEREICH (feste Position am unteren Rand) =====
    footer_y = 20*mm  # Feste Position vom unteren Rand

    # Trennlinie
    c.setStrokeColor(HexColor("#cccccc"))
    c.setDash(2, 2)
    c.line(20*mm, footer_y + 10*mm, width - 20*mm, footer_y + 10*mm)
    c.setDash()
    c.setStrokeColor(black)

    # Fußzeile horizontal ueber die gesamte Breite
    c.setFillColor(primary_color)
    c.setFont("Helvetica-Bold", 9)
    c.drawString(20*mm, footer_y, APOTHEKE_NAME)

    c.setFillColor(black)
    c.setFont("Helvetica", 8)
    # Alle Infos in einer Zeile mit Trennzeichen
    footer_text = f"{APOTHEKE_STRASSE}, {APOTHEKE_PLZ_ORT}  |  Tel: {APOTHEKE_TELEFON}  |  {APOTHEKE_EMAIL}"
    c.drawString(65*mm, footer_y, footer_text)

    c.save()
    buffer.seek(0)
    return buffer.getvalue()


@dataclass
class InvoiceRow:
    id: int
    invoice_number: Optional[str]
    invoice_date: str
    customer_name: str
    customer_address: str  # Deprecated: use customer_street and customer_city
    amount_cents: int
    status: str  # 'open' or 'paid'
    last_seen_snapshot: str  # Last snapshot where this invoice appeared
    first_seen_snapshot: str  # First snapshot where this invoice appeared
    file_path: Optional[str] = None  # Path in the latest/last snapshot
    in_collective_invoice: bool = False  # Whether this invoice is in a collective invoice
    uncollectible: int = 0  # Whether this invoice is marked as uncollectible
    customer_street: Optional[str] = None
    customer_city: Optional[str] = None
    address_incomplete: bool = False  # Whether the address was auto-completed
    name_needs_review: bool = False  # Whether customer name failed AI validation

    @property
    def amount_eur(self) -> float:
        return self.amount_cents / 100

    @property
    def is_paid(self) -> bool:
        return self.status == 'paid'


@dataclass
class ReminderInfo:
    """Information about reminders for an invoice."""
    invoice_id: int
    last_reminder_level: Optional[int]  # 0=Zahlungserinnerung, 1=1. Mahnung, 2=2. Mahnung
    last_reminder_date: Optional[str]
    letterexpress_status: Optional[str]
    has_reminders: bool = False


@dataclass
class InvoiceWithReminder(InvoiceRow):
    """Extended invoice row with reminder information."""
    months_open: int = 0
    recommended_level: Optional[int] = None  # Next recommended reminder level
    last_reminder_level: Optional[int] = None
    last_reminder_date: Optional[str] = None
    letterexpress_status: Optional[str] = None
    has_reminders: bool = False
    reminder_pdf_path: Optional[str] = None
    invoices_in_group: int = 1  # Number of invoices in the same reminder group (same PDF)

    @property
    def reminder_status_text(self) -> str:
        """Human-readable reminder status."""
        if not self.has_reminders:
            if self.months_open >= 4:
                return "2. Mahnung fällig (Einschreiben)"
            elif self.months_open >= 3:
                return "Zahlungserinnerung empfohlen"
            else:
                return "Keine Mahnung erforderlich"
        else:
            level_names = {0: "Zahlungserinnerung", 1: "1. Mahnung", 2: "2. Mahnung"}
            return f"{level_names.get(self.last_reminder_level, 'Unbekannt')} gesendet"


def create_app(config: Optional[dict] = None) -> Flask:
    app = Flask(__name__)
    app.config.from_mapping(
        DATABASE=str(DEFAULT_DB_PATH),
        INVOICE_ROOT=str(DEFAULT_INVOICE_ROOT),
        MAX_LIMIT=DEFAULT_LIMIT,
        TEMPLATES_AUTO_RELOAD=True,
    )
    if config:
        app.config.update(config)

    # Configure logging with file handler for errors
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

        # Add file handler for errors
        file_handler = logging.FileHandler("import_errors.log", encoding="utf-8")
        file_handler.setLevel(logging.ERROR)
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
        )
        logging.getLogger().addHandler(file_handler)

    # Initialize database tables if they don't exist
    conn = sqlite3.connect(app.config["DATABASE"])
    init_db(conn)
    conn.commit()
    conn.close()

    # Custom filter for German date format
    @app.template_filter('german_date')
    def german_date_filter(iso_date: str) -> str:
        """Convert ISO date (YYYY-MM-DD) to German format (DD.MM.YYYY)."""
        try:
            from datetime import datetime
            dt = datetime.strptime(iso_date, "%Y-%m-%d")
            return dt.strftime("%d.%m.%Y")
        except (ValueError, TypeError):
            return iso_date

    @app.template_filter('german_month')
    def german_month_filter(snapshot_date: str) -> str:
        """Convert snapshot date (YYYY-MM) to German format (MM.YYYY)."""
        try:
            if not snapshot_date:
                return snapshot_date
            parts = snapshot_date.split('-')
            if len(parts) == 2:
                return f"{parts[1]}.{parts[0]}"
            return snapshot_date
        except (ValueError, TypeError, AttributeError):
            return snapshot_date

    # Prevent browser caching of HTML responses
    @app.after_request
    def add_no_cache_headers(response):
        """Add headers to prevent browser caching of HTML pages."""
        if response.content_type and 'text/html' in response.content_type:
            response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
            response.headers['Pragma'] = 'no-cache'
            response.headers['Expires'] = '0'
        return response

    @app.route("/")
    def dashboard() -> Response:
        """Statistics dashboard - overview page with invoice statistics."""
        conn = sqlite3.connect(app.config["DATABASE"])
        conn.row_factory = sqlite3.Row

        try:
            # Get overall statistics
            stats_query = """
            WITH invoice_status AS (
                SELECT
                    i.id,
                    i.customer_name,
                    i.amount_cents,
                    CASE
                        WHEN EXISTS (
                            SELECT 1 FROM invoice_snapshots isnap2
                            JOIN snapshots s2 ON isnap2.snapshot_id = s2.id
                            WHERE isnap2.invoice_id = i.id
                            AND s2.snapshot_date = (SELECT MAX(snapshot_date) FROM snapshots)
                        ) THEN 'open'
                        ELSE 'paid'
                    END AS status
                FROM invoices i
            )
            SELECT
                COUNT(CASE WHEN status = 'open' THEN 1 END) as open_count,
                SUM(CASE WHEN status = 'open' THEN amount_cents ELSE 0 END) / 100.0 as open_total,
                COUNT(CASE WHEN status = 'paid' THEN 1 END) as paid_count,
                SUM(CASE WHEN status = 'paid' THEN amount_cents ELSE 0 END) / 100.0 as paid_total,
                COUNT(DISTINCT customer_name) as unique_customers
            FROM invoice_status
            """
            stats = conn.execute(stats_query).fetchone()

            # Get top 10 customers by open amounts
            top_customers_query = """
            WITH invoice_status AS (
                SELECT
                    i.id,
                    i.customer_name,
                    i.amount_cents,
                    CASE
                        WHEN EXISTS (
                            SELECT 1 FROM invoice_snapshots isnap2
                            JOIN snapshots s2 ON isnap2.snapshot_id = s2.id
                            WHERE isnap2.invoice_id = i.id
                            AND s2.snapshot_date = (SELECT MAX(snapshot_date) FROM snapshots)
                        ) THEN 'open'
                        ELSE 'paid'
                    END AS status
                FROM invoices i
            )
            SELECT
                customer_name as name,
                COUNT(*) as count,
                SUM(amount_cents) / 100.0 as total
            FROM invoice_status
            WHERE status = 'open'
            GROUP BY customer_name
            ORDER BY total DESC
            LIMIT 10
            """
            top_customers = [dict(row) for row in conn.execute(top_customers_query).fetchall()]

            # Get snapshots overview
            snapshots_query = """
            SELECT
                s.snapshot_date as date,
                s.folder_name as folder,
                COUNT(DISTINCT isnap.invoice_id) as count
            FROM snapshots s
            LEFT JOIN invoice_snapshots isnap ON s.id = isnap.snapshot_id
            GROUP BY s.id, s.snapshot_date, s.folder_name
            ORDER BY s.snapshot_date DESC
            LIMIT 12
            """
            snapshots = [dict(row) for row in conn.execute(snapshots_query).fetchall()]

            # Get latest snapshot date
            latest_snapshot_query = "SELECT MAX(snapshot_date) as latest FROM snapshots"
            latest_result = conn.execute(latest_snapshot_query).fetchone()
            latest_snapshot = latest_result['latest'] if latest_result else None

            # Get reminder success statistics (paid invoices that had reminders)
            # Only count the LAST reminder level per invoice to avoid double-counting
            reminder_success_query = """
            WITH last_two_snapshots AS (
                SELECT snapshot_date
                FROM snapshots
                ORDER BY snapshot_date DESC
                LIMIT 2
            ),
            invoice_status AS (
                SELECT
                    i.id,
                    i.amount_cents,
                    CASE
                        WHEN EXISTS (
                            SELECT 1 FROM invoice_snapshots isnap2
                            JOIN snapshots s2 ON isnap2.snapshot_id = s2.id
                            WHERE isnap2.invoice_id = i.id
                            AND s2.snapshot_date = (SELECT MAX(snapshot_date) FROM snapshots)
                        ) THEN 'open'
                        ELSE 'paid'
                    END AS status
                FROM invoices i
            ),
            last_reminder_per_invoice AS (
                SELECT
                    invoice_id,
                    MAX(created_at) as max_created
                FROM reminders
                GROUP BY invoice_id
            ),
            reminded_and_paid AS (
                SELECT
                    r.reminder_level,
                    i.amount_cents,
                    r.created_at,
                    (SELECT snapshot_date FROM last_two_snapshots ORDER BY snapshot_date DESC LIMIT 1) as last_month,
                    (SELECT snapshot_date FROM last_two_snapshots ORDER BY snapshot_date DESC LIMIT 1 OFFSET 1) as second_last_month
                FROM reminders r
                INNER JOIN last_reminder_per_invoice lrpi ON r.invoice_id = lrpi.invoice_id AND r.created_at = lrpi.max_created
                JOIN invoices i ON r.invoice_id = i.id
                JOIN invoice_status ist ON i.id = ist.id
                WHERE ist.status = 'paid'
            )
            SELECT
                reminder_level,
                -- Last month
                COUNT(CASE WHEN strftime('%Y-%m', created_at) = strftime('%Y-%m', last_month) THEN 1 END) as last_month_count,
                SUM(CASE WHEN strftime('%Y-%m', created_at) = strftime('%Y-%m', last_month) THEN amount_cents ELSE 0 END) / 100.0 as last_month_total,
                -- Second last month
                COUNT(CASE WHEN strftime('%Y-%m', created_at) = strftime('%Y-%m', second_last_month) THEN 1 END) as second_last_month_count,
                SUM(CASE WHEN strftime('%Y-%m', created_at) = strftime('%Y-%m', second_last_month) THEN amount_cents ELSE 0 END) / 100.0 as second_last_month_total,
                -- All time
                COUNT(*) as total_count,
                SUM(amount_cents) / 100.0 as total_amount
            FROM reminded_and_paid
            GROUP BY reminder_level
            ORDER BY reminder_level
            """
            reminder_success_rows = conn.execute(reminder_success_query).fetchall()

            # Organize reminder success data by level
            reminder_success = {
                'level_0': {'last_month_count': 0, 'last_month_total': 0.0, 'second_last_month_count': 0, 'second_last_month_total': 0.0, 'total_count': 0, 'total_amount': 0.0},
                'level_1': {'last_month_count': 0, 'last_month_total': 0.0, 'second_last_month_count': 0, 'second_last_month_total': 0.0, 'total_count': 0, 'total_amount': 0.0},
                'level_2': {'last_month_count': 0, 'last_month_total': 0.0, 'second_last_month_count': 0, 'second_last_month_total': 0.0, 'total_count': 0, 'total_amount': 0.0}
            }

            for row in reminder_success_rows:
                level_key = f"level_{row['reminder_level']}"
                reminder_success[level_key] = {
                    'last_month_count': row['last_month_count'] or 0,
                    'last_month_total': row['last_month_total'] or 0.0,
                    'second_last_month_count': row['second_last_month_count'] or 0,
                    'second_last_month_total': row['second_last_month_total'] or 0.0,
                    'total_count': row['total_count'] or 0,
                    'total_amount': row['total_amount'] or 0.0
                }

            # Get the last two snapshot dates for display
            last_two_dates_query = """
            SELECT snapshot_date
            FROM snapshots
            ORDER BY snapshot_date DESC
            LIMIT 2
            """
            snapshot_dates = [row['snapshot_date'] for row in conn.execute(last_two_dates_query).fetchall()]
            last_month_name = snapshot_dates[0] if len(snapshot_dates) > 0 else None
            second_last_month_name = snapshot_dates[1] if len(snapshot_dates) > 1 else None

            # Get currently open reminders (unpaid invoices with reminders)
            open_reminders_query = """
            WITH invoice_status AS (
                SELECT
                    i.id,
                    i.amount_cents,
                    CASE
                        WHEN EXISTS (
                            SELECT 1 FROM invoice_snapshots isnap2
                            JOIN snapshots s2 ON isnap2.snapshot_id = s2.id
                            WHERE isnap2.invoice_id = i.id
                            AND s2.snapshot_date = (SELECT MAX(snapshot_date) FROM snapshots)
                        ) THEN 'open'
                        ELSE 'paid'
                    END AS status
                FROM invoices i
            ),
            last_reminder_per_invoice AS (
                SELECT
                    invoice_id,
                    MAX(reminder_level) as last_reminder_level
                FROM reminders
                GROUP BY invoice_id
            )
            SELECT
                lr.last_reminder_level as reminder_level,
                COUNT(*) as count,
                SUM(i.amount_cents) / 100.0 as total
            FROM invoices i
            JOIN invoice_status ist ON i.id = ist.id
            JOIN last_reminder_per_invoice lr ON i.id = lr.invoice_id
            WHERE ist.status = 'open'
            GROUP BY lr.last_reminder_level
            ORDER BY lr.last_reminder_level
            """
            open_reminders_rows = conn.execute(open_reminders_query).fetchall()

            # Organize open reminders data by level
            open_reminders = {
                'level_0': {'count': 0, 'total': 0.0},
                'level_1': {'count': 0, 'total': 0.0},
                'level_2': {'count': 0, 'total': 0.0}
            }

            for row in open_reminders_rows:
                level_key = f"level_{row['reminder_level']}"
                open_reminders[level_key] = {
                    'count': row['count'] or 0,
                    'total': row['total'] or 0.0
                }

            # Build stats dictionary for template
            dashboard_stats = {
                'open_count': stats['open_count'] or 0,
                'open_total': stats['open_total'] or 0.0,
                'paid_count': stats['paid_count'] or 0,
                'paid_total': stats['paid_total'] or 0.0,
                'unique_customers': stats['unique_customers'] or 0,
                'top_customers': top_customers,
                'snapshots': snapshots,
                'latest_snapshot': latest_snapshot,
                'reminder_success': reminder_success,
                'open_reminders': open_reminders,
                'last_month_name': last_month_name,
                'second_last_month_name': second_last_month_name
            }

            return render_template("dashboard.html", stats=dashboard_stats)

        finally:
            conn.close()

    @app.route("/mahnungen")
    def mahnungen() -> Response:
        """Mahnungen overview page with 4 tabs by reminder status."""
        view = request.args.get("view", "unbemahnt")  # 'unbemahnt', 'zahlungserinnerung', '1_mahnung', '2_mahnung'
        show_uncollectible = request.args.get("show_uncollectible", "false").lower() == "true"
        hide_never_remind = request.args.get("hide_never_remind", "true").lower() == "true"  # Default: hide customers with never_remind=1
        only_actionable = request.args.get("only_actionable", "true").lower() == "true"  # Default: show only invoices that need action (have recommendation)

        # Fetch LetterXpress status from database
        letterxpress_status = {}
        try:
            with sqlite3.connect(app.config["DATABASE"]) as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute(
                    "SELECT pdf_path, letterxpress_job_id, mode, submitted_at FROM mahnungen_letterxpress"
                ).fetchall()
                for row in rows:
                    # Format timestamp for display
                    submitted_at = row["submitted_at"]
                    try:
                        dt = datetime.fromisoformat(submitted_at.replace('Z', '+00:00'))
                        formatted_date = dt.strftime("%d.%m.%Y %H:%M")
                    except:
                        formatted_date = submitted_at

                    letterxpress_status[row["pdf_path"]] = {
                        "job_id": row["letterxpress_job_id"],
                        "mode": row["mode"],
                        "submitted_at": formatted_date
                    }
        except Exception as e:
            logging.error(f"Failed to fetch LetterXpress status for mahnungen: {e}")

        # Fetch all invoices to calculate tab counts
        all_unbemahnt = fetch_invoices_with_reminders(app.config["DATABASE"], filter_reminded=False, hide_never_remind=hide_never_remind)
        # Filter for actionable invoices (those with a recommendation) if only_actionable is True
        if only_actionable:
            unbemahnt_invoices = [inv for inv in all_unbemahnt if inv.recommended_level is not None]
        else:
            unbemahnt_invoices = all_unbemahnt
        all_reminded = fetch_invoices_with_reminders(app.config["DATABASE"], filter_reminded=True, hide_never_remind=hide_never_remind)
        zahlungserinnerung_invoices = [inv for inv in all_reminded if inv.last_reminder_level == 0]
        mahnung_1_invoices = [inv for inv in all_reminded if inv.last_reminder_level == 1]
        mahnung_2_invoices_all = [inv for inv in all_reminded if inv.last_reminder_level == 2]

        # For 2. Mahnung view: filter uncollectible invoices unless explicitly shown
        if view == "2_mahnung" and not show_uncollectible:
            mahnung_2_invoices = [inv for inv in mahnung_2_invoices_all if not inv.uncollectible]
        else:
            mahnung_2_invoices = mahnung_2_invoices_all

        # Calculate tab counts for badges (always show total including uncollectible)
        tab_counts = {
            'unbemahnt': len(unbemahnt_invoices),
            'zahlungserinnerung': len(zahlungserinnerung_invoices),
            '1_mahnung': len(mahnung_1_invoices),
            '2_mahnung': len(mahnung_2_invoices_all),  # Total count
        }

        # Select invoices based on current view
        if view == "unbemahnt":
            invoices = unbemahnt_invoices
        elif view == "zahlungserinnerung":
            invoices = zahlungserinnerung_invoices
        elif view == "1_mahnung":
            invoices = mahnung_1_invoices
        elif view == "2_mahnung":
            invoices = mahnung_2_invoices
        else:
            # Default to unbemahnt
            invoices = unbemahnt_invoices

        # Group invoices by customer
        from collections import defaultdict
        customer_groups = defaultdict(list)
        for inv in invoices:
            customer_groups[inv.customer_name].append(inv)

        # Convert to sorted list with reminder grouping
        grouped_invoices = []
        for customer_name in sorted(customer_groups.keys()):
            customer_invoices = sorted(customer_groups[customer_name], key=lambda x: x.invoice_date)

            # For views with reminders, group invoices by reminder_pdf_path
            if view != 'unbemahnt':
                # Group by reminder_pdf_path
                reminder_groups = defaultdict(list)
                for inv in customer_invoices:
                    # Use reminder_pdf_path as grouping key, or invoice id if no pdf
                    group_key = inv.reminder_pdf_path if inv.reminder_pdf_path else f"single_{inv.id}"
                    reminder_groups[group_key].append(inv)

                # Create invoice groups (each group can be a single invoice or multiple bundled invoices)
                invoice_groups = []
                for group_key, group_invoices in reminder_groups.items():
                    if len(group_invoices) > 1:
                        # Multiple invoices bundled together
                        invoice_groups.append({
                            "is_group": True,
                            "invoices": group_invoices,
                            "reminder_pdf_path": group_key,
                            "total_amount": sum(inv.amount_eur for inv in group_invoices),
                            "count": len(group_invoices),
                        })
                    else:
                        # Single invoice
                        invoice_groups.append({
                            "is_group": False,
                            "invoices": group_invoices,
                            "reminder_pdf_path": group_key if not group_key.startswith("single_") else None,
                        })

                grouped_invoices.append({
                    "customer_name": customer_name,
                    "customer_address": customer_invoices[0].customer_address,
                    "invoice_groups": invoice_groups,
                    "invoices": customer_invoices,  # Keep for compatibility
                })
            else:
                # For unbemahnt view, keep simple list
                grouped_invoices.append({
                    "customer_name": customer_name,
                    "customer_address": customer_invoices[0].customer_address,
                    "invoices": customer_invoices,
                })

        # Calculate statistics
        total_amount = sum(inv.amount_eur for inv in invoices)
        count_by_level = {0: 0, 1: 0, 2: 0, None: 0}

        # For unbemahnt view, show recommended levels
        if view == "unbemahnt":
            for inv in invoices:
                count_by_level[inv.recommended_level] = count_by_level.get(inv.recommended_level, 0) + 1

        stats = {
            "total_count": len(invoices),
            "total_amount": total_amount,
            "count_by_level": count_by_level,
            "tab_counts": tab_counts,
        }

        return render_template(
            "mahnungen.html",
            customer_groups=grouped_invoices,
            view=view,
            stats=stats,
            show_uncollectible=show_uncollectible,
            hide_never_remind=hide_never_remind,
            only_actionable=only_actionable,
            letterxpress_status=letterxpress_status
        )

    @app.route("/vorlagen")
    def vorlagen() -> Response:
        """Templates page for payment reminder and dunning letter templates."""
        return render_template("vorlagen.html")

    @app.route("/personenverwaltung")
    def personenverwaltung() -> Response:
        """Customer management page."""
        customers = fetch_all_customers(app.config["DATABASE"])
        return render_template("personenverwaltung.html", customers=customers)

    @app.route("/letterxpress")
    def letterxpress() -> Response:
        """LetterXpress management page."""
        return render_template("letterxpress.html")

    @app.route("/api/customers/<path:customer_name>", methods=["PUT"])
    def update_customer(customer_name: str) -> Response:
        """Update customer details (salutation, email, notes, never_remind, bank_debit, print_only flags, and custom name/address)."""
        data = request.get_json()

        if not data:
            return jsonify({"success": False, "error": "Keine Daten empfangen"}), 400

        salutation = data.get("salutation", "")
        email = data.get("email", "")
        notes = data.get("notes", "")
        never_remind = 1 if data.get("never_remind", False) else 0
        bank_debit = 1 if data.get("bank_debit", False) else 0
        print_only = 1 if data.get("print_only", False) else 0
        clear_address_incomplete = data.get("clear_address_incomplete", False)
        clear_name_needs_review = data.get("clear_name_needs_review", False)

        # Check if custom_* fields were explicitly sent (from modal)
        # If not sent, we should preserve existing values
        has_custom_fields = "custom_name" in data

        try:
            with sqlite3.connect(app.config["DATABASE"]) as conn:
                init_db(conn)

                if has_custom_fields:
                    # Full update including custom fields (from modal)
                    custom_name = data.get("custom_name", "")
                    custom_street = data.get("custom_street", "")
                    custom_city = data.get("custom_city", "")

                    conn.execute(
                        """
                        INSERT INTO customer_details (customer_name, salutation, email, notes, never_remind, bank_debit, print_only, custom_name, custom_street, custom_city, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now', 'localtime'))
                        ON CONFLICT(customer_name) DO UPDATE SET
                            salutation = excluded.salutation,
                            email = excluded.email,
                            notes = excluded.notes,
                            never_remind = excluded.never_remind,
                            bank_debit = excluded.bank_debit,
                            print_only = excluded.print_only,
                            custom_name = excluded.custom_name,
                            custom_street = excluded.custom_street,
                            custom_city = excluded.custom_city,
                            updated_at = datetime('now', 'localtime')
                        """,
                        (customer_name, salutation, email, notes, never_remind, bank_debit, print_only, custom_name, custom_street, custom_city)
                    )
                else:
                    # Partial update - preserve existing custom_* fields (from table save)
                    conn.execute(
                        """
                        INSERT INTO customer_details (customer_name, salutation, email, notes, never_remind, bank_debit, print_only, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now', 'localtime'))
                        ON CONFLICT(customer_name) DO UPDATE SET
                            salutation = excluded.salutation,
                            email = excluded.email,
                            notes = excluded.notes,
                            never_remind = excluded.never_remind,
                            bank_debit = excluded.bank_debit,
                            print_only = excluded.print_only,
                            updated_at = datetime('now', 'localtime')
                        """,
                        (customer_name, salutation, email, notes, never_remind, bank_debit, print_only)
                    )

                # If user wants to clear the address_incomplete flag, update all invoices for this customer
                if clear_address_incomplete:
                    conn.execute(
                        """
                        UPDATE invoices
                        SET address_incomplete = 0
                        WHERE customer_name = ?
                        """,
                        (customer_name,)
                    )

                # If user wants to clear the name_needs_review flag, or if a custom_name was set
                # (automatically clear when user provides a custom name)
                if clear_name_needs_review or (has_custom_fields and custom_name):
                    conn.execute(
                        """
                        UPDATE invoices
                        SET name_needs_review = 0
                        WHERE customer_name = ?
                        """,
                        (customer_name,)
                    )

                conn.commit()

                return jsonify({
                    "success": True,
                    "message": "Kundendaten wurden aktualisiert"
                })
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500

    @app.route("/api/determine-salutations", methods=["POST"])
    def determine_salutations() -> Response:
        """Automatically determine salutations for all customers without salutation using AI."""
        try:
            with sqlite3.connect(app.config["DATABASE"]) as conn:
                conn.row_factory = sqlite3.Row
                init_db(conn)

                # Get all unique customers without salutation
                customers_query = """
                    SELECT DISTINCT i.customer_name
                    FROM invoices i
                    LEFT JOIN customer_details cd ON i.customer_name = cd.customer_name
                    WHERE cd.salutation IS NULL OR cd.salutation = ''
                    ORDER BY i.customer_name
                """
                customers = conn.execute(customers_query).fetchall()

                success_count = 0
                failed_count = 0
                results = []

                for customer_row in customers:
                    customer_name = customer_row["customer_name"]

                    # Determine salutation via AI
                    salutation = determine_salutation_for_customer(customer_name)

                    if salutation:
                        # Update database
                        conn.execute(
                            """
                            INSERT INTO customer_details (customer_name, salutation, updated_at)
                            VALUES (?, ?, datetime('now', 'localtime'))
                            ON CONFLICT(customer_name) DO UPDATE SET
                                salutation = excluded.salutation,
                                updated_at = datetime('now', 'localtime')
                            """,
                            (customer_name, salutation)
                        )
                        conn.commit()
                        success_count += 1
                        results.append({
                            "customer_name": customer_name,
                            "salutation": salutation,
                            "status": "success"
                        })
                        logging.info(f"Set salutation for {customer_name}: {salutation}")
                    else:
                        failed_count += 1
                        results.append({
                            "customer_name": customer_name,
                            "salutation": None,
                            "status": "failed"
                        })
                        logging.warning(f"Could not determine salutation for {customer_name}")

                return jsonify({
                    "success": True,
                    "total": len(customers),
                    "success_count": success_count,
                    "failed_count": failed_count,
                    "results": results
                })

        except Exception as e:
            logging.error(f"Error determining salutations: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    @app.route("/api/batch-salutations-stream", methods=["GET"])
    def batch_salutations_stream() -> Response:
        """
        Stream-based batch salutation determination after import.
        Uses SSE to report progress while processing names in batches.
        """
        def generate():
            try:
                with sqlite3.connect(app.config["DATABASE"]) as conn:
                    conn.row_factory = sqlite3.Row
                    init_db(conn)

                    # Get all unique customers without salutation
                    customers_query = """
                        SELECT DISTINCT i.customer_name
                        FROM invoices i
                        LEFT JOIN customer_details cd ON i.customer_name = cd.customer_name
                        WHERE cd.salutation IS NULL OR cd.salutation = ''
                        ORDER BY i.customer_name
                    """
                    customers = conn.execute(customers_query).fetchall()
                    total = len(customers)

                    if total == 0:
                        yield f"data: {json.dumps({'type': 'complete', 'total': 0, 'success': 0, 'message': 'Keine neuen Kunden ohne Anrede'})}\n\n"
                        return

                    yield f"data: {json.dumps({'type': 'start', 'total': total})}\n\n"

                    # Extract first names and build mapping
                    name_to_customer = {}
                    first_names = []
                    for customer_row in customers:
                        customer_name = customer_row["customer_name"]
                        first_name = extract_first_name(customer_name)
                        if first_name:
                            # Use first_name as key, but could have duplicates
                            if first_name not in name_to_customer:
                                name_to_customer[first_name] = []
                            name_to_customer[first_name].append(customer_name)
                            if first_name not in first_names:
                                first_names.append(first_name)

                    if not first_names:
                        yield f"data: {json.dumps({'type': 'complete', 'total': total, 'success': 0, 'message': 'Keine Vornamen extrahierbar'})}\n\n"
                        return

                    # Process in batches of 20 names
                    batch_size = 20
                    success_count = 0
                    processed = 0

                    for i in range(0, len(first_names), batch_size):
                        batch = first_names[i:i + batch_size]

                        yield f"data: {json.dumps({'type': 'progress', 'processed': processed, 'total': total, 'batch': batch})}\n\n"

                        # Call batch AI
                        results = determine_genders_batch_via_ai(batch)

                        # Update database for each result
                        for first_name, salutation in results.items():
                            if salutation and first_name in name_to_customer:
                                for customer_name in name_to_customer[first_name]:
                                    conn.execute(
                                        """
                                        INSERT INTO customer_details (customer_name, salutation, updated_at)
                                        VALUES (?, ?, datetime('now', 'localtime'))
                                        ON CONFLICT(customer_name) DO UPDATE SET
                                            salutation = excluded.salutation,
                                            updated_at = datetime('now', 'localtime')
                                        """,
                                        (customer_name, salutation)
                                    )
                                    success_count += 1
                                    processed += 1
                            else:
                                if first_name in name_to_customer:
                                    processed += len(name_to_customer[first_name])

                        conn.commit()

                    yield f"data: {json.dumps({'type': 'complete', 'total': total, 'success': success_count, 'message': f'{success_count} Anreden ermittelt'})}\n\n"

            except Exception as e:
                logging.error(f"Error in batch salutations stream: {e}")
                yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"

        return Response(stream_with_context(generate()), mimetype="text/event-stream")

    @app.route("/api/batch-validate-names-stream", methods=["GET"])
    def batch_validate_names_stream() -> Response:
        """
        Stream-based batch name validation after import.
        Uses SSE to report progress while validating customer names in batches.
        """
        def generate():
            try:
                with sqlite3.connect(app.config["DATABASE"]) as conn:
                    conn.row_factory = sqlite3.Row
                    init_db(conn)

                    # Ensure customer_data table exists
                    conn.execute("""
                        CREATE TABLE IF NOT EXISTS customer_data (
                            customer_name TEXT PRIMARY KEY,
                            salutation TEXT,
                            email TEXT,
                            notes TEXT,
                            never_remind INTEGER DEFAULT 0,
                            bank_debit INTEGER DEFAULT 0,
                            print_only INTEGER DEFAULT 0,
                            custom_name TEXT,
                            custom_street TEXT,
                            custom_city TEXT
                        )
                    """)

                    # Get all unique customer names that haven't been validated yet
                    # (name_needs_review is NULL = never checked)
                    # Exclude customers with custom_name set (they were already manually corrected)
                    customers_query = """
                        SELECT DISTINCT i.customer_name
                        FROM invoices i
                        LEFT JOIN customer_data cd ON i.customer_name = cd.customer_name
                        WHERE i.name_needs_review IS NULL
                          AND (cd.custom_name IS NULL OR cd.custom_name = '')
                        ORDER BY i.customer_name
                    """
                    customers = conn.execute(customers_query).fetchall()
                    customer_names = [row["customer_name"] for row in customers]
                    total = len(customer_names)

                    if total == 0:
                        yield f"data: {json.dumps({'type': 'complete', 'total': 0, 'flagged': 0, 'message': 'Keine Namen zu validieren'})}\n\n"
                        return

                    yield f"data: {json.dumps({'type': 'start', 'total': total})}\n\n"

                    # Process in batches of 20 names
                    batch_size = 20
                    flagged_count = 0
                    processed = 0

                    for i in range(0, len(customer_names), batch_size):
                        batch = customer_names[i:i + batch_size]

                        yield f"data: {json.dumps({'type': 'progress', 'processed': processed, 'total': total, 'batch': batch})}\n\n"

                        # Call batch AI validation
                        results = validate_customer_names_batch_via_ai(batch)

                        # Update database for each result
                        for name, is_valid in results.items():
                            if not is_valid:
                                # Name is invalid - flag it
                                conn.execute(
                                    """
                                    UPDATE invoices
                                    SET name_needs_review = 1
                                    WHERE customer_name = ?
                                    """,
                                    (name,)
                                )
                                flagged_count += 1
                                logging.info(f"Flagged invalid name: {name}")
                            else:
                                # Name is valid - mark as checked (0 = validated OK)
                                conn.execute(
                                    """
                                    UPDATE invoices
                                    SET name_needs_review = 0
                                    WHERE customer_name = ?
                                    """,
                                    (name,)
                                )
                            processed += 1

                        conn.commit()

                    yield f"data: {json.dumps({'type': 'complete', 'total': total, 'flagged': flagged_count, 'message': f'{flagged_count} Namen zur Prüfung markiert'})}\n\n"

            except Exception as e:
                logging.error(f"Error in batch name validation stream: {e}")
                yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"

        return Response(stream_with_context(generate()), mimetype="text/event-stream")

    @app.route("/invoices")
    def index() -> Response:
        query = request.args.get("q", "").strip()
        limit = clamp_limit(request.args.get("limit"), app.config["MAX_LIMIT"])
        grouped = request.args.get("grouped", "").lower() == "true"

        # Separate time and status filters
        time_filter = request.args.get("time", "current_month")  # 'all', 'current_month', or 'custom'
        status_filter = request.args.get("status", "open")  # 'all', 'open', or 'paid'
        email_filter = request.args.get("email", "all")  # 'all', 'with_email', or 'without_email'
        uncollectible_filter = request.args.get("uncollectible", "hide")  # 'hide', 'show', or 'only'

        # Invoice date range filter (format: YYYY-MM-DD)
        invoice_date_from = request.args.get("invoice_date_from", "")
        invoice_date_to = request.args.get("invoice_date_to", "")

        # Custom date range parameters (format: YYYY-MM)
        from_month = request.args.get("from_month", "")
        to_month = request.args.get("to_month", "")

        sort_by, sort_direction = normalize_sort_params(
            request.args.get("sort", "date"),
            request.args.get("direction", "desc"),
        )

        invoices = fetch_invoices(
            app.config["DATABASE"],
            query,
            limit,
            time_filter,
            status_filter,
            from_month,
            to_month,
            email_filter,
            uncollectible_filter,
            sort_by,
            sort_direction,
            invoice_date_from=invoice_date_from,
            invoice_date_to=invoice_date_to,
        )
        total_amount = sum(row.amount_eur for row in invoices)

        # Get latest snapshot and date range for display
        with sqlite3.connect(app.config["DATABASE"]) as conn:
            latest_snapshot_row = conn.execute(
                "SELECT MAX(snapshot_date) as latest FROM snapshots"
            ).fetchone()
            latest_snapshot = latest_snapshot_row[0] if latest_snapshot_row and latest_snapshot_row[0] else None

            # Get min and max snapshot dates for custom date range helper
            date_range_row = conn.execute(
                "SELECT MIN(snapshot_date) as min_date, MAX(snapshot_date) as max_date FROM snapshots"
            ).fetchone()
            min_date = date_range_row[0] if date_range_row and date_range_row[0] else None
            max_date = date_range_row[1] if date_range_row and date_range_row[1] else None

            # Format to YYYY-MM for month input
            min_month = min_date[:7] if min_date else None
            max_month = max_date[:7] if max_date else None

        if grouped:
            grouped_data = group_by_customer(invoices)
            return render_template(
                "index.html",
                invoices=invoices,
                grouped_data=grouped_data,
                query=query,
                limit=limit,
                total_amount=total_amount,
                grouped=True,
                time_filter=time_filter,
                status_filter=status_filter,
                email_filter=email_filter,
                uncollectible_filter=uncollectible_filter,
                invoice_date_from=invoice_date_from,
                invoice_date_to=invoice_date_to,
                from_month=from_month,
                to_month=to_month,
                latest_snapshot=latest_snapshot,
                min_month=min_month,
                max_month=max_month,
                sort_by=sort_by,
                sort_direction=sort_direction,
            )
        else:
            return render_template(
                "index.html",
                invoices=invoices,
                grouped_data=None,
                query=query,
                limit=limit,
                total_amount=total_amount,
                grouped=False,
                time_filter=time_filter,
                status_filter=status_filter,
                email_filter=email_filter,
                uncollectible_filter=uncollectible_filter,
                invoice_date_from=invoice_date_from,
                invoice_date_to=invoice_date_to,
                from_month=from_month,
                to_month=to_month,
                latest_snapshot=latest_snapshot,
                min_month=min_month,
                max_month=max_month,
                sort_by=sort_by,
                sort_direction=sort_direction,
            )

    @app.route("/sammelrechnungen")
    def sammelrechnungen() -> Response:
        """Display all collective invoices from the Sammelrechnungen folder."""
        sammelrechnungen_dir = BASE_DIR / "Sammelrechnungen"

        # Fetch LetterXpress status, customer print_only flags, and rX selections from database
        letterxpress_status = {}
        customer_print_only = {}
        rx_selections = {}  # {(filename, month): True}
        try:
            with sqlite3.connect(app.config["DATABASE"]) as conn:
                conn.row_factory = sqlite3.Row
                init_db(conn)  # Ensure new table exists
                rows = conn.execute(
                    "SELECT filename, letterxpress_job_id, mode, submitted_at FROM sammelrechnungen_letterxpress"
                ).fetchall()
                for row in rows:
                    # Format timestamp for display
                    submitted_at = row["submitted_at"]
                    try:
                        dt = datetime.fromisoformat(submitted_at.replace('Z', '+00:00'))
                        formatted_date = dt.strftime("%d.%m.%Y %H:%M")
                    except:
                        formatted_date = submitted_at

                    letterxpress_status[row["filename"]] = {
                        "job_id": row["letterxpress_job_id"],
                        "mode": row["mode"],
                        "submitted_at": formatted_date
                    }

                # Fetch print_only status for all customers
                customer_rows = conn.execute(
                    "SELECT customer_name, custom_name, print_only FROM customer_details WHERE print_only = 1"
                ).fetchall()
                for row in customer_rows:
                    # Store both original name and normalized version (without parentheses)
                    # because filenames may have parentheses removed
                    import re
                    name = row["customer_name"]
                    customer_print_only[name] = True
                    # Also store version without parentheses
                    name_no_parens = re.sub(r'[()]', '', name).strip()
                    name_no_parens = re.sub(r'\s+', ' ', name_no_parens)  # collapse multiple spaces
                    customer_print_only[name_no_parens] = True
                    # Also check custom_name if set
                    if row["custom_name"]:
                        customer_print_only[row["custom_name"]] = True
                        custom_no_parens = re.sub(r'[()]', '', row["custom_name"]).strip()
                        custom_no_parens = re.sub(r'\s+', ' ', custom_no_parens)
                        customer_print_only[custom_no_parens] = True

                # Fetch rX selections
                rx_rows = conn.execute(
                    "SELECT filename, month FROM sammelrechnungen_rx WHERE selected = 1"
                ).fetchall()
                for row in rx_rows:
                    rx_selections[(row["filename"], row["month"])] = True
        except Exception as e:
            logging.error(f"Failed to fetch LetterXpress status: {e}")

        # Collect all collective invoices
        collective_invoices = []

        if sammelrechnungen_dir.exists():
            # Iterate through month folders (e.g., 2025-11)
            for month_folder in sorted(sammelrechnungen_dir.iterdir(), reverse=True):
                if month_folder.is_dir():
                    month = month_folder.name  # e.g., "2025-11"

                    # Iterate through PDF files in the month folder
                    for pdf_file in sorted(month_folder.glob("*.pdf")):
                        # Extract customer name from filename
                        # Format: Sammelrechnung_2025-11_Kundenname_YYYYMMDD_HHMMSS.pdf
                        filename = pdf_file.stem  # Remove .pdf extension
                        parts = filename.split("_")

                        # Remove "Sammelrechnung", month, and timestamp (last 2 parts: YYYYMMDD and HHMMSS)
                        if len(parts) >= 5:
                            # Join all parts between month and timestamp
                            customer_name = "_".join(parts[2:-2]).replace("_", " ")
                        elif len(parts) >= 3:
                            customer_name = parts[2].replace("_", " ")  # Fallback without timestamp
                        else:
                            customer_name = filename  # Fallback to full filename

                        # Get file stats
                        stat = pdf_file.stat()
                        created_at = datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
                        file_size_kb = stat.st_size / 1024

                        # Build relative path for PDF viewing
                        relative_path = pdf_file.relative_to(BASE_DIR)

                        # Get LetterXpress status for this file
                        lx_status = letterxpress_status.get(pdf_file.name, None)

                        # Check if customer has print_only flag
                        is_print_only = customer_print_only.get(customer_name, False)

                        # Check if rX is selected for this invoice
                        is_rx_selected = rx_selections.get((pdf_file.name, month), False)

                        collective_invoices.append({
                            "month": month,
                            "customer_name": customer_name,
                            "filename": pdf_file.name,
                            "created_at": created_at,
                            "file_size_kb": file_size_kb,
                            "relative_path": str(relative_path),
                            "letterxpress_status": lx_status,
                            "print_only": is_print_only,
                            "rx_selected": is_rx_selected
                        })

        # Group by month for better display
        grouped_by_month = defaultdict(list)
        for invoice in collective_invoices:
            grouped_by_month[invoice["month"]].append(invoice)

        # Sort each month's invoices by last name (last word of customer_name)
        def get_last_name(name: str) -> str:
            """Extract last name (last word) from customer name for sorting."""
            parts = name.strip().split()
            return parts[-1].lower() if parts else ""

        for month in grouped_by_month:
            grouped_by_month[month].sort(key=lambda inv: get_last_name(inv["customer_name"]))

        return render_template(
            "sammelrechnungen.html",
            collective_invoices=collective_invoices,
            grouped_by_month=dict(grouped_by_month),
        )

    @app.route("/api/invoices")
    def invoices_api() -> Response:
        query = request.args.get("q", "").strip()
        limit = clamp_limit(request.args.get("limit"), app.config["MAX_LIMIT"])
        time_filter = request.args.get("time", "all")
        status_filter = request.args.get("status", "open")
        email_filter = request.args.get("email", "all")
        uncollectible_filter = request.args.get("uncollectible", "hide")
        invoice_date_from = request.args.get("invoice_date_from", "")
        invoice_date_to = request.args.get("invoice_date_to", "")
        from_month = request.args.get("from_month", "")
        to_month = request.args.get("to_month", "")
        sort_by, sort_direction = normalize_sort_params(
            request.args.get("sort", "date"),
            request.args.get("direction", "desc"),
        )
        invoices = fetch_invoices(
            app.config["DATABASE"],
            query,
            limit,
            time_filter,
            status_filter,
            from_month,
            to_month,
            email_filter,
            uncollectible_filter,
            sort_by,
            sort_direction,
            invoice_date_from=invoice_date_from,
            invoice_date_to=invoice_date_to,
        )
        return jsonify(
            {
                "count": len(invoices),
                "limit": limit,
                "query": query,
                "status_filter": status_filter,
                "sort": sort_by,
                "direction": sort_direction,
                "results": [
                    {
                        "id": row.id,
                        "invoice_number": row.invoice_number,
                        "invoice_date": row.invoice_date,
                        "customer_name": row.customer_name,
                        "customer_address": row.customer_address,
                        "amount_cents": row.amount_cents,
                        "amount_eur": row.amount_eur,
                        "status": row.status,
                        "last_seen_snapshot": row.last_seen_snapshot,
                        "first_seen_snapshot": row.first_seen_snapshot,
                        "file_path": row.file_path,
                        "pdf_url": url_for("serve_pdf", relative_path=row.file_path) if row.file_path else None,
                        "in_collective_invoice": row.in_collective_invoice,
                    }
                    for row in invoices
                ],
            }
        )

    @app.route("/api/scan", methods=["POST"])
    def scan_new_invoices() -> Response:
        """Scan the invoice directory for new PDFs and add them to the database."""
        db_path = Path(app.config["DATABASE"])
        root = Path(app.config["INVOICE_ROOT"])

        if not root.exists():
            return jsonify({"error": f"Verzeichnis {root} nicht gefunden"}), 404

        new_count = 0
        errors = []
        payments_detected = 0
        pdf_count = 0

        with sqlite3.connect(db_path) as conn:
            init_db(conn)
            # Use find_pdfs_for_import to skip already completed folders
            pdf_files = list(find_pdfs_for_import(root, conn))
            pdf_count = len(pdf_files)
            for pdf_path in pdf_files:
                try:
                    if process_pdf_file(conn, pdf_path, root):
                        new_count += 1
                except Exception as exc:
                    error_msg = f"{pdf_path.name}: {str(exc)}"
                    errors.append(error_msg)
                    logging.error("Fehler beim Verarbeiten von %s: %s", pdf_path, exc)
            conn.commit()

            # After scanning, detect and log payments for the latest snapshot
            try:
                from invoice_tracker import detect_and_log_payments
                latest_snapshot = conn.execute(
                    "SELECT snapshot_date FROM snapshots ORDER BY snapshot_date DESC LIMIT 1"
                ).fetchone()

                if latest_snapshot:
                    payments_detected = detect_and_log_payments(conn, latest_snapshot[0])
                    if payments_detected > 0:
                        logging.info(f"Zahlungserkennung: {payments_detected} Rechnung(en) als bezahlt markiert")
                conn.commit()
            except Exception as e:
                logging.error(f"Fehler bei Zahlungserkennung: {e}")

        return jsonify({
            "success": True,
            "new_invoices": new_count,
            "total_scanned": pdf_count,
            "payments_detected": payments_detected,
            "errors": errors
        })

    @app.route("/api/scan-stream", methods=["GET"])
    def scan_new_invoices_stream() -> Response:
        """Scan the invoice directory for new PDFs with real-time progress using Server-Sent Events."""
        import json
        import re
        from flask import stream_with_context

        def generate():
            try:
                db_path = Path(app.config["DATABASE"])
                root = Path(app.config["INVOICE_ROOT"])

                if not root.exists():
                    yield f"data: {json.dumps({'type': 'error', 'message': f'Verzeichnis {root} nicht gefunden'})}\n\n"
                    return

                with sqlite3.connect(db_path) as conn:
                    init_db(conn)

                    # Use optimized function that skips already completed folders
                    completed_folders = get_completed_folders(conn)
                    if completed_folders:
                        folders_str = ', '.join(sorted(completed_folders))
                        message = f'{len(completed_folders)} bereits importierte Ordner werden übersprungen: {folders_str}'
                        yield f"data: {json.dumps({'type': 'info', 'message': message})}\n\n"

                    pdf_files = list(find_pdfs_for_import(root, conn))
                    total_pdfs = len(pdf_files)

                    if total_pdfs == 0:
                        yield f"data: {json.dumps({'type': 'complete', 'success': 0, 'failed': 0, 'total': 0, 'skipped_folders': list(completed_folders)})}\n\n"
                        return

                    # Send summary
                    yield f"data: {json.dumps({'type': 'summary', 'total': total_pdfs})}\n\n"

                    new_count = 0
                    skipped_count = 0
                    error_count = 0
                    errors = []
                    processed_folders = set()  # Track which folders we processed

                    for idx, pdf_path in enumerate(pdf_files, 1):
                        try:
                            # Track folder
                            relative_path = pdf_path.relative_to(root)
                            if len(relative_path.parts) >= 1:
                                processed_folders.add(relative_path.parts[0])

                            # Yield progress
                            progress = int((idx / total_pdfs) * 100)
                            yield f"data: {json.dumps({'type': 'progress', 'progress': progress, 'processed': idx, 'total': total_pdfs, 'file': pdf_path.name})}\n\n"

                            if process_pdf_file(conn, pdf_path, root):
                                new_count += 1
                                yield f"data: {json.dumps({'type': 'success', 'file': pdf_path.name})}\n\n"
                            else:
                                skipped_count += 1
                                yield f"data: {json.dumps({'type': 'skipped', 'file': pdf_path.name})}\n\n"
                        except Exception as exc:
                            error_count += 1
                            error_msg = f"{pdf_path.name}: {str(exc)}"
                            errors.append(error_msg)
                            yield f"data: {json.dumps({'type': 'error', 'file': pdf_path.name, 'message': str(exc)})}\n\n"
                            logging.error("Fehler beim Verarbeiten von %s: %s", pdf_path, exc)
                    conn.commit()

                    # After scanning, detect and log payments for the latest snapshot
                    try:
                        from invoice_tracker import detect_and_log_payments
                        latest_snapshot = conn.execute(
                            "SELECT snapshot_date FROM snapshots ORDER BY snapshot_date DESC LIMIT 1"
                        ).fetchone()

                        if latest_snapshot:
                            payments_detected = detect_and_log_payments(conn, latest_snapshot[0])
                            if payments_detected > 0:
                                yield f"data: {json.dumps({'type': 'info', 'message': f'{payments_detected} Zahlung(en) erkannt und in Historie eingetragen'})}\n\n"
                                logging.info(f"Zahlungserkennung: {payments_detected} Rechnung(en) als bezahlt markiert")
                        conn.commit()
                    except Exception as e:
                        logging.error(f"Fehler bei Zahlungserkennung: {e}")

                    # Mark all processed folders as complete (regardless of pending imports)
                    # This ensures folders are not re-scanned on subsequent imports
                    marked_complete = []
                    for folder_name in processed_folders:
                        if mark_folder_complete(conn, folder_name):
                            marked_complete.append(folder_name)
                            logging.info(f"Ordner '{folder_name}' als komplett importiert markiert")

                    if marked_complete:
                        folders_str = ', '.join(marked_complete)
                        message = f'{len(marked_complete)} Ordner als vollständig importiert markiert: {folders_str}'
                        yield f"data: {json.dumps({'type': 'info', 'message': message})}\n\n"

                # Send completion message
                yield f"data: {json.dumps({'type': 'complete', 'success': new_count, 'skipped': skipped_count, 'failed': error_count, 'total': total_pdfs, 'errors': errors, 'processed_folders': list(processed_folders)})}\n\n"

            except Exception as e:
                logging.error(f"Error in scan stream: {e}")
                yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
                # Always send complete event so frontend can close modal
                yield f"data: {json.dumps({'type': 'complete', 'success': 0, 'skipped': 0, 'failed': 0, 'total': 0, 'errors': [str(e)]})}\n\n"

        return Response(stream_with_context(generate()), mimetype="text/event-stream")

    @app.route("/api/pending-imports", methods=["GET"])
    def get_pending_imports() -> Response:
        """Get all pending imports that need user review."""
        db_path = Path(app.config["DATABASE"])

        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute(
                """
                SELECT id, file_path, invoice_number, invoice_date,
                       customer_name, customer_street, customer_city,
                       amount_cents, snapshot_date, similar_customers,
                       created_at
                FROM pending_imports
                WHERE status = 'pending'
                ORDER BY created_at DESC
                """
            )

            pending_imports = []
            for row in cursor.fetchall():
                (import_id, file_path, invoice_number, invoice_date,
                 customer_name, customer_street, customer_city,
                 amount_cents, snapshot_date, similar_customers_json,
                 created_at) = row

                similar_customers = json.loads(similar_customers_json) if similar_customers_json else []

                pending_imports.append({
                    "id": import_id,
                    "file_path": file_path,
                    "invoice_number": invoice_number,
                    "invoice_date": invoice_date,
                    "customer_name": customer_name,
                    "customer_street": customer_street,
                    "customer_city": customer_city,
                    "amount_cents": amount_cents,
                    "amount_euros": amount_cents / 100,
                    "snapshot_date": snapshot_date,
                    "similar_customers": similar_customers,
                    "created_at": created_at
                })

        return jsonify({
            "success": True,
            "pending_imports": pending_imports,
            "count": len(pending_imports)
        })

    @app.route("/api/resolve-import", methods=["POST"])
    def resolve_import() -> Response:
        """Resolve a pending import by creating new customer or merging with existing."""
        data = request.get_json()
        import_id = data.get("import_id")
        action = data.get("action")  # 'create_new' or 'merge_with_existing'
        selected_customer = data.get("selected_customer")  # Only for merge action
        use_new_data = data.get("use_new_data", False)  # Whether to use new data from import

        if not import_id or not action:
            return jsonify({"error": "import_id and action sind erforderlich"}), 400

        if action not in ['create_new', 'merge_with_existing']:
            return jsonify({"error": "action muss 'create_new' oder 'merge_with_existing' sein"}), 400

        if action == 'merge_with_existing' and not selected_customer:
            return jsonify({"error": "selected_customer ist erforderlich für merge_with_existing"}), 400

        db_path = Path(app.config["DATABASE"])

        try:
            with sqlite3.connect(db_path) as conn:
                init_db(conn)
                success = resolve_pending_import(conn, import_id, action, selected_customer, use_new_data)

                if success:
                    # Get count of remaining pending imports
                    remaining_count = conn.execute(
                        "SELECT COUNT(*) FROM pending_imports WHERE status = 'pending'"
                    ).fetchone()[0]

                    return jsonify({
                        "success": True,
                        "message": "Import erfolgreich aufgelöst",
                        "remaining_pending": remaining_count
                    })
                else:
                    return jsonify({"error": "Import konnte nicht aufgelöst werden"}), 500

        except Exception as e:
            logging.error(f"Fehler beim Auflösen des Imports: {e}")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/import-folders", methods=["GET"])
    def get_import_folders() -> Response:
        """Get list of all import folders with their status."""
        db_path = Path(app.config["DATABASE"])
        root = Path(app.config["INVOICE_ROOT"])

        with sqlite3.connect(db_path) as conn:
            init_db(conn)

            # Get folders from database
            cursor = conn.execute(
                """
                SELECT folder_name, snapshot_date, import_complete, scanned_at,
                       (SELECT COUNT(*) FROM invoice_snapshots is2
                        JOIN snapshots s2 ON is2.snapshot_id = s2.id
                        WHERE s2.folder_name = snapshots.folder_name) as invoice_count
                FROM snapshots
                ORDER BY snapshot_date DESC
                """
            )

            folders = []
            for row in cursor.fetchall():
                folder_name, snapshot_date, import_complete, scanned_at, invoice_count = row
                folder_path = root / folder_name
                pdf_count = len(list(folder_path.glob("*.pdf"))) if folder_path.exists() else 0

                folders.append({
                    "folder_name": folder_name,
                    "snapshot_date": snapshot_date,
                    "import_complete": bool(import_complete),
                    "scanned_at": scanned_at,
                    "invoice_count": invoice_count,
                    "pdf_count_on_disk": pdf_count,
                    "exists_on_disk": folder_path.exists()
                })

            # Also find folders on disk that aren't in the database yet
            if root.exists():
                import re
                for folder in root.iterdir():
                    if folder.is_dir() and re.match(r'^\d{4}-\d{2}', folder.name):
                        if not any(f["folder_name"] == folder.name for f in folders):
                            pdf_count = len(list(folder.glob("*.pdf")))
                            folders.append({
                                "folder_name": folder.name,
                                "snapshot_date": None,
                                "import_complete": False,
                                "scanned_at": None,
                                "invoice_count": 0,
                                "pdf_count_on_disk": pdf_count,
                                "exists_on_disk": True,
                                "not_yet_imported": True
                            })

            # Sort by folder name descending
            folders.sort(key=lambda x: x["folder_name"], reverse=True)

        return jsonify({
            "success": True,
            "folders": folders
        })

    @app.route("/api/import-folders/mark-complete", methods=["POST"])
    def mark_folder_complete_api() -> Response:
        """Mark a folder as completely imported."""
        data = request.get_json()
        folder_name = data.get("folder_name")

        if not folder_name:
            return jsonify({"error": "folder_name ist erforderlich"}), 400

        db_path = Path(app.config["DATABASE"])

        with sqlite3.connect(db_path) as conn:
            init_db(conn)
            success = mark_folder_complete(conn, folder_name)

            if success:
                return jsonify({
                    "success": True,
                    "message": f"Ordner '{folder_name}' als vollständig importiert markiert"
                })
            else:
                return jsonify({
                    "error": f"Ordner '{folder_name}' nicht gefunden"
                }), 404

    @app.route("/api/import-folders/mark-incomplete", methods=["POST"])
    def mark_folder_incomplete_api() -> Response:
        """Mark a folder as incomplete (to allow re-scanning)."""
        data = request.get_json()
        folder_name = data.get("folder_name")

        if not folder_name:
            return jsonify({"error": "folder_name ist erforderlich"}), 400

        db_path = Path(app.config["DATABASE"])

        with sqlite3.connect(db_path) as conn:
            init_db(conn)
            success = mark_folder_incomplete(conn, folder_name)

            if success:
                return jsonify({
                    "success": True,
                    "message": f"Ordner '{folder_name}' für erneuten Scan freigegeben"
                })
            else:
                return jsonify({
                    "error": f"Ordner '{folder_name}' nicht gefunden"
                }), 404

    @app.route("/api/print-invoices")
    def print_invoices() -> Response:
        """Combine all filtered invoice PDFs into a single PDF for printing."""
        query = request.args.get("q", "").strip()
        limit = clamp_limit(request.args.get("limit"), app.config["MAX_LIMIT"])
        time_filter = request.args.get("time", "all")
        status_filter = request.args.get("status", "open")
        email_filter = request.args.get("email", "all")
        uncollectible_filter = request.args.get("uncollectible", "hide")
        invoice_date_from = request.args.get("invoice_date_from", "")
        invoice_date_to = request.args.get("invoice_date_to", "")
        from_month = request.args.get("from_month", "")
        to_month = request.args.get("to_month", "")

        invoices = fetch_invoices(app.config["DATABASE"], query, limit, time_filter, status_filter, from_month, to_month, email_filter, uncollectible_filter, invoice_date_from=invoice_date_from, invoice_date_to=invoice_date_to)

        if not invoices:
            return jsonify({"error": "Keine Rechnungen zum Drucken gefunden"}), 404

        # Filter out invoices without file_path
        invoices_with_files = [inv for inv in invoices if inv.file_path]

        if not invoices_with_files:
            return jsonify({"error": "Keine PDF-Dateien für die ausgewählten Rechnungen gefunden"}), 404

        try:
            # Create PDF writer
            pdf_writer = PdfWriter()
            root = BASE_DIR

            # Add all PDFs to the writer
            for invoice in invoices_with_files:
                pdf_path = root / invoice.file_path
                if pdf_path.exists():
                    try:
                        pdf_reader = PdfReader(pdf_path)
                        page_count = len(pdf_reader.pages)
                        logging.info(f"Adding {page_count} page(s) from {pdf_path.name}")
                        for page in pdf_reader.pages:
                            pdf_writer.add_page(page)
                    except Exception as e:
                        logging.error(f"Fehler beim Lesen von {pdf_path}: {e}")
                        continue

            # Write combined PDF to bytes
            output = io.BytesIO()
            pdf_writer.write(output)
            output.seek(0)

            return send_file(
                output,
                mimetype='application/pdf',
                as_attachment=False,
                download_name=f'rechnungen_{datetime.now().strftime("%Y%m%d_%H%M%S")}.pdf'
            )
        except Exception as e:
            logging.error(f"Fehler beim Kombinieren der PDFs: {e}")
            return jsonify({"error": f"Fehler beim Erstellen des PDFs: {str(e)}"}), 500

    @app.route("/api/send-invoices-email-stream", methods=["GET"])
    def send_invoices_email_stream() -> Response:
        """Send invoices via email with real-time progress updates using Server-Sent Events."""
        import json
        from flask import stream_with_context

        query = request.args.get("q", "").strip()
        limit = clamp_limit(request.args.get("limit"), app.config["MAX_LIMIT"])
        time_filter = request.args.get("time", "all")
        status_filter = request.args.get("status", "open")
        email_filter = request.args.get("email", "all")
        uncollectible_filter = request.args.get("uncollectible", "hide")
        invoice_date_from = request.args.get("invoice_date_from", "")
        invoice_date_to = request.args.get("invoice_date_to", "")
        from_month = request.args.get("from_month", "")
        to_month = request.args.get("to_month", "")

        def generate():
            try:
                invoices = fetch_invoices(app.config["DATABASE"], query, limit, time_filter, status_filter, from_month, to_month, email_filter, uncollectible_filter, invoice_date_from=invoice_date_from, invoice_date_to=invoice_date_to)

                if not invoices:
                    yield f"data: {json.dumps({'type': 'error', 'message': 'Keine Rechnungen zum Versenden gefunden'})}\n\n"
                    return

                # Group invoices by customer only (all invoices for a customer in one email)
                grouped_invoices = defaultdict(list)

                for invoice in invoices:
                    key = invoice.customer_name
                    grouped_invoices[key].append(invoice)

                total_groups = len(grouped_invoices)
                total_invoices = len(invoices)

                # Send summary
                yield f"data: {json.dumps({'type': 'summary', 'total_groups': total_groups, 'total_invoices': total_invoices})}\n\n"

                smtp_config = load_smtp_config()
                smtp_connection: Optional[smtplib.SMTP] = None

                def ensure_smtp_connection() -> smtplib.SMTP:
                    nonlocal smtp_connection
                    if smtp_connection is None:
                        smtp_connection = create_smtp_connection(smtp_config)
                    return smtp_connection

                def reset_smtp_connection() -> None:
                    nonlocal smtp_connection
                    if smtp_connection is not None:
                        try:
                            smtp_connection.quit()
                        except Exception:
                            logging.warning("Failed to close SMTP connection cleanly")
                    smtp_connection = None

                smtp_connection_failed = False

                # Status: Connecting to SMTP
                yield f"data: {json.dumps({'type': 'status', 'message': 'Verbinde mit SMTP-Server...'})}\n\n"

                try:
                    ensure_smtp_connection()
                    # Status: Connection established
                    yield f"data: {json.dumps({'type': 'status', 'message': 'SMTP-Verbindung hergestellt ✓'})}\n\n"
                except Exception as exc:
                    logging.error(f"Unable to establish SMTP connection: {exc}")
                    yield f"data: {json.dumps({'type': 'error', 'message': 'SMTP-Verbindung konnte nicht aufgebaut werden'})}\n\n"
                    yield f"data: {json.dumps({'type': 'status', 'message': 'SMTP-Verbindung fehlgeschlagen ✗'})}\n\n"
                    smtp_connection_failed = True

                # Get customer emails from database
                with sqlite3.connect(app.config["DATABASE"]) as conn:
                    conn.row_factory = sqlite3.Row
                    init_db(conn)

                    success_count = 0
                    failed_count = 0
                    processed_groups = 0
                    root = BASE_DIR

                    # Process each group only if SMTP connection is established
                    if not smtp_connection_failed:
                        for customer_name, invoice_list in grouped_invoices.items():
                            # Get customer email and salutation
                            customer_row = conn.execute(
                                "SELECT email, salutation FROM customer_details WHERE customer_name = ?",
                                (customer_name,)
                            ).fetchone()

                            if not customer_row or not customer_row["email"]:
                                error_msg = f"Keine E-Mail-Adresse hinterlegt"
                                yield f"data: {json.dumps({'type': 'error', 'customer': customer_name, 'message': error_msg})}\n\n"
                                failed_count += len(invoice_list)
                                processed_groups += 1
                                progress = int((processed_groups / total_groups) * 100)
                                yield f"data: {json.dumps({'type': 'progress', 'progress': progress, 'processed': processed_groups, 'total': total_groups})}\n\n"
                                continue

                            customer_email = customer_row["email"]
                            customer_salutation = customer_row["salutation"] if "salutation" in customer_row.keys() else None

                            # Collect PDFs
                            pdf_paths = []
                            missing_pdfs = []
                            for invoice in invoice_list:
                                if not invoice.file_path:
                                    missing_pdfs.append(f"{invoice.invoice_number or invoice.id} (kein Pfad)")
                                    continue
                                pdf_path = root / invoice.file_path
                                if pdf_path.exists():
                                    pdf_paths.append(pdf_path)
                                else:
                                    missing_pdfs.append(f"{invoice.invoice_number or invoice.id} (nicht gefunden: {invoice.file_path})")

                            if not pdf_paths:
                                error_msg = f"Keine gültigen PDF-Dateien gefunden"
                                if missing_pdfs:
                                    error_msg += f" - Fehlende PDFs: {', '.join(missing_pdfs[:3])}"
                                    if len(missing_pdfs) > 3:
                                        error_msg += f" (+{len(missing_pdfs)-3} weitere)"
                                yield f"data: {json.dumps({'type': 'error', 'customer': customer_name, 'message': error_msg})}\n\n"
                                failed_count += len(invoice_list)
                                processed_groups += 1
                                progress = int((processed_groups / total_groups) * 100)
                                yield f"data: {json.dumps({'type': 'progress', 'progress': progress, 'processed': processed_groups, 'total': total_groups})}\n\n"
                                continue

                            # Get other open invoices for this customer (not in current filter)
                            current_invoice_ids = {inv.id for inv in invoice_list}
                            other_open_cursor = conn.execute(
                                """
                                SELECT i.id, i.invoice_number, i.invoice_date, i.amount_cents
                                FROM invoices i
                                JOIN invoice_snapshots isnap ON i.id = isnap.invoice_id
                                JOIN snapshots s ON isnap.snapshot_id = s.id
                                WHERE i.customer_name = ?
                                  AND s.snapshot_date = (SELECT MAX(snapshot_date) FROM snapshots)
                                  AND i.uncollectible = 0
                                ORDER BY i.invoice_date ASC
                                """,
                                (customer_name,)
                            )
                            # Create simple objects for other open invoices
                            other_open_invoices = []
                            for row in other_open_cursor.fetchall():
                                if row["id"] not in current_invoice_ids:
                                    class SimpleInvoice:
                                        pass
                                    inv = SimpleInvoice()
                                    inv.id = row["id"]
                                    inv.invoice_number = row["invoice_number"]
                                    inv.invoice_date = row["invoice_date"]
                                    inv.amount_cents = row["amount_cents"]
                                    other_open_invoices.append(inv)

                            # Send info message
                            yield f"data: {json.dumps({'type': 'info', 'customer': customer_name, 'email': customer_email, 'count': len(pdf_paths)})}\n\n"

                            # Status: Sending email
                            yield f"data: {json.dumps({'type': 'status', 'message': f'Sende E-Mail an {customer_email}... ({processed_groups + 1}/{total_groups})'})}\n\n"

                            # Send email (retry once if the SMTP server disconnects)
                            # Pass invoice_list so we can include details in email
                            send_success = send_invoices_batch_email(
                                customer_email,
                                customer_name,
                                pdf_paths,
                                None,  # month_year - will be handled in the function
                                customer_salutation,
                                smtp_connection=ensure_smtp_connection(),
                                smtp_config=smtp_config,
                                invoice_list=invoice_list,  # Pass the invoice list for details
                                other_open_invoices=other_open_invoices if other_open_invoices else None,
                            )

                            if not send_success:
                                reset_smtp_connection()
                                yield f"data: {json.dumps({'type': 'status', 'message': f'Verbindung unterbrochen, stelle Verbindung wieder her...'})}\n\n"
                                try:
                                    connection_for_retry = ensure_smtp_connection()
                                    yield f"data: {json.dumps({'type': 'status', 'message': f'Verbindung wiederhergestellt, sende E-Mail erneut an {customer_email}...'})}\n\n"
                                except Exception as exc:
                                    logging.error(f"SMTP reconnect failed: {exc}")
                                    reset_smtp_connection()
                                    failed_count += len(invoice_list)
                                    yield f"data: {json.dumps({'type': 'error', 'customer': customer_name, 'message': 'E-Mail-Versand fehlgeschlagen: SMTP-Verbindung getrennt'})}\n\n"
                                    yield f"data: {json.dumps({'type': 'status', 'message': f'Wiederverbindung fehlgeschlagen ✗'})}\n\n"
                                    processed_groups += 1
                                    progress = int((processed_groups / total_groups) * 100)
                                    yield f"data: {json.dumps({'type': 'progress', 'progress': progress, 'processed': processed_groups, 'total': total_groups})}\n\n"
                                    continue

                                send_success = send_invoices_batch_email(
                                    customer_email,
                                    customer_name,
                                    pdf_paths,
                                    None,  # month_year - will be handled in the function
                                    customer_salutation,
                                    smtp_connection=connection_for_retry,
                                    smtp_config=smtp_config,
                                    invoice_list=invoice_list,  # Pass the invoice list for details
                                    other_open_invoices=other_open_invoices if other_open_invoices else None,
                                )

                            if send_success:
                                success_count += len(pdf_paths)
                                # Log email sent event for each invoice
                                for invoice in invoice_list:
                                    log_invoice_event(
                                        conn,
                                        invoice.id,
                                        "EMAIL_SENT",
                                        {
                                            "email": customer_email,
                                            "invoice_date": invoice.invoice_date,
                                            "pdf_count": len(pdf_paths)
                                        }
                                    )
                                conn.commit()
                                yield f"data: {json.dumps({'type': 'success', 'customer': customer_name, 'email': customer_email, 'count': len(pdf_paths)})}\n\n"
                                yield f"data: {json.dumps({'type': 'status', 'message': f'✓ E-Mail erfolgreich versendet an {customer_email} ({processed_groups + 1}/{total_groups})'})}\n\n"
                            else:
                                failed_count += len(invoice_list)
                                yield f"data: {json.dumps({'type': 'error', 'customer': customer_name, 'message': 'E-Mail-Versand fehlgeschlagen (möglicherweise Rate Limit des SMTP-Servers)'})}\n\n"
                                yield f"data: {json.dumps({'type': 'status', 'message': f'✗ E-Mail-Versand fehlgeschlagen an {customer_email}'})}\n\n"

                            processed_groups += 1
                            progress = int((processed_groups / total_groups) * 100)
                            yield f"data: {json.dumps({'type': 'progress', 'progress': progress, 'processed': processed_groups, 'total': total_groups})}\n\n"

                            # Add delay between emails to avoid rate limiting (2 seconds)
                            if processed_groups < total_groups:
                                time.sleep(2)

                # Close SMTP connection and send completion message
                yield f"data: {json.dumps({'type': 'status', 'message': 'Schließe SMTP-Verbindung...'})}\n\n"
                reset_smtp_connection()
                yield f"data: {json.dumps({'type': 'status', 'message': f'✓ Versand abgeschlossen: {success_count} erfolgreich, {failed_count} fehlgeschlagen'})}\n\n"
                yield f"data: {json.dumps({'type': 'complete', 'success': success_count, 'failed': failed_count, 'total': total_invoices})}\n\n"

            except Exception as e:
                logging.error(f"Error in email stream: {e}")
                yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
                # Always send complete event so frontend can close modal
                yield f"data: {json.dumps({'type': 'complete', 'success': 0, 'failed': 0, 'total': 0})}\n\n"

        return Response(stream_with_context(generate()), mimetype="text/event-stream")

    @app.route("/api/reminders", methods=["POST"])
    def create_reminder() -> Response:
        """Create a new reminder for an invoice."""
        data = request.get_json()

        if not data:
            return jsonify({"success": False, "error": "Keine Daten empfangen"}), 400

        invoice_id = data.get("invoice_id")
        reminder_level = data.get("reminder_level")

        if invoice_id is None or reminder_level is None:
            return jsonify({"success": False, "error": "invoice_id und reminder_level erforderlich"}), 400

        if reminder_level not in [0, 1, 2]:
            return jsonify({"success": False, "error": "Ungültige reminder_level (muss 0, 1 oder 2 sein)"}), 400

        try:
            with sqlite3.connect(app.config["DATABASE"]) as conn:
                # Ensure reminders table exists
                init_db(conn)

                # Check if invoice exists
                cursor = conn.execute("SELECT id FROM invoices WHERE id = ?", (invoice_id,))
                if not cursor.fetchone():
                    return jsonify({"success": False, "error": "Rechnung nicht gefunden"}), 404

                # Create reminder entry (initially without LetterExpress ID)
                conn.execute(
                    """
                    INSERT INTO reminders (invoice_id, reminder_level, letterexpress_status)
                    VALUES (?, ?, 'pending')
                    """,
                    (invoice_id, reminder_level)
                )

                # Log reminder creation event
                log_invoice_event(
                    conn,
                    invoice_id,
                    "REMINDER_CREATED",
                    {
                        "reminder_level": reminder_level
                    }
                )

                conn.commit()

                return jsonify({
                    "success": True,
                    "message": "Mahnung wurde erstellt",
                    "invoice_id": invoice_id,
                    "reminder_level": reminder_level
                })

        except sqlite3.IntegrityError as e:
            logging.error("Database integrity error: %s", e)
            return jsonify({"success": False, "error": "Datenbankfehler"}), 500
        except Exception as e:
            logging.error("Error creating reminder: %s", e)
            return jsonify({"success": False, "error": str(e)}), 500

    @app.route("/api/reminders/bulk", methods=["POST"])
    def create_bulk_reminders() -> Response:
        """Create grouped payment reminders with PDFs (one PDF per customer per reminder level)."""
        data = request.get_json()

        if not data:
            return jsonify({"success": False, "error": "Keine Daten empfangen"}), 400

        invoices_list = data.get("invoices", [])

        if not invoices_list or not isinstance(invoices_list, list):
            return jsonify({"success": False, "error": "invoices Liste erforderlich"}), 400

        try:
            # Create output folder for reminders
            current_month = datetime.now().strftime("%Y-%m")
            reminders_folder = BASE_DIR / "Mahnungen" / current_month
            reminders_folder.mkdir(parents=True, exist_ok=True)

            created_pdfs = 0
            created_reminders = 0
            skipped_paid_invoices = 0

            with sqlite3.connect(app.config["DATABASE"]) as conn:
                # Ensure reminders table exists
                init_db(conn)

                # Get the latest snapshot date for safety check
                latest_snapshot_row = conn.execute(
                    "SELECT MAX(snapshot_date) as latest FROM snapshots"
                ).fetchone()

                if not latest_snapshot_row or not latest_snapshot_row[0]:
                    return jsonify({
                        "success": False,
                        "error": "Kein Snapshot gefunden. Bitte scannen Sie zuerst Rechnungen ein."
                    }), 400

                latest_snapshot = latest_snapshot_row[0]

                # Fetch invoice details from database and group by customer and level
                grouped = defaultdict(list)

                for inv_data in invoices_list:
                    invoice_id = inv_data.get("invoice_id")
                    reminder_level = inv_data.get("reminder_level")

                    if invoice_id is None or reminder_level is None:
                        continue

                    if reminder_level not in [0, 1, 2]:
                        continue

                    # SAFETY CHECK: Verify invoice is still open (present in latest snapshot)
                    status_check = conn.execute(
                        """
                        SELECT
                            CASE
                                WHEN MAX(s.snapshot_date) = ? THEN 'open'
                                ELSE 'paid'
                            END as status
                        FROM invoices i
                        JOIN invoice_snapshots isnap ON i.id = isnap.invoice_id
                        JOIN snapshots s ON isnap.snapshot_id = s.id
                        WHERE i.id = ?
                        GROUP BY i.id
                        """,
                        (latest_snapshot, invoice_id)
                    ).fetchone()

                    # Skip if invoice is paid or not found
                    if not status_check or status_check[0] != 'open':
                        skipped_paid_invoices += 1
                        logging.warning(f"Skipping invoice {invoice_id} - already paid or not found in latest snapshot")
                        continue

                    # Fetch invoice details including file_path
                    cursor = conn.execute(
                        """
                        SELECT
                            i.id,
                            i.invoice_number,
                            i.invoice_date,
                            i.customer_name,
                            i.customer_address,
                            i.customer_street,
                            i.customer_city,
                            i.amount_cents,
                            isnap.file_path
                        FROM invoices i
                        LEFT JOIN invoice_snapshots isnap ON i.id = isnap.invoice_id
                        LEFT JOIN snapshots s ON isnap.snapshot_id = s.id
                        WHERE i.id = ? AND s.snapshot_date = ?
                        GROUP BY i.id
                        """,
                        (invoice_id, latest_snapshot)
                    )
                    row = cursor.fetchone()

                    if not row:
                        continue

                    inv_id, inv_number, inv_date, cust_name, cust_address, cust_street, cust_city, amount_cents, file_path = row

                    # Try to get custom address from customer_details first (for consistent addresses across invoices)
                    custom_address_data = get_customer_custom_address(conn, cust_name)
                    if custom_address_data:
                        custom_name, custom_street, custom_city = custom_address_data
                        # Use custom address for grouping and PDF generation
                        full_address = f"{custom_street}, {custom_city}"
                        display_name = custom_name  # Use custom name if set
                    else:
                        # Fallback: Construct address from invoice data
                        if cust_street and cust_city:
                            full_address = f"{cust_street}, {cust_city}"
                        else:
                            full_address = cust_address or ""
                        display_name = cust_name

                    # Group by customer name and reminder level
                    key = (display_name, full_address, reminder_level)
                    grouped[key].append({
                        'id': inv_id,
                        'number': inv_number or f"#{inv_id}",
                        'date': inv_date,
                        'amount': amount_cents / 100.0,
                        'file_path': file_path
                    })

                # Generate PDFs for each group
                # Use BASE_DIR since file_path already contains "Rechnungen/" prefix
                root = BASE_DIR

                for (customer_name, customer_address, reminder_level), invoice_list in grouped.items():
                    # Get salutation for customer from customer_details, or determine via AI
                    salutation_row = conn.execute(
                        "SELECT salutation FROM customer_details WHERE customer_name = ?",
                        (customer_name,)
                    ).fetchone()
                    salutation = salutation_row[0] if salutation_row and salutation_row[0] else determine_salutation_for_customer(customer_name)

                    # Create reminder PDF (letter)
                    reminder_pdf_bytes = create_reminder_pdf(
                        customer_name=customer_name,
                        customer_address=customer_address,
                        invoices=invoice_list,
                        reminder_level=reminder_level,
                        salutation=salutation
                    )

                    # Create PDF merger to combine reminder letter with invoice PDFs
                    pdf_merger = PdfWriter()

                    # Add reminder letter
                    reminder_pdf = PdfReader(io.BytesIO(reminder_pdf_bytes))
                    for page in reminder_pdf.pages:
                        pdf_merger.add_page(page)

                    # Add all invoice PDFs
                    invoices_added = 0
                    for inv in invoice_list:
                        if inv.get('file_path'):
                            invoice_pdf_path = root / inv['file_path']
                            if invoice_pdf_path.exists():
                                try:
                                    invoice_pdf = PdfReader(invoice_pdf_path)
                                    for page in invoice_pdf.pages:
                                        pdf_merger.add_page(page)
                                    invoices_added += 1
                                except Exception as e:
                                    logging.error(f"Error reading invoice PDF {invoice_pdf_path}: {e}")

                    # Save combined PDF
                    level_names = {
                        0: "Zahlungserinnerung",
                        1: "1_Mahnung",
                        2: "2_Mahnung"
                    }
                    level_name = level_names.get(reminder_level, f"Level_{reminder_level}")

                    safe_customer_name = "".join(
                        c for c in customer_name if c.isalnum() or c in (' ', '-', '_')
                    ).strip().replace(' ', '_')

                    # Add timestamp to make filename unique (avoid overwriting when creating multiple reminders for same customer)
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"{level_name}_{current_month}_{safe_customer_name}_{timestamp}.pdf"
                    pdf_path = reminders_folder / filename

                    with open(pdf_path, 'wb') as f:
                        pdf_merger.write(f)

                    created_pdfs += 1
                    logging.info(f"Created reminder PDF with {invoices_added} invoice(s): {pdf_path}")

                    # Calculate relative path from BASE_DIR
                    relative_pdf_path = str(pdf_path.relative_to(BASE_DIR))

                    # Create database entries for all invoices in this group
                    for inv in invoice_list:
                        try:
                            conn.execute(
                                """
                                INSERT INTO reminders (invoice_id, reminder_level, letterexpress_status, pdf_path)
                                VALUES (?, ?, 'pending', ?)
                                """,
                                (inv['id'], reminder_level, relative_pdf_path)
                            )
                            # Log reminder creation event
                            log_invoice_event(
                                conn,
                                inv['id'],
                                "REMINDER_CREATED",
                                {
                                    "reminder_level": reminder_level,
                                    "pdf_path": relative_pdf_path,
                                    "invoice_count": len(invoice_list)
                                }
                            )
                            created_reminders += 1
                        except sqlite3.IntegrityError:
                            # Skip if reminder already exists
                            continue

                conn.commit()

                # Check if any reminders were created
                if created_pdfs == 0 and skipped_paid_invoices > 0:
                    return jsonify({
                        "success": False,
                        "error": f"Alle {skipped_paid_invoices} ausgewählten Rechnungen wurden bereits bezahlt und übersprungen."
                    }), 400

                # Build success message
                message = f"{created_pdfs} Mahnung(en) wurden erstellt mit {created_reminders} Rechnungen"
                if skipped_paid_invoices > 0:
                    message += f". HINWEIS: {skipped_paid_invoices} bereits bezahlte Rechnung(en) wurden übersprungen."

                return jsonify({
                    "success": True,
                    "message": message,
                    "created_pdfs": created_pdfs,
                    "created_reminders": created_reminders,
                    "skipped_paid": skipped_paid_invoices,
                    "folder": str(reminders_folder)
                })

        except Exception as e:
            logging.error("Error creating bulk reminders: %s", e)
            return jsonify({"success": False, "error": str(e)}), 500

    @app.route("/api/generate-collective-invoices", methods=["POST"])
    def generate_collective_invoices() -> Response:
        """Generate collective invoices (cover letter + latest invoice) for each customer."""
        try:
            # Get folder name from request or use current month as default
            folder_name = request.args.get("folder_name", "").strip()
            if not folder_name:
                folder_name = datetime.now().strftime("%Y-%m")

            # Sanitize folder name to prevent path traversal
            folder_name = folder_name.replace("/", "-").replace("\\", "-").replace("..", "-")

            output_folder = BASE_DIR / "Sammelrechnungen" / folder_name
            output_folder.mkdir(parents=True, exist_ok=True)

            # Get filters from request (for selecting which customers to process)
            query = request.args.get("q", "").strip()
            limit = clamp_limit(request.args.get("limit"), app.config["MAX_LIMIT"])
            time_filter = request.args.get("time", "all")
            status_filter = "open"  # Only open invoices
            from_month = request.args.get("from_month", "")
            to_month = request.args.get("to_month", "")
            email_filter = request.args.get("email", "all")
            uncollectible_filter = request.args.get("uncollectible", "hide")
            invoice_date_from = request.args.get("invoice_date_from", "")
            invoice_date_to = request.args.get("invoice_date_to", "")
            include_sepa = request.args.get("include_sepa", "false").lower() == "true"
            include_email_consent = request.args.get("include_email_consent", "false").lower() == "true"

            # First, get invoices based on user filters to determine which customers to process
            filtered_invoices = fetch_invoices(
                app.config["DATABASE"],
                query,
                limit,
                time_filter,
                status_filter,
                from_month,
                to_month,
                email_filter,
                uncollectible_filter,
                invoice_date_from=invoice_date_from,
                invoice_date_to=invoice_date_to
            )

            if not filtered_invoices:
                return jsonify({"success": False, "error": "Keine offenen Rechnungen gefunden"}), 404

            # Get unique customer names from filtered results
            customer_names = set(inv.customer_name for inv in filtered_invoices)

            # Now fetch ALL open invoices for these customers (ignore time filters)
            # This ensures we show all older open invoices in the cover letter
            all_invoices = fetch_invoices(
                app.config["DATABASE"],
                "",  # no search query
                10000,  # high limit to get all invoices
                "all",  # all time periods
                "open",  # only open invoices
                "",  # no from_month filter
                "",  # no to_month filter
                "all",  # all email statuses
                uncollectible_filter  # respect uncollectible filter
            )

            # Filter to only the customers we want to process
            invoices = [inv for inv in all_invoices if inv.customer_name in customer_names]

            # Group by customer
            customer_invoices = defaultdict(list)
            for invoice in invoices:
                customer_invoices[invoice.customer_name].append(invoice)

            # Get customer details from database
            with sqlite3.connect(app.config["DATABASE"]) as conn:
                conn.row_factory = sqlite3.Row
                init_db(conn)

                count = 0
                total_invoices = 0
                # Use BASE_DIR since file_path already contains "Rechnungen/" prefix
                root = BASE_DIR

                for customer_name, customer_invoice_list in customer_invoices.items():
                    # Sort by date descending to get latest invoices first
                    customer_invoice_list.sort(key=lambda x: x.invoice_date, reverse=True)

                    # Find the latest month (YYYY-MM)
                    latest_month = customer_invoice_list[0].invoice_date[:7]  # e.g., "2025-10"

                    # Separate invoices into current month and older
                    current_month_invoices = []
                    older_invoices = []

                    for inv in customer_invoice_list:
                        inv_month = inv.invoice_date[:7]
                        if inv_month == latest_month:
                            current_month_invoices.append(inv)
                        else:
                            older_invoices.append(inv)

                    # Get customer salutation, address, and bank debit status
                    customer_row = conn.execute(
                        "SELECT salutation, bank_debit FROM customer_details WHERE customer_name = ?",
                        (customer_name,)
                    ).fetchone()
                    salutation = customer_row["salutation"] if customer_row else None
                    bank_debit = customer_row["bank_debit"] if customer_row and "bank_debit" in customer_row.keys() else 0

                    # Try to get custom address from customer_details first (for consistent addresses)
                    custom_address_data = get_customer_custom_address(conn, customer_name)
                    if custom_address_data:
                        custom_name, custom_street, custom_city = custom_address_data
                        customer_address = f"{custom_street}, {custom_city}"
                        # Use custom name if set
                        display_customer_name = custom_name
                    else:
                        # Fallback: Use the address from the first invoice
                        customer_address = current_month_invoices[0].customer_address if current_month_invoices else customer_invoice_list[0].customer_address
                        display_customer_name = customer_name

                    # Prepare current month invoices list
                    current_month_list = []
                    for inv in current_month_invoices:
                        current_month_list.append({
                            'date': inv.invoice_date,
                            'number': inv.invoice_number or "N/A",
                            'amount': inv.amount_eur
                        })

                    # Prepare older open invoices list
                    older_open_list = []
                    for inv in older_invoices:
                        older_open_list.append({
                            'date': inv.invoice_date,
                            'number': inv.invoice_number or "N/A",
                            'amount': inv.amount_eur
                        })

                    # Create cover letter PDF
                    cover_letter_bytes = create_cover_letter_pdf(
                        customer_name=display_customer_name,
                        customer_address=customer_address,
                        current_month_invoices=current_month_list,
                        older_open_invoices=older_open_list,
                        salutation=salutation
                    )

                    # Create PDF merger
                    pdf_merger = PdfWriter()

                    # Add cover letter
                    cover_letter_pdf = PdfReader(io.BytesIO(cover_letter_bytes))
                    for page in cover_letter_pdf.pages:
                        pdf_merger.add_page(page)

                    # Add all current month invoice PDFs
                    current_month_count = 0
                    for inv in current_month_invoices:
                        if inv.file_path:
                            invoice_pdf_path = root / inv.file_path
                            if invoice_pdf_path.exists():
                                try:
                                    invoice_pdf = PdfReader(invoice_pdf_path)
                                    for page in invoice_pdf.pages:
                                        pdf_merger.add_page(page)
                                    current_month_count += 1
                                except Exception as e:
                                    logging.error(f"Error reading invoice PDF {invoice_pdf_path}: {e}")

                    # Add SEPA-Lastschriftmandat at the end if requested and customer doesn't have bank_debit enabled
                    if include_sepa and not bank_debit:
                        sepa_mandate_bytes = create_sepa_mandate_pdf(
                            customer_name=display_customer_name,
                            customer_address=customer_address
                        )
                        sepa_mandate_pdf = PdfReader(io.BytesIO(sepa_mandate_bytes))
                        for page in sepa_mandate_pdf.pages:
                            pdf_merger.add_page(page)

                    # Add email consent form if requested
                    if include_email_consent:
                        email_consent_bytes = create_email_consent_form_pdf(
                            customer_name=display_customer_name
                        )
                        email_consent_pdf = PdfReader(io.BytesIO(email_consent_bytes))
                        for page in email_consent_pdf.pages:
                            pdf_merger.add_page(page)

                    # Save combined PDF
                    # Sanitize filename
                    safe_customer_name = "".join(
                        c for c in display_customer_name if c.isalnum() or c in (' ', '-', '_')
                    ).strip()
                    # Add timestamp to prevent overwriting files when creating multiple collective invoices for the same customer in the same month
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"Sammelrechnung_{folder_name}_{safe_customer_name}_{timestamp}.pdf"
                    output_path = output_folder / filename

                    with open(output_path, 'wb') as f:
                        pdf_merger.write(f)

                    # Track which invoices are included in this collective invoice
                    for inv in current_month_invoices:
                        try:
                            conn.execute(
                                """
                                INSERT OR IGNORE INTO collective_invoice_items
                                (invoice_id, collective_invoice_filename, collective_invoice_month)
                                VALUES (?, ?, ?)
                                """,
                                (inv.id, filename, folder_name)
                            )
                            # Log collective invoice creation event
                            log_invoice_event(
                                conn,
                                inv.id,
                                "COLLECTIVE_INVOICE_CREATED",
                                {
                                    "filename": filename,
                                    "month": folder_name,
                                    "invoice_count": len(current_month_invoices)
                                }
                            )
                        except Exception as e:
                            logging.error(f"Error tracking invoice {inv.id} in collective invoice: {e}")

                    conn.commit()

                    count += 1
                    total_invoices += current_month_count
                    logging.info(f"Created collective invoice for {customer_name}: {output_path} ({current_month_count} invoices)")

            return jsonify({
                "success": True,
                "count": count,
                "total_invoices": total_invoices,
                "output_folder": str(output_folder.relative_to(BASE_DIR)),
                "output_folder_absolute": str(output_folder)
            })

        except Exception as e:
            logging.error(f"Error generating collective invoices: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    @app.route("/api/open-folder", methods=["POST"])
    def open_folder() -> Response:
        """Open a folder in Finder/Explorer."""
        try:
            data = request.get_json()
            folder_path = data.get("folder_path")

            if not folder_path:
                return jsonify({"success": False, "error": "Kein Ordnerpfad angegeben"}), 400

            folder = Path(folder_path)
            if not folder.exists():
                return jsonify({"success": False, "error": "Ordner existiert nicht"}), 404

            # Open folder in default file manager
            import subprocess
            import sys

            if sys.platform == "darwin":  # macOS
                subprocess.run(["open", str(folder)])
            elif sys.platform == "win32":  # Windows
                subprocess.run(["explorer", str(folder)])
            else:  # Linux
                subprocess.run(["xdg-open", str(folder)])

            return jsonify({"success": True, "message": "Ordner geöffnet"})

        except Exception as e:
            logging.error(f"Error opening folder: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    @app.route("/api/letterxpress-mode", methods=["GET"])
    def get_letterxpress_mode():
        """Get the current LetterXpress mode (test or live)."""
        try:
            mode = os.getenv("LETTERXPRESS_MODE", "test")
            return jsonify({"success": True, "mode": mode})
        except Exception as e:
            logging.error(f"Error getting LetterXpress mode: {e}")
            return jsonify({"success": False, "mode": "unknown"}), 500

    @app.route("/api/letterxpress/balance", methods=["GET"])
    def get_letterxpress_balance():
        """Get the current LetterXpress account balance."""
        try:
            lx_client = LetterXpressClient()
            balance, currency = lx_client.check_balance()
            return jsonify({
                "success": True,
                "balance": balance,
                "currency": currency,
                "mode": lx_client.mode
            })
        except Exception as e:
            logging.error(f"Error getting LetterXpress balance: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    @app.route("/api/letterxpress/jobs", methods=["GET"])
    def get_letterxpress_jobs():
        """List LetterXpress print jobs."""
        try:
            filter_type = request.args.get("filter")
            lx_client = LetterXpressClient()
            jobs = lx_client.list_jobs(filter_type=filter_type)
            return jsonify({
                "success": True,
                "jobs": jobs,
                "filter": filter_type,
                "mode": lx_client.mode
            })
        except Exception as e:
            logging.error(f"Error listing LetterXpress jobs: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    @app.route("/api/letterxpress/jobs/<int:job_id>", methods=["GET"])
    def get_letterxpress_job(job_id: int):
        """Get details of a specific LetterXpress print job."""
        try:
            lx_client = LetterXpressClient()
            job = lx_client.get_job(job_id)
            return jsonify({
                "success": True,
                "job": job,
                "mode": lx_client.mode
            })
        except Exception as e:
            logging.error(f"Error getting LetterXpress job {job_id}: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    @app.route("/api/letterxpress/jobs/<int:job_id>", methods=["DELETE"])
    def delete_letterxpress_job(job_id: int):
        """Delete a draft LetterXpress print job."""
        try:
            lx_client = LetterXpressClient()
            success = lx_client.delete_job(job_id)
            return jsonify({
                "success": success,
                "message": f"Job {job_id} erfolgreich gelöscht"
            })
        except Exception as e:
            logging.error(f"Error deleting LetterXpress job {job_id}: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    @app.route("/api/letterxpress/jobs/<int:job_id>/activate", methods=["PUT"])
    def activate_letterxpress_job(job_id: int):
        """Activate a draft LetterXpress print job (set to live)."""
        try:
            lx_client = LetterXpressClient()
            success = lx_client.activate_job(job_id)
            return jsonify({
                "success": success,
                "message": f"Job {job_id} erfolgreich aktiviert"
            })
        except Exception as e:
            logging.error(f"Error activating LetterXpress job {job_id}: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    @app.route("/api/letterxpress/price", methods=["POST"])
    def get_letterxpress_price():
        """Calculate price for a letter."""
        try:
            data = request.json
            if not data:
                return jsonify({"success": False, "error": "Keine Daten übermittelt"}), 400

            pages = data.get("pages", 1)
            color = data.get("color", "4")
            mode = data.get("mode", "duplex")
            shipping = data.get("shipping", "national")
            registered = data.get("registered")

            lx_client = LetterXpressClient()
            price = lx_client.get_price(
                pages=pages,
                color=color,
                mode=mode,
                shipping=shipping,
                registered=registered
            )

            return jsonify({
                "success": True,
                "price": price,
                "currency": "EUR",
                "specification": {
                    "pages": pages,
                    "color": color,
                    "mode": mode,
                    "shipping": shipping,
                    "registered": registered
                }
            })
        except Exception as e:
            logging.warning(f"Price API not available: {e}")
            # Price endpoint not available in API v3
            return jsonify({
                "success": False,
                "error": "Preisberechnung nicht verfügbar in API v3"
            }), 503

    @app.route("/api/sammelrechnungen-rx", methods=["POST"])
    def update_rx_selection():
        """Update rX selection for a collective invoice."""
        try:
            data = request.json
            if not data:
                return jsonify({"success": False, "error": "Keine Daten übermittelt"}), 400

            filename = data.get("filename")
            month = data.get("month")
            selected = data.get("selected", False)

            if not filename or not month:
                return jsonify({"success": False, "error": "Filename und Monat erforderlich"}), 400

            invoices_logged = 0
            with sqlite3.connect(app.config["DATABASE"]) as conn:
                init_db(conn)
                if selected:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO sammelrechnungen_rx (filename, month, selected)
                        VALUES (?, ?, 1)
                        """,
                        (filename, month)
                    )
                else:
                    conn.execute(
                        "DELETE FROM sammelrechnungen_rx WHERE filename = ? AND month = ?",
                        (filename, month)
                    )

                # Log event in invoice history for all invoices in this collective invoice
                conn.row_factory = sqlite3.Row
                invoice_rows = conn.execute(
                    """
                    SELECT invoice_id FROM collective_invoice_items
                    WHERE collective_invoice_filename = ?
                    """,
                    (filename,)
                ).fetchall()

                event_type = "RX_MARKED" if selected else "RX_UNMARKED"
                for row in invoice_rows:
                    log_invoice_event(conn, row["invoice_id"], event_type, {
                        "collective_invoice": filename,
                        "month": month
                    })
                    invoices_logged += 1

                conn.commit()

            return jsonify({"success": True, "invoices_logged": invoices_logged})
        except Exception as e:
            logging.error(f"Error updating rX selection: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    @app.route("/api/sammelrechnungen-rx/print", methods=["POST"])
    def print_rx_selected():
        """Get all rX-selected PDFs for a specific month and merge them for printing."""
        try:
            data = request.json
            if not data:
                return jsonify({"success": False, "error": "Keine Daten übermittelt"}), 400

            month = data.get("month")
            if not month:
                return jsonify({"success": False, "error": "Monat erforderlich"}), 400

            sammelrechnungen_dir = BASE_DIR / "Sammelrechnungen" / month

            if not sammelrechnungen_dir.exists():
                return jsonify({"success": False, "error": f"Verzeichnis für {month} nicht gefunden"}), 404

            with sqlite3.connect(app.config["DATABASE"]) as conn:
                conn.row_factory = sqlite3.Row
                init_db(conn)
                rows = conn.execute(
                    "SELECT filename FROM sammelrechnungen_rx WHERE month = ? AND selected = 1",
                    (month,)
                ).fetchall()

            if not rows:
                return jsonify({"success": False, "error": "Keine rX-markierten Sammelrechnungen für diesen Monat"}), 404

            # Collect PDF paths
            pdf_paths = []
            for row in rows:
                pdf_path = sammelrechnungen_dir / row["filename"]
                if pdf_path.exists():
                    pdf_paths.append(str(pdf_path.relative_to(BASE_DIR)))

            if not pdf_paths:
                return jsonify({"success": False, "error": "Keine PDF-Dateien gefunden"}), 404

            return jsonify({
                "success": True,
                "pdf_paths": pdf_paths,
                "count": len(pdf_paths)
            })
        except Exception as e:
            logging.error(f"Error getting rX-selected PDFs: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    @app.route("/api/send-letterxpress", methods=["POST"])
    def send_via_letterxpress():
        """Send collective invoices via LetterXpress API."""
        try:
            data = request.json
            if not data:
                return jsonify({"success": False, "error": "Keine Daten übermittelt"}), 400

            # Get list of relative paths to PDFs
            pdf_paths = data.get("pdf_paths", [])
            if not pdf_paths:
                return jsonify({"success": False, "error": "Keine PDFs ausgewählt"}), 400

            # Get LetterXpress options from request (with defaults)
            color = data.get("color", "1")  # Default: black/white printing
            print_mode = data.get("mode", "duplex")  # Default: double-sided
            shipping = data.get("shipping", "national")  # Default: Germany
            registered = data.get("registered")  # Default: None (no registered mail)
            api_mode = data.get("api_mode")  # Optional: override API mode (test/live)

            # Validate options
            if color not in ["1", "4"]:
                return jsonify({"success": False, "error": "Ungültige Farboption"}), 400
            if print_mode not in ["simplex", "duplex"]:
                return jsonify({"success": False, "error": "Ungültiger Druckmodus"}), 400
            if shipping not in ["national", "international"]:
                return jsonify({"success": False, "error": "Ungültige Versandart"}), 400
            if registered and registered not in ["r1", "r2"]:
                return jsonify({"success": False, "error": "Ungültige Einschreiben-Option"}), 400
            if api_mode and api_mode not in ["test", "live"]:
                return jsonify({"success": False, "error": "Ungültiger API-Modus"}), 400

            # Initialize LetterXpress client
            try:
                # Use api_mode from request if provided, otherwise use default from env
                lx_client = LetterXpressClient(mode=api_mode) if api_mode else LetterXpressClient()
                mode = lx_client.mode
                logging.info(f"LetterXpress client initialized in {mode.upper()} mode")
            except Exception as e:
                logging.error(f"Failed to initialize LetterXpress client: {e}")
                return jsonify({
                    "success": False,
                    "error": f"LetterXpress-Client konnte nicht initialisiert werden: {str(e)}"
                }), 500

            # Check balance first
            try:
                balance, currency = lx_client.check_balance()
                logging.info(f"LetterXpress balance: {balance} {currency}")
            except Exception as e:
                logging.warning(f"Could not check balance: {e}")
                balance, currency = None, None

            # Convert relative paths to absolute paths
            results = []
            base_dir = BASE_DIR.resolve()

            for relative_path in pdf_paths:
                try:
                    # Resolve the PDF path
                    pdf_path = (base_dir / relative_path).resolve()

                    # Security check: ensure path is within BASE_DIR
                    try:
                        pdf_path.relative_to(base_dir)
                    except ValueError:
                        results.append({
                            "success": False,
                            "filename": relative_path,
                            "error": "Ungültiger Pfad"
                        })
                        continue

                    # Check if file exists
                    if not pdf_path.exists():
                        results.append({
                            "success": False,
                            "filename": relative_path,
                            "error": "Datei nicht gefunden"
                        })
                        continue

                    # Extract customer name from filename for notice
                    filename = pdf_path.name
                    customer_name = filename.replace("Sammelrechnung_", "").replace(".pdf", "")

                    # Submit to LetterXpress
                    logging.info(f"Submitting {filename} to LetterXpress ({mode.upper()} mode) - "
                               f"color={color}, print_mode={print_mode}, shipping={shipping}, registered={registered}")
                    result = lx_client.submit_letter(
                        pdf_path=pdf_path,
                        color=color,
                        mode=print_mode,
                        shipping=shipping,
                        registered=registered,
                        notice=f"Sammelrechnung {customer_name}",
                        filename_original=filename
                    )

                    job_id = result.get("id")
                    price = result.get("price", 0.0)

                    # Save to database
                    try:
                        with sqlite3.connect(app.config["DATABASE"]) as db_conn:
                            # Extract month and customer name from filename
                            # Format: Sammelrechnung_2025-11_CustomerName.pdf
                            parts = filename.replace(".pdf", "").split("_", 2)
                            month = parts[1] if len(parts) > 1 else None
                            customer = parts[2] if len(parts) > 2 else customer_name

                            db_conn.execute(
                                """
                                INSERT OR REPLACE INTO sammelrechnungen_letterxpress
                                (filename, letterxpress_job_id, mode, price, customer_name, month)
                                VALUES (?, ?, ?, ?, ?, ?)
                                """,
                                (filename, job_id, mode, price, customer, month)
                            )

                            # Log event for all invoices in this collective invoice
                            cursor = db_conn.execute(
                                "SELECT invoice_id FROM collective_invoice_items WHERE collective_invoice_filename = ?",
                                (filename,)
                            )
                            invoice_ids = [row[0] for row in cursor.fetchall()]
                            for inv_id in invoice_ids:
                                log_invoice_event(
                                    db_conn,
                                    inv_id,
                                    "COLLECTIVE_INVOICE_SENT",
                                    {
                                        "letterxpress_job_id": job_id,
                                        "price": price,
                                        "mode": mode,
                                        "filename": filename
                                    }
                                )

                            db_conn.commit()
                            logging.info(f"Saved LetterXpress job {job_id} for {filename} to database")
                    except Exception as db_err:
                        logging.error(f"Failed to save job to database: {db_err}")

                    results.append({
                        "success": True,
                        "filename": filename,
                        "job_id": job_id,
                        "price": price,
                        "mode": mode
                    })

                    logging.info(f"Successfully submitted {filename} (Job ID: {job_id}, Price: {price} EUR)")

                except Exception as e:
                    logging.error(f"Failed to submit {relative_path}: {e}")
                    results.append({
                        "success": False,
                        "filename": relative_path,
                        "error": str(e)
                    })

            # Calculate statistics
            success_count = sum(1 for r in results if r["success"])
            total_price = sum(r.get("price", 0.0) for r in results if r["success"])

            return jsonify({
                "success": True,
                "mode": mode,
                "balance": balance,
                "currency": currency,
                "results": results,
                "statistics": {
                    "total": len(results),
                    "successful": success_count,
                    "failed": len(results) - success_count,
                    "total_price": total_price
                }
            })

        except Exception as e:
            logging.error(f"Error in send_via_letterxpress: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    @app.route("/api/send-letterxpress-reminders", methods=["POST"])
    def send_reminders_via_letterxpress():
        """Send reminders (Mahnungen) via LetterXpress API."""
        try:
            data = request.json
            if not data:
                return jsonify({"success": False, "error": "Keine Daten übermittelt"}), 400

            # Get list of relative paths to PDFs
            pdf_paths = data.get("pdf_paths", [])
            if not pdf_paths:
                return jsonify({"success": False, "error": "Keine PDFs ausgewählt"}), 400

            # Get LetterXpress options from request (with defaults)
            color = data.get("color", "1")  # Default: black/white printing
            print_mode = data.get("mode", "duplex")  # Default: double-sided
            shipping = data.get("shipping", "national")  # Default: Germany
            registered = data.get("registered")  # Default: None (no registered mail)
            api_mode = data.get("api_mode")  # Optional: override API mode (test/live)
            include_original_invoices = data.get("include_original_invoices", True)  # Include original invoices as additional pages

            # Validate options
            if color not in ["1", "4"]:
                return jsonify({"success": False, "error": "Ungültige Farboption"}), 400
            if print_mode not in ["simplex", "duplex"]:
                return jsonify({"success": False, "error": "Ungültiger Druckmodus"}), 400
            if shipping not in ["national", "international"]:
                return jsonify({"success": False, "error": "Ungültige Versandart"}), 400
            if registered and registered not in ["r1", "r2"]:
                return jsonify({"success": False, "error": "Ungültige Einschreiben-Option"}), 400
            if api_mode and api_mode not in ["test", "live"]:
                return jsonify({"success": False, "error": "Ungültiger API-Modus"}), 400

            # Initialize LetterXpress client
            try:
                # Use api_mode from request if provided, otherwise use default from env
                lx_client = LetterXpressClient(mode=api_mode) if api_mode else LetterXpressClient()
                mode = lx_client.mode
                logging.info(f"LetterXpress client initialized in {mode.upper()} mode for reminders")
            except Exception as e:
                logging.error(f"Failed to initialize LetterXpress client: {e}")
                return jsonify({
                    "success": False,
                    "error": f"LetterXpress-Client konnte nicht initialisiert werden: {str(e)}"
                }), 500

            # Check balance first
            try:
                balance, currency = lx_client.check_balance()
                logging.info(f"LetterXpress balance: {balance} {currency}")
            except Exception as e:
                logging.warning(f"Could not check balance: {e}")
                balance, currency = None, None

            # Convert relative paths to absolute paths
            results = []
            base_dir = BASE_DIR.resolve()

            for relative_path in pdf_paths:
                try:
                    # Resolve the PDF path
                    pdf_path = (base_dir / relative_path).resolve()

                    # Security check: ensure path is within BASE_DIR
                    try:
                        pdf_path.relative_to(base_dir)
                    except ValueError:
                        results.append({
                            "success": False,
                            "filename": relative_path,
                            "error": "Ungültiger Pfad"
                        })
                        continue

                    # Check if file exists
                    if not pdf_path.exists():
                        results.append({
                            "success": False,
                            "filename": relative_path,
                            "error": "Datei nicht gefunden"
                        })
                        continue

                    # Extract customer name from filename for notice
                    filename = pdf_path.name
                    # Format: Mahnung_CustomerName_2025-01-15.pdf or similar
                    customer_name = filename.replace(".pdf", "").split("_")[1] if "_" in filename else "Kunde"

                    # Determine which PDF to send
                    pdf_to_send = pdf_path
                    temp_file = None

                    if not include_original_invoices:
                        # Extract only the reminder letter (first 2 pages) without original invoices
                        try:
                            reader = PdfReader(pdf_path)
                            if len(reader.pages) > 2:
                                writer = PdfWriter()
                                # Add only the first 2 pages (the reminder letter)
                                for i in range(min(2, len(reader.pages))):
                                    writer.add_page(reader.pages[i])

                                # Create temporary file for the letter-only PDF
                                temp_file = tempfile.NamedTemporaryFile(suffix='.pdf', delete=False)
                                writer.write(temp_file)
                                temp_file.close()
                                pdf_to_send = Path(temp_file.name)
                                logging.info(f"Created letter-only PDF (2 pages) for {filename}")
                        except Exception as e:
                            logging.warning(f"Could not extract letter-only PDF for {filename}: {e}. Using full PDF.")

                    # Submit to LetterXpress
                    logging.info(f"Submitting {filename} to LetterXpress ({mode.upper()} mode) - "
                               f"color={color}, print_mode={print_mode}, shipping={shipping}, registered={registered}, "
                               f"include_invoices={include_original_invoices}")
                    result = lx_client.submit_letter(
                        pdf_path=pdf_to_send,
                        color=color,
                        mode=print_mode,
                        shipping=shipping,
                        registered=registered,
                        notice=f"Mahnung {customer_name}",
                        filename_original=filename
                    )

                    # Clean up temporary file if created
                    if temp_file:
                        try:
                            Path(temp_file.name).unlink()
                        except Exception:
                            pass

                    job_id = result.get("id")
                    price = result.get("price", 0.0)

                    # Save to database
                    try:
                        with sqlite3.connect(app.config["DATABASE"]) as db_conn:
                            db_conn.execute(
                                """
                                INSERT OR REPLACE INTO mahnungen_letterxpress
                                (filename, pdf_path, letterxpress_job_id, mode, price, customer_name, submitted_at)
                                VALUES (?, ?, ?, ?, ?, ?, datetime('now', 'localtime'))
                                """,
                                (filename, relative_path, job_id, mode, price, customer_name)
                            )

                            # Log event for all invoices associated with this reminder PDF
                            cursor = db_conn.execute(
                                "SELECT invoice_id, reminder_level FROM reminders WHERE pdf_path = ?",
                                (relative_path,)
                            )
                            reminder_rows = cursor.fetchall()
                            for inv_id, reminder_level in reminder_rows:
                                log_invoice_event(
                                    db_conn,
                                    inv_id,
                                    "REMINDER_SENT",
                                    {
                                        "letterxpress_job_id": job_id,
                                        "price": price,
                                        "mode": mode,
                                        "reminder_level": reminder_level,
                                        "filename": filename
                                    }
                                )

                            db_conn.commit()
                            logging.info(f"Saved LetterXpress job {job_id} for {filename} to database")
                    except Exception as db_err:
                        logging.error(f"Failed to save job to database: {db_err}")

                    results.append({
                        "success": True,
                        "filename": filename,
                        "job_id": job_id,
                        "price": price,
                        "mode": mode
                    })

                    logging.info(f"Successfully submitted {filename} (Job ID: {job_id}, Price: {price} EUR)")

                except Exception as e:
                    logging.error(f"Failed to submit {relative_path}: {e}")
                    results.append({
                        "success": False,
                        "filename": relative_path,
                        "error": str(e)
                    })

            # Calculate statistics
            success_count = sum(1 for r in results if r["success"])
            total_price = sum(r.get("price", 0.0) for r in results if r["success"])

            return jsonify({
                "success": True,
                "mode": mode,
                "balance": balance,
                "currency": currency,
                "results": results,
                "statistics": {
                    "total": len(results),
                    "successful": success_count,
                    "failed": len(results) - success_count,
                    "total_price": total_price
                }
            })

        except Exception as e:
            logging.error(f"Error in send_reminders_via_letterxpress: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    @app.route("/api/invoices/<int:invoice_id>/history", methods=["GET"])
    def get_invoice_history(invoice_id: int):
        """Get the complete history of events for a specific invoice."""
        try:
            with sqlite3.connect(app.config["DATABASE"]) as conn:
                conn.row_factory = sqlite3.Row
                init_db(conn)

                # Check if invoice exists
                invoice_check = conn.execute(
                    "SELECT id, customer_name, invoice_number, invoice_date, amount_cents FROM invoices WHERE id = ?",
                    (invoice_id,)
                ).fetchone()

                if not invoice_check:
                    return jsonify({"success": False, "error": "Rechnung nicht gefunden"}), 404

                # Get all history events for this invoice
                cursor = conn.execute(
                    """
                    SELECT
                        id,
                        event_type,
                        event_timestamp,
                        metadata
                    FROM invoice_history
                    WHERE invoice_id = ?
                    ORDER BY event_timestamp DESC
                    """,
                    (invoice_id,)
                )

                events = []
                for row in cursor.fetchall():
                    metadata_dict = json.loads(row["metadata"]) if row["metadata"] else {}
                    events.append({
                        "id": row["id"],
                        "event_type": row["event_type"],
                        "timestamp": row["event_timestamp"],
                        "metadata": metadata_dict
                    })

                return jsonify({
                    "success": True,
                    "invoice": {
                        "id": invoice_check["id"],
                        "customer_name": invoice_check["customer_name"],
                        "invoice_number": invoice_check["invoice_number"],
                        "invoice_date": invoice_check["invoice_date"],
                        "amount_eur": invoice_check["amount_cents"] / 100.0
                    },
                    "events": events
                })

        except Exception as e:
            logging.error(f"Error fetching invoice history: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    @app.route("/api/invoices/<int:invoice_id>/toggle-uncollectible", methods=["POST"])
    def toggle_uncollectible(invoice_id: int):
        """Toggle the uncollectible status of an invoice."""
        try:
            with sqlite3.connect(app.config["DATABASE"]) as conn:
                conn.row_factory = sqlite3.Row
                init_db(conn)

                # Check if invoice exists and get current status
                invoice = conn.execute(
                    "SELECT id, customer_name, invoice_number, uncollectible FROM invoices WHERE id = ?",
                    (invoice_id,)
                ).fetchone()

                if not invoice:
                    return jsonify({"success": False, "error": "Rechnung nicht gefunden"}), 404

                # Toggle the uncollectible status
                current_status = invoice["uncollectible"] or 0
                new_status = 0 if current_status else 1

                conn.execute(
                    "UPDATE invoices SET uncollectible = ? WHERE id = ?",
                    (new_status, invoice_id)
                )

                # Log the event in history
                event_type = "MARKED_UNCOLLECTIBLE" if new_status else "UNMARKED_UNCOLLECTIBLE"
                log_invoice_event(conn, invoice_id, event_type, {})

                conn.commit()

                return jsonify({
                    "success": True,
                    "uncollectible": bool(new_status)
                })

        except Exception as e:
            logging.error(f"Error toggling uncollectible status: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    @app.route("/pdf/merge")
    def merge_pdfs():
        """Merge multiple PDFs into one for printing."""
        from pypdf import PdfWriter
        from io import BytesIO

        paths_param = request.args.get("paths", "")
        if not paths_param:
            abort(400, "Keine PDF-Pfade angegeben")

        paths = paths_param.split(",")
        root = BASE_DIR.resolve()

        pdf_writer = PdfWriter()

        for relative_path in paths:
            target = (root / relative_path.strip()).resolve()
            try:
                target.relative_to(root)
            except ValueError:
                continue  # Skip invalid paths
            if not target.exists():
                continue

            try:
                from pypdf import PdfReader
                reader = PdfReader(str(target))
                for page in reader.pages:
                    pdf_writer.add_page(page)
            except Exception as e:
                logging.error(f"Error reading PDF {target}: {e}")
                continue

        if len(pdf_writer.pages) == 0:
            abort(404, "Keine gültigen PDFs gefunden")

        # Write merged PDF to memory
        output = BytesIO()
        pdf_writer.write(output)
        output.seek(0)

        return Response(
            output.getvalue(),
            mimetype="application/pdf",
            headers={
                "Content-Disposition": "inline; filename=Sammelrechnungen_Druck.pdf"
            }
        )

    @app.route("/pdf/<path:relative_path>")
    def serve_pdf(relative_path: str):
        # Allow serving PDFs from both Rechnungen and Sammelrechnungen folders
        # Use BASE_DIR as root instead of INVOICE_ROOT
        root = BASE_DIR.resolve()
        target = (root / relative_path).resolve()
        try:
            target.relative_to(root)
        except ValueError:
            abort(404)
        if not target.exists():
            abort(404)
        return send_from_directory(root, relative_path, mimetype="application/pdf")

    return app


def clamp_limit(raw_limit: Optional[str], max_limit: int) -> int:
    if not raw_limit:
        return max_limit
    try:
        value = int(raw_limit)
    except ValueError:
        return max_limit
    return max(1, min(value, max_limit))


def fetch_invoices(
    database_path: str,
    query: str,
    limit: int,
    time_filter: str = "current_month",
    status_filter: str = "all",
    from_month: str = "",
    to_month: str = "",
    email_filter: str = "all",
    uncollectible_filter: str = "hide",
    sort_by: str = "date",
    sort_direction: str = "desc",
    invoice_date_from: str = "",
    invoice_date_to: str = "",
) -> List[InvoiceRow]:
    """
    Fetch invoices with their payment status based on snapshot tracking.

    Time filter (Snapshot):
    - 'all': All snapshots
    - 'current_month': Only invoices present in the latest snapshot
    - 'custom': Snapshots within the provided month range (YYYY-MM)

    Status filter:
    - 'all': All statuses
    - 'open': Invoice appears in the latest snapshot
    - 'paid': Invoice doesn't appear in latest snapshot but appeared in earlier ones

    Uncollectible filter:
    - 'hide': Hide uncollectible invoices (default)
    - 'show': Show uncollectible invoices
    - 'only': Show only uncollectible invoices

    Invoice date range filter (Rechnungsdatum):
    - invoice_date_from: Start date in YYYY-MM-DD format
    - invoice_date_to: End date in YYYY-MM-DD format

    Custom date range:
    - from_month: Start month in YYYY-MM format
    - to_month: End month in YYYY-MM format
    """
    with sqlite3.connect(database_path) as conn:
        conn.row_factory = sqlite3.Row
        # Register helper used in ORDER BY to sort by surname
        conn.create_function("LAST_WORD", 1, sql_last_word)

        # Get the latest snapshot date
        latest_snapshot_row = conn.execute(
            "SELECT MAX(snapshot_date) as latest FROM snapshots"
        ).fetchone()

        if not latest_snapshot_row or not latest_snapshot_row["latest"]:
            # No snapshots yet
            return []

        latest_snapshot = latest_snapshot_row["latest"]

        # Snapshot filter configuration
        snapshot_filter_sql = ""
        snapshot_filter_params: List[str] = []
        snapshot_filter_active = False

        if time_filter == "current_month":
            snapshot_filter_sql += " AND s.snapshot_date = ?"
            snapshot_filter_params.append(latest_snapshot)
            snapshot_filter_active = True
        elif time_filter == "custom" and (from_month or to_month):
            snapshot_filter_active = True
            if from_month:
                snapshot_filter_sql += " AND s.snapshot_date >= ?"
                snapshot_filter_params.append(from_month)
            if to_month:
                snapshot_filter_sql += " AND s.snapshot_date <= ?"
                snapshot_filter_params.append(to_month)

        # Build the main query
        sql = """
            WITH invoice_status AS (
                SELECT
                    i.id,
                    i.invoice_number,
                    i.invoice_date,
                    i.customer_name,
                    i.customer_address,
                    i.customer_street,
                    i.customer_city,
                    i.amount_cents,
                    i.uncollectible,
                    i.address_incomplete,
                    i.name_needs_review as name_needs_review_raw,
                    MAX(s.snapshot_date) as last_seen_snapshot,
                    MIN(s.snapshot_date) as first_seen_snapshot,
                    CASE
                        WHEN MAX(s.snapshot_date) = ? THEN 'open'
                        ELSE 'paid'
                    END as status
                FROM invoices i
                JOIN invoice_snapshots isnap ON i.id = isnap.invoice_id
                JOIN snapshots s ON isnap.snapshot_id = s.id
                GROUP BY i.id
            ),
            snapshot_files AS (
                SELECT
                    isnap.invoice_id,
                    s.snapshot_date,
                    isnap.file_path,
                    ROW_NUMBER() OVER (
                        PARTITION BY isnap.invoice_id
                        ORDER BY s.snapshot_date DESC
                    ) as rn
                FROM invoice_snapshots isnap
                JOIN snapshots s ON isnap.snapshot_id = s.id
                WHERE 1=1
                {snapshot_filter_sql}
            )
            SELECT
                ist.*,
                sf.file_path,
                CASE
                    WHEN EXISTS (
                        SELECT 1 FROM collective_invoice_items cii
                        WHERE cii.invoice_id = ist.id
                    ) THEN 1
                ELSE 0
                END as in_collective_invoice,
                cd.custom_name,
                cd.custom_street,
                cd.custom_city,
                -- If custom_name is set, user already corrected the name, so ignore name_needs_review
                CASE WHEN cd.custom_name IS NOT NULL AND cd.custom_name != '' THEN 0 ELSE ist.name_needs_review_raw END as name_needs_review
            FROM invoice_status ist
            LEFT JOIN snapshot_files sf ON ist.id = sf.invoice_id AND sf.rn = 1
            LEFT JOIN customer_details cd ON ist.customer_name = cd.customer_name
            WHERE 1=1
        """

        # The format string is safe because snapshot_filter_sql is built from static fragments
        sql = sql.format(snapshot_filter_sql=snapshot_filter_sql)

        params: List[Any] = [latest_snapshot]
        params.extend(snapshot_filter_params)

        # Apply uncollectible filter
        if uncollectible_filter == "hide":
            sql += " AND (ist.uncollectible IS NULL OR ist.uncollectible = 0)"
        elif uncollectible_filter == "only":
            sql += " AND ist.uncollectible = 1"
        # If uncollectible_filter == "show", don't add any filter (show all)

        # Apply search filter
        if query:
            sql += """
                AND (ist.customer_name LIKE ?
                     OR ist.invoice_number LIKE ?
                     OR ist.customer_address LIKE ?
                     OR ist.customer_street LIKE ?
                     OR ist.customer_city LIKE ?)
            """
            pattern = f"%{query}%"
            params.extend([pattern, pattern, pattern, pattern, pattern])

        # Require the invoice to be present in the requested snapshot range
        if snapshot_filter_active:
            sql += " AND sf.invoice_id IS NOT NULL"

        # Apply status filter
        if status_filter == "open":
            sql += " AND ist.status = 'open'"
        elif status_filter == "paid":
            sql += " AND ist.status = 'paid'"

        # Apply email filter
        if email_filter == "with_email":
            sql += " AND cd.email IS NOT NULL AND cd.email != ''"
        elif email_filter == "without_email":
            sql += " AND (cd.email IS NULL OR cd.email = '')"

        # Apply invoice date range filter (Rechnungsdatum)
        if invoice_date_from:
            sql += " AND ist.invoice_date >= ?"
            params.append(invoice_date_from)
        if invoice_date_to:
            sql += " AND ist.invoice_date <= ?"
            params.append(invoice_date_to)

        sort_key, sort_dir = normalize_sort_params(sort_by, sort_direction)
        order_expression = SORT_COLUMN_MAP[sort_key]

        sql += f" ORDER BY {order_expression} {sort_dir.upper()}, ist.id DESC LIMIT ?"
        params.append(limit)

        rows = conn.execute(sql, params).fetchall()

    return [row_from_sql(row) for row in rows]


def row_from_sql(row: sqlite3.Row) -> InvoiceRow:
    # Get custom values from customer_details if available
    custom_name = row["custom_name"] if "custom_name" in row.keys() and row["custom_name"] else None
    custom_street = row["custom_street"] if "custom_street" in row.keys() and row["custom_street"] else None
    custom_city = row["custom_city"] if "custom_city" in row.keys() and row["custom_city"] else None

    # Get street and city if available
    customer_street = custom_street or (row["customer_street"] if "customer_street" in row.keys() else None)
    customer_city = custom_city or (row["customer_city"] if "customer_city" in row.keys() else None)

    # Use custom_name if available, otherwise use original customer_name
    customer_name = custom_name or row["customer_name"]

    # If street and city are available, construct address from them
    # Otherwise use the old customer_address field
    if customer_street and customer_city:
        customer_address = f"{customer_street}, {customer_city}"
    else:
        customer_address = row["customer_address"]

    return InvoiceRow(
        id=row["id"],
        invoice_number=row["invoice_number"],
        invoice_date=row["invoice_date"],
        customer_name=customer_name,
        customer_address=customer_address,
        amount_cents=row["amount_cents"],
        status=row["status"],
        last_seen_snapshot=row["last_seen_snapshot"],
        first_seen_snapshot=row["first_seen_snapshot"],
        file_path=row["file_path"] if "file_path" in row.keys() else None,
        in_collective_invoice=bool(row["in_collective_invoice"]) if "in_collective_invoice" in row.keys() else False,
        customer_street=customer_street,
        customer_city=customer_city,
        address_incomplete=bool(row["address_incomplete"]) if "address_incomplete" in row.keys() else False,
        name_needs_review=bool(row["name_needs_review"]) if "name_needs_review" in row.keys() else False,
    )


def group_by_customer(invoices: List[InvoiceRow]) -> List[Dict]:
    """Group invoices by customer name, returning a list of customer groups."""
    groups = defaultdict(list)
    for invoice in invoices:
        groups[invoice.customer_name].append(invoice)

    # Convert to list of dicts with summary info
    result = []
    for customer_name, customer_invoices in sorted(groups.items()):
        total = sum(inv.amount_eur for inv in customer_invoices)
        result.append({
            "customer_name": customer_name,
            "customer_address": customer_invoices[0].customer_address,
            "invoice_count": len(customer_invoices),
            "total_amount": total,
            "invoices": sorted(customer_invoices, key=lambda x: x.invoice_date, reverse=True),
        })

    # Sort by total amount descending
    result.sort(key=lambda x: x["total_amount"], reverse=True)
    return result


def calculate_months_open(invoice_date_str: str) -> int:
    """Calculate how many months an invoice has been open."""
    try:
        invoice_date = datetime.strptime(invoice_date_str, "%Y-%m-%d").date()
        today = date.today()

        # Calculate month difference
        months_diff = (today.year - invoice_date.year) * 12 + (today.month - invoice_date.month)
        return max(0, months_diff)
    except (ValueError, AttributeError):
        return 0


def get_recommended_reminder_level(months_open: int, last_reminder_level: Optional[int]) -> Optional[int]:
    """
    Determine the recommended reminder level based on how long the invoice has been open.

    Rules:
    - ALWAYS start with Zahlungserinnerung (level 0) when > 2 months (>= 3 months)
    - Then 1. Mahnung (level 1) after level 0 was sent and >= 3 months
    - Then 2. Mahnung (level 2, Einschreiben) after level 1 was sent and >= 4 months

    Important: Levels must be sent in sequence! Never skip a level.
    """
    if months_open < 3:
        return None

    if last_reminder_level is None:
        # No reminders sent yet - ALWAYS start with level 0
        if months_open >= 3:
            return 0
        return None
    elif last_reminder_level == 0:
        # Zahlungserinnerung was sent, recommend 1. Mahnung if >= 3 months
        if months_open >= 3:
            return 1
        return None
    elif last_reminder_level == 1:
        # 1. Mahnung was sent, recommend 2. Mahnung if >= 4 months
        if months_open >= 4:
            return 2
        return None
    else:
        # Already at max level (2)
        return None


def fetch_all_customers(database_path: str) -> List[Dict]:
    """
    Fetch all unique customers from invoices with their details.
    Returns a list of customer dictionaries with name, address, email, notes.
    Custom name/street/city from customer_details will override invoice data if present.
    """
    with sqlite3.connect(database_path) as conn:
        conn.row_factory = sqlite3.Row
        init_db(conn)

        # Get all unique customers from invoices with their details
        sql = """
            SELECT
                i.customer_name,
                i.customer_address,
                i.customer_street,
                i.customer_city,
                MAX(i.address_incomplete) as address_incomplete,
                -- If custom_name is set, user already corrected the name, so ignore name_needs_review
                CASE WHEN cd.custom_name IS NOT NULL AND cd.custom_name != '' THEN 0 ELSE MAX(i.name_needs_review) END as name_needs_review,
                cd.salutation,
                cd.email,
                cd.notes,
                cd.never_remind,
                cd.bank_debit,
                cd.print_only,
                cd.custom_name,
                cd.custom_street,
                cd.custom_city,
                COUNT(DISTINCT i.id) as invoice_count,
                SUM(i.amount_cents) as total_amount_cents
            FROM invoices i
            LEFT JOIN customer_details cd ON i.customer_name = cd.customer_name
            GROUP BY i.customer_name, i.customer_address, i.customer_street, i.customer_city, cd.salutation, cd.email, cd.notes, cd.never_remind, cd.bank_debit, cd.print_only, cd.custom_name, cd.custom_street, cd.custom_city
            ORDER BY i.customer_name
        """

        rows = conn.execute(sql).fetchall()

    customers = []
    for row in rows:
        # Use custom values if available, otherwise fall back to invoice data
        display_name = row["custom_name"] if row["custom_name"] else row["customer_name"]
        display_street = row["custom_street"] if row["custom_street"] else (row["customer_street"] or "")
        display_city = row["custom_city"] if row["custom_city"] else (row["customer_city"] or "")

        customers.append({
            "customer_name": row["customer_name"],  # Keep original for identification
            "display_name": display_name,  # Display name (custom or original)
            "customer_address": row["customer_address"],
            "customer_street": display_street,
            "customer_city": display_city,
            "custom_name": row["custom_name"] or "",  # For editing
            "custom_street": row["custom_street"] or "",  # For editing
            "custom_city": row["custom_city"] or "",  # For editing
            "salutation": row["salutation"] or "",
            "email": row["email"] or "",
            "notes": row["notes"] or "",
            "never_remind": row["never_remind"] or 0,
            "bank_debit": row["bank_debit"] or 0,
            "print_only": row["print_only"] or 0,
            "address_incomplete": row["address_incomplete"] or 0,
            "name_needs_review": row["name_needs_review"] or 0,
            "invoice_count": row["invoice_count"],
            "total_amount_eur": row["total_amount_cents"] / 100.0 if row["total_amount_cents"] else 0.0,
        })

    # Sort by last name (last word of display_name), case-insensitive
    def get_last_name(customer: Dict) -> str:
        name = customer.get("display_name", "")
        parts = name.strip().split()
        return parts[-1].lower() if parts else ""

    customers.sort(key=get_last_name)

    return customers


def fetch_invoices_with_reminders(database_path: str, filter_reminded: Optional[bool] = None, hide_never_remind: bool = True) -> List[InvoiceWithReminder]:
    """
    Fetch open invoices with their reminder information.

    Args:
        database_path: Path to the database
        filter_reminded: If True, only show invoices with reminders. If False, only show invoices without reminders.
                        If None, show all open invoices.
        hide_never_remind: If True (default), hide customers with never_remind flag set. If False, show all.
    """
    with sqlite3.connect(database_path) as conn:
        conn.row_factory = sqlite3.Row

        # Get the latest snapshot date
        latest_snapshot_row = conn.execute(
            "SELECT MAX(snapshot_date) as latest FROM snapshots"
        ).fetchone()

        if not latest_snapshot_row or not latest_snapshot_row["latest"]:
            return []

        latest_snapshot = latest_snapshot_row["latest"]

        # Query to get open invoices with reminder info
        sql = """
            WITH invoice_status AS (
                SELECT
                    i.id,
                    i.invoice_number,
                    i.invoice_date,
                    i.customer_name,
                    i.customer_address,
                    i.customer_street,
                    i.customer_city,
                    i.amount_cents,
                    i.uncollectible,
                    MAX(s.snapshot_date) as last_seen_snapshot,
                    MIN(s.snapshot_date) as first_seen_snapshot,
                    CASE
                        WHEN MAX(s.snapshot_date) = ? THEN 'open'
                        ELSE 'paid'
                    END as status
                FROM invoices i
                JOIN invoice_snapshots isnap ON i.id = isnap.invoice_id
                JOIN snapshots s ON isnap.snapshot_id = s.id
                GROUP BY i.id
                HAVING status = 'open'
            ),
            last_reminder AS (
                SELECT
                    r.invoice_id,
                    r.reminder_level as last_reminder_level,
                    r.sent_date as last_reminder_date,
                    r.letterexpress_status,
                    r.pdf_path
                FROM reminders r
                INNER JOIN (
                    SELECT invoice_id, MAX(created_at) as max_created
                    FROM reminders
                    WHERE invoice_id IN (SELECT id FROM invoice_status)
                    GROUP BY invoice_id
                ) latest ON r.invoice_id = latest.invoice_id AND r.created_at = latest.max_created
            ),
            reminder_group_counts AS (
                SELECT
                    pdf_path,
                    COUNT(*) as invoices_in_group
                FROM last_reminder
                WHERE pdf_path IS NOT NULL
                GROUP BY pdf_path
            ),
            invoice_files AS (
                SELECT
                    ist.id,
                    CASE
                        WHEN isnap.file_path IS NOT NULL THEN isnap.file_path
                        ELSE NULL
                    END as file_path
                FROM invoice_status ist
                LEFT JOIN invoice_snapshots isnap ON ist.id = isnap.invoice_id
                LEFT JOIN snapshots s ON isnap.snapshot_id = s.id
                WHERE s.snapshot_date = ist.last_seen_snapshot
                GROUP BY ist.id
            )
            SELECT
                ist.*,
                if.file_path,
                lr.last_reminder_level,
                lr.last_reminder_date,
                lr.letterexpress_status,
                lr.pdf_path as reminder_pdf_path,
                CASE WHEN lr.invoice_id IS NOT NULL THEN 1 ELSE 0 END as has_reminders,
                COALESCE(rgc.invoices_in_group, 1) as invoices_in_group,
                COALESCE(cd.never_remind, 0) as never_remind,
                cd.custom_name,
                cd.custom_street,
                cd.custom_city
            FROM invoice_status ist
            LEFT JOIN invoice_files if ON ist.id = if.id
            LEFT JOIN last_reminder lr ON ist.id = lr.invoice_id
            LEFT JOIN reminder_group_counts rgc ON lr.pdf_path = rgc.pdf_path
            LEFT JOIN customer_details cd ON ist.customer_name = cd.customer_name
            WHERE 1=1
        """

        params = [latest_snapshot]

        # Apply never_remind filter (hide customers with never_remind=1 by default)
        if hide_never_remind:
            sql += " AND COALESCE(cd.never_remind, 0) = 0"

        # Apply reminder filter
        if filter_reminded is True:
            sql += " AND lr.invoice_id IS NOT NULL"
        elif filter_reminded is False:
            sql += " AND lr.invoice_id IS NULL"

        sql += " ORDER BY ist.invoice_date ASC"

        rows = conn.execute(sql, params).fetchall()

    result = []
    for row in rows:
        months_open = calculate_months_open(row["invoice_date"])
        recommended_level = get_recommended_reminder_level(
            months_open,
            row["last_reminder_level"]
        )

        # Get custom values from customer_details if available
        custom_name = row["custom_name"] if "custom_name" in row.keys() and row["custom_name"] else None
        custom_street = row["custom_street"] if "custom_street" in row.keys() and row["custom_street"] else None
        custom_city = row["custom_city"] if "custom_city" in row.keys() and row["custom_city"] else None

        # Get original street and city
        original_street = row["customer_street"] if "customer_street" in row.keys() else None
        original_city = row["customer_city"] if "customer_city" in row.keys() else None

        # Use custom values if available, otherwise use originals
        customer_street = custom_street or original_street
        customer_city = custom_city or original_city

        # Use custom_name if available, otherwise use original customer_name
        customer_name = custom_name or row["customer_name"]

        # Construct address from street and city (prefer custom over original)
        if customer_street and customer_city:
            customer_address = f"{customer_street}, {customer_city}"
        else:
            customer_address = row["customer_address"]

        invoice = InvoiceWithReminder(
            id=row["id"],
            invoice_number=row["invoice_number"],
            invoice_date=row["invoice_date"],
            customer_name=customer_name,
            customer_address=customer_address,
            amount_cents=row["amount_cents"],
            status=row["status"],
            last_seen_snapshot=row["last_seen_snapshot"],
            first_seen_snapshot=row["first_seen_snapshot"],
            file_path=row["file_path"] if "file_path" in row.keys() else None,
            uncollectible=row["uncollectible"] if "uncollectible" in row.keys() and row["uncollectible"] is not None else 0,
            months_open=months_open,
            recommended_level=recommended_level,
            last_reminder_level=row["last_reminder_level"],
            last_reminder_date=row["last_reminder_date"],
            letterexpress_status=row["letterexpress_status"],
            has_reminders=bool(row["has_reminders"]),
            reminder_pdf_path=row["reminder_pdf_path"] if "reminder_pdf_path" in row.keys() else None,
            invoices_in_group=row["invoices_in_group"] if "invoices_in_group" in row.keys() else 1,
            customer_street=customer_street,
            customer_city=customer_city,
        )
        result.append(invoice)

    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Start the invoice web dashboard.")
    parser.add_argument("--host", default="127.0.0.1", help="Bind address (default: %(default)s)")
    parser.add_argument("--port", type=int, default=8080, help="Port (default: %(default)s)")
    parser.add_argument("--debug", action="store_true", help="Enable Flask debug mode.")
    parser.add_argument("--database", type=Path, default=DEFAULT_DB_PATH, help="Path to invoice_data.db")
    parser.add_argument("--root", type=Path, default=DEFAULT_INVOICE_ROOT, help="Root directory containing PDFs")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Maximum rows per request (default: %(default)s)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    app = create_app(
        {
            "DATABASE": str(args.database.resolve()),
            "INVOICE_ROOT": str(args.root.resolve()),
            "MAX_LIMIT": max(1, args.limit),
        }
    )
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
