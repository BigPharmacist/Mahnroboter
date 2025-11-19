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
from typing import Iterable, List, Optional, Dict, Tuple
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
from invoice_tracker import (
    find_pdfs,
    init_db,
    process_pdf_file,
)
from letterxpress_client import LetterXpressClient

# Load environment variables
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DB_PATH = BASE_DIR / "invoice_data.db"
DEFAULT_INVOICE_ROOT = BASE_DIR / "Rechnungen"
DEFAULT_LIMIT = 500

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
    "name": "LOWER(ist.customer_name)",
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

        # Format subject with month/year
        subject = f"Ihre Monatsrechnungen {month_year}"
        msg['Subject'] = subject

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

        # Adjust message based on number of invoices
        invoice_count = len(invoice_pdf_paths)
        if invoice_count == 1:
            invoice_text = "anbei senden wir Ihnen Ihre aktuelle Monatsrechnung."
        else:
            invoice_text = f"anbei senden wir Ihnen Ihre {invoice_count} Monatsrechnungen."

        email_body = f"""{greeting},

{invoice_text}

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


def create_cover_letter_pdf(
    customer_name: str,
    customer_address: str,
    current_month_invoices: List[Dict],
    older_open_invoices: List[Dict],
    salutation: Optional[str] = None
) -> bytes:
    """
    Create a cover letter PDF with recipient address positioned at 66-88mm from top.

    Args:
        customer_name: Name of the customer
        customer_address: Full address of the customer
        current_month_invoices: List of invoices from the latest month with date, number, and amount
        older_open_invoices: List of older open invoices with date, number, and amount
        salutation: Salutation for the customer (e.g., "Herr", "Frau")

    Returns:
        PDF bytes
    """
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4

    # Pharmacy info (top right)
    c.setFont("Helvetica-Bold", 11)
    c.drawString(360, height - 50, "Apotheke am Damm")
    c.setFont("Helvetica", 10)
    c.drawString(360, height - 65, "Am Damm 17")
    c.drawString(360, height - 80, "55232 Alzey")
    c.drawString(360, height - 95, "Tel.: 06731-548846")
    c.drawString(360, height - 110, "Fax: 06731-548847")

    # Return address (small, above recipient address)
    left_margin = 25 * mm
    return_address_y = height - (20 * mm)
    c.setFont("Helvetica", 8)
    c.drawString(left_margin, return_address_y, "Apotheke am Damm, Am Damm 17, 55232 Alzey")

    # Recipient address (must be between 66mm and 88mm from top - DIN 5008)
    recipient_y_start = height - (66 * mm)
    c.setFont("Helvetica", 11)

    # Parse address
    address_lines = customer_address.split('\n') if '\n' in customer_address else customer_address.split(',')
    address_lines = [line.strip() for line in address_lines if line.strip()]

    # Determine greeting
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

    # Date
    date_y = height - (106 * mm)
    today = datetime.now().strftime("%d.%m.%Y")
    c.setFont("Helvetica", 10)
    c.drawRightString(width - 25 * mm, date_y, f"Alzey, {today}")

    # Subject line
    subject_y = date_y - 20
    c.setFont("Helvetica-Bold", 12)

    # Get the month/year from first current invoice
    if current_month_invoices:
        first_date = datetime.strptime(current_month_invoices[0]['date'], '%Y-%m-%d')
        month_year = first_date.strftime("%m.%Y")
        if len(current_month_invoices) == 1:
            c.drawString(left_margin, subject_y, f"Ihre Monatsrechnung {month_year}")
        else:
            c.drawString(left_margin, subject_y, f"Ihre Monatsrechnungen {month_year}")
    else:
        c.drawString(left_margin, subject_y, "Ihre Monatsrechnungen")

    # Salutation
    content_y = subject_y - 25
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

    # Main text
    content_y -= 20
    if len(current_month_invoices) == 1:
        c.drawString(left_margin, content_y, "anbei erhalten Sie Ihre aktuelle Rechnung:")
    else:
        c.drawString(left_margin, content_y, "anbei erhalten Sie Ihre aktuellen Rechnungen:")

    # Table for current month invoices
    content_y -= 20
    c.setFont("Helvetica-Bold", 9)

    # Table header
    col1_x = left_margin + 10  # Rechnungsnummer
    col2_x = left_margin + 120  # Datum
    col3_x = left_margin + 220  # Betrag (rechts ausgerichtet)

    c.drawString(col1_x, content_y, "Rechnungs-Nr.")
    c.drawString(col2_x, content_y, "Datum")
    c.drawRightString(col3_x, content_y, "Betrag")

    content_y -= 2
    c.line(left_margin + 10, content_y, col3_x, content_y)
    content_y -= 10

    # Table rows
    c.setFont("Helvetica", 9)
    total_current = 0.0
    for inv in current_month_invoices:
        inv_date_str = datetime.strptime(inv['date'], '%Y-%m-%d').strftime('%d.%m.%Y')
        c.drawString(col1_x, content_y, inv['number'])
        c.drawString(col2_x, content_y, inv_date_str)
        c.drawRightString(col3_x, content_y, f"{inv['amount']:.2f} €")
        total_current += inv['amount']
        content_y -= 12

    # Total line
    content_y -= 2
    c.line(left_margin + 10, content_y, col3_x, content_y)
    content_y -= 10
    c.setFont("Helvetica-Bold", 9)
    c.drawString(col1_x, content_y, "Gesamtsumme:")
    c.drawRightString(col3_x, content_y, f"{total_current:.2f} €")

    # Older open invoices section
    if older_open_invoices:
        content_y -= 20
        c.setFont("Helvetica", 10)
        c.drawString(left_margin, content_y, "Bitte beachten Sie außerdem folgende noch offene Rechnungen:")

        content_y -= 15
        c.setFont("Helvetica-Bold", 9)

        # Table header for older invoices
        c.drawString(col1_x, content_y, "Rechnungs-Nr.")
        c.drawString(col2_x, content_y, "Datum")
        c.drawRightString(col3_x, content_y, "Betrag")

        content_y -= 2
        c.line(left_margin + 10, content_y, col3_x, content_y)
        content_y -= 10

        # Table rows for older invoices
        c.setFont("Helvetica", 9)
        total_older = 0.0
        for inv in older_open_invoices:
            inv_date_str = datetime.strptime(inv['date'], '%Y-%m-%d').strftime('%d.%m.%Y')
            c.drawString(col1_x, content_y, inv['number'])
            c.drawString(col2_x, content_y, inv_date_str)
            c.drawRightString(col3_x, content_y, f"{inv['amount']:.2f} €")
            total_older += inv['amount']
            content_y -= 12

        # Total line for older invoices
        content_y -= 2
        c.line(left_margin + 10, content_y, col3_x, content_y)
        content_y -= 10
        c.setFont("Helvetica-Bold", 9)
        c.drawString(col1_x, content_y, "Summe offener Rechnungen:")
        c.drawRightString(col3_x, content_y, f"{total_older:.2f} €")

    # Closing
    content_y -= 20
    c.setFont("Helvetica", 10)
    c.drawString(left_margin, content_y, "Wir bedanken uns herzlich für Ihr Vertrauen und Ihre Treue.")

    content_y -= 20
    c.drawString(left_margin, content_y, "Mit freundlichen Grüßen")
    content_y -= 10
    c.setFont("Helvetica-Bold", 10)
    c.drawString(left_margin, content_y, "Ihr Team der Apotheke am Damm")

    # Footer
    footer_y = 80
    c.setFont("Helvetica", 8)
    c.line(left_margin, footer_y + 15, width - 25 * mm, footer_y + 15)
    c.drawString(left_margin, footer_y, "Bankverbindung: Sparkasse Worms-Alzey-Ried, IBAN: DE51 5535 0010 0033 7173 83, BIC: MALADE51WOR")
    c.drawString(left_margin, footer_y - 10, "Inhaber/in: Matthias Blüm")
    c.drawString(left_margin, footer_y - 20, "Gerichtsstand: Alzey | HRA-Nummer: 31710")

    # Page 2: Additional information
    c.showPage()

    # Pharmacy info (top right) on page 2
    c.setFont("Helvetica-Bold", 11)
    c.drawString(360, height - 50, "Apotheke am Damm")
    c.setFont("Helvetica", 10)
    c.drawString(360, height - 65, "Inh. Matthias Blüm, e.K.")
    c.drawString(360, height - 80, "Am Damm 17")
    c.drawString(360, height - 95, "55232 Alzey")
    c.drawString(360, height - 110, "Tel.: 06731-548846")
    c.drawString(360, height - 125, "info@apothekeamdamm.de")

    # Title
    info_y = height - 150
    c.setFont("Helvetica-Bold", 12)
    c.drawString(left_margin, info_y, "Weitere Informationen und Hinweise")

    info_y -= 25
    c.setFont("Helvetica", 10)

    # Paragraph 1
    c.drawString(left_margin, info_y, "Sollten Sie den Betrag bereits überwiesen haben, betrachten Sie dieses Schreiben bitte als")
    info_y -= 12
    c.drawString(left_margin, info_y, "gegenstandslos. In diesem Fall bitten wir um Entschuldigung für die Unannehmlichkeiten.")
    info_y -= 20

    # Paragraph 2
    c.drawString(left_margin, info_y, "Falls Sie Fragen zu den Rechnungspositionen haben oder in einer finanziellen Notlage sind,")
    info_y -= 12
    c.drawString(left_margin, info_y, "bitten wir Sie, sich umgehend mit uns in Verbindung zu setzen. Wir sind gerne bereit, mit")
    info_y -= 12
    c.drawString(left_margin, info_y, "Ihnen eine Ratenzahlungsvereinbarung zu treffen.")
    info_y -= 20

    # Paragraph 3
    c.drawString(left_margin, info_y, "Bitte beachten Sie, dass bei Nichtzahlung weitere Kosten auf Sie zukommen können,")
    info_y -= 12
    c.drawString(left_margin, info_y, "einschließlich Zinsen, Anwaltskosten und Gerichtsgebühren. Diese können den")
    info_y -= 12
    c.drawString(left_margin, info_y, "ursprünglichen Rechnungsbetrag erheblich erhöhen.")
    info_y -= 20

    # Paragraph 4
    c.drawString(left_margin, info_y, "Wir möchten Sie darauf hinweisen, dass ein gerichtliches Mahnverfahren auch negative")
    info_y -= 12
    c.drawString(left_margin, info_y, "Auswirkungen auf Ihre Bonität haben kann. Dies kann zukünftige Geschäftsbeziehungen und")
    info_y -= 12
    c.drawString(left_margin, info_y, "Kreditwürdigkeitsprüfungen beeinflussen.")
    info_y -= 20

    # Paragraph 5
    c.drawString(left_margin, info_y, "Ihre Gesundheit liegt uns am Herzen, und wir möchten unsere gute Geschäftsbeziehung")
    info_y -= 12
    c.drawString(left_margin, info_y, "fortführen. Daher bitten wir Sie eindringlich, den offenen Betrag zu begleichen oder sich mit")
    info_y -= 12
    c.drawString(left_margin, info_y, "uns in Verbindung zu setzen, um eine Lösung zu finden.")

    # Footer on page 2
    c.setFont("Helvetica", 8)
    c.line(left_margin, footer_y + 15, width - 25 * mm, footer_y + 15)
    c.drawString(left_margin, footer_y, "Bankverbindung: Sparkasse Worms-Alzey-Ried, IBAN: DE51 5535 0010 0033 7173 83, BIC: MALADE51WOR")
    c.drawString(left_margin, footer_y - 10, "Inhaber/in: Matthias Blüm")
    c.drawString(left_margin, footer_y - 20, "Gerichtsstand: Alzey | HRA-Nummer: 31710")

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
    Create a payment reminder or dunning letter PDF with multiple invoices.

    Args:
        customer_name: Name of the customer
        customer_address: Full address of the customer
        invoices: List of invoices with date, number, and amount
        reminder_level: 0 = Zahlungserinnerung, 1 = 1. Mahnung, 2 = 2. Mahnung
        salutation: Salutation for the customer (e.g., "Herr", "Frau")

    Returns:
        PDF bytes
    """
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4

    # Pharmacy info (top right)
    c.setFont("Helvetica-Bold", 11)
    c.drawString(360, height - 50, "Apotheke am Damm")
    c.setFont("Helvetica", 10)
    c.drawString(360, height - 65, "Inh. Matthias Blüm, e.K.")
    c.drawString(360, height - 80, "Am Damm 17")
    c.drawString(360, height - 95, "55232 Alzey")
    c.drawString(360, height - 110, "Tel.: 06731-548846")
    c.drawString(360, height - 125, "info@apothekeamdamm.de")

    # Return address (small, above recipient address)
    left_margin = 25 * mm
    return_address_y = height - (20 * mm)
    c.setFont("Helvetica", 8)
    c.drawString(left_margin, return_address_y, "Apotheke am Damm, Am Damm 17, 55232 Alzey")

    # Recipient address (must be between 66mm and 88mm from top - DIN 5008)
    recipient_y_start = height - (66 * mm)
    c.setFont("Helvetica", 11)

    # Parse address
    address_lines = customer_address.split('\n') if '\n' in customer_address else customer_address.split(',')
    address_lines = [line.strip() for line in address_lines if line.strip()]

    # Determine greeting
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

    # Date
    date_y = height - (106 * mm)
    today = datetime.now().strftime("%d.%m.%Y")
    c.setFont("Helvetica", 10)
    c.drawRightString(width - 25 * mm, date_y, f"Alzey, {today}")

    # Subject line
    subject_y = date_y - 20
    c.setFont("Helvetica-Bold", 12)

    # Different subject based on reminder level
    level_names = {
        0: "Zahlungserinnerung",
        1: "1. Mahnung",
        2: "2. Mahnung - LETZTE ZAHLUNGSAUFFORDERUNG"
    }

    subject_text = level_names.get(reminder_level, "Zahlungserinnerung")

    # Red color for level 2
    if reminder_level == 2:
        c.setFillColorRGB(0.52, 0.13, 0.16)  # Dark red

    c.drawString(left_margin, subject_y, subject_text)
    c.setFillColorRGB(0, 0, 0)  # Back to black

    # Salutation
    content_y = subject_y - 25
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

    # Main text based on reminder level
    content_y -= 20

    if reminder_level == 0:
        c.drawString(left_margin, content_y, "bei der Durchsicht unserer Buchhaltung ist uns aufgefallen, dass der")
        content_y -= 12
        c.drawString(left_margin, content_y, "Rechnungsbetrag für die unten aufgeführten Rechnungen noch nicht bei uns")
        content_y -= 12
        c.drawString(left_margin, content_y, "eingegangen ist. Wir bitten Sie, die offenen Beträge innerhalb von 14 Tagen")
        content_y -= 12
        c.drawString(left_margin, content_y, "auf unser Konto zu überweisen.")
    elif reminder_level == 1:
        c.drawString(left_margin, content_y, "trotz unserer Zahlungserinnerung haben wir bisher keinen Zahlungseingang")
        content_y -= 12
        c.drawString(left_margin, content_y, "für die unten aufgeführten Rechnungen feststellen können. Wir fordern Sie")
        content_y -= 12
        c.drawString(left_margin, content_y, "hiermit auf, den ausstehenden Betrag innerhalb von 10 Tagen nach Erhalt")
        content_y -= 12
        c.drawString(left_margin, content_y, "dieses Schreibens zu überweisen.")
    else:  # Level 2
        c.drawString(left_margin, content_y, "trotz mehrmaliger Zahlungsaufforderungen ist der ausstehende Rechnungsbetrag")
        content_y -= 12
        c.setFont("Helvetica-Bold", 10)
        c.drawString(left_margin, content_y, "bis heute nicht bei uns eingegangen. Dies ist unsere letzte Zahlungsaufforderung")
        content_y -= 12
        c.drawString(left_margin, content_y, "vor Einleitung rechtlicher Schritte.")
        c.setFont("Helvetica", 10)

    # Table for invoices
    content_y -= 25
    c.setFont("Helvetica-Bold", 9)

    # Table header
    col1_x = left_margin + 10  # Rechnungsnummer
    col2_x = left_margin + 120  # Datum
    col3_x = left_margin + 220  # Betrag (rechts ausgerichtet)

    c.drawString(col1_x, content_y, "Rechnungs-Nr.")
    c.drawString(col2_x, content_y, "Datum")
    c.drawRightString(col3_x, content_y, "Betrag")

    content_y -= 2
    c.line(left_margin + 10, content_y, col3_x, content_y)
    content_y -= 10

    # Table rows
    c.setFont("Helvetica", 9)
    total_amount = 0.0
    for inv in invoices:
        inv_date_str = datetime.strptime(inv['date'], '%Y-%m-%d').strftime('%d.%m.%Y')
        c.drawString(col1_x, content_y, inv['number'])
        c.drawString(col2_x, content_y, inv_date_str)
        c.drawRightString(col3_x, content_y, f"{inv['amount']:.2f} €")
        total_amount += inv['amount']
        content_y -= 12

    # Total line
    content_y -= 2
    c.line(left_margin + 10, content_y, col3_x, content_y)
    content_y -= 10
    c.setFont("Helvetica-Bold", 9)
    c.drawString(col1_x, content_y, "Gesamtbetrag:")
    c.drawRightString(col3_x, content_y, f"{total_amount:.2f} €")

    # Bank details
    content_y -= 20
    c.setFont("Helvetica", 10)
    c.drawString(left_margin, content_y, "Bitte überweisen Sie den Betrag auf folgendes Konto:")

    content_y -= 15
    c.setFont("Helvetica", 9)
    c.drawString(left_margin + 10, content_y, "Kontoinhaber: Apotheke am Damm")
    content_y -= 12
    c.drawString(left_margin + 10, content_y, "IBAN: DE51 5535 0010 0033 7173 83")
    content_y -= 12
    c.drawString(left_margin + 10, content_y, "BIC: MALADE51WOR")
    content_y -= 12
    c.drawString(left_margin + 10, content_y, "Verwendungszweck: Rechnungsnummer")

    # Additional text for level 2
    if reminder_level == 2:
        content_y -= 20
        c.setFont("Helvetica-Bold", 10)
        c.drawString(left_margin, content_y, "Sollte der Betrag nicht innerhalb von 7 Tagen eingehen, werden wir ohne")
        content_y -= 12
        c.drawString(left_margin, content_y, "weitere Ankündigung folgende Maßnahmen ergreifen:")

        content_y -= 15
        c.setFont("Helvetica", 9)
        c.drawString(left_margin + 15, content_y, "• Übergabe der Forderung an ein Inkassobüro")
        content_y -= 12
        c.drawString(left_margin + 15, content_y, "• Einleitung eines gerichtlichen Mahnverfahrens")
        content_y -= 12
        c.drawString(left_margin + 15, content_y, "• Geltendmachung von Verzugszinsen und Mahnkosten")

        content_y -= 15
        c.setFont("Helvetica", 10)
        c.drawString(left_margin, content_y, "Die dadurch entstehenden zusätzlichen Kosten gehen zu Ihren Lasten.")

    # Closing
    content_y -= 20
    c.setFont("Helvetica", 10)
    if reminder_level < 2:
        c.drawString(left_margin, content_y, "Sofern Sie bereits bezahlt haben oder Unstimmigkeiten bestehen, bitten wir um")
        content_y -= 12
        c.drawString(left_margin, content_y, "umgehende Kontaktaufnahme.")
        content_y -= 20

    c.drawString(left_margin, content_y, "Mit freundlichen Grüßen")
    content_y -= 10
    c.setFont("Helvetica-Bold", 10)
    c.drawString(left_margin, content_y, "Matthias Blüm")
    content_y -= 10
    c.drawString(left_margin, content_y, "Apotheke am Damm")

    # Footer
    footer_y = 80
    c.setFont("Helvetica", 7)
    c.line(left_margin, footer_y + 15, width - 25 * mm, footer_y + 15)
    c.drawString(left_margin, footer_y, "Apotheke am Damm | Inh. Matthias Blüm, e.K. | Am Damm 17 | 55232 Alzey")
    c.drawString(left_margin, footer_y - 9, "Handelsregister: HRA 31710, Registergericht: Amtsgericht Mainz | USt-IdNr. DE814983365")

    # Page 2: Additional information
    c.showPage()

    # Pharmacy info (top right) on page 2
    c.setFont("Helvetica-Bold", 11)
    c.drawString(360, height - 50, "Apotheke am Damm")
    c.setFont("Helvetica", 10)
    c.drawString(360, height - 65, "Inh. Matthias Blüm, e.K.")
    c.drawString(360, height - 80, "Am Damm 17")
    c.drawString(360, height - 95, "55232 Alzey")
    c.drawString(360, height - 110, "Tel.: 06731-548846")
    c.drawString(360, height - 125, "info@apothekeamdamm.de")

    # Title
    info_y = height - 150
    c.setFont("Helvetica-Bold", 12)
    c.drawString(left_margin, info_y, "Weitere Informationen und Hinweise")

    info_y -= 25
    c.setFont("Helvetica", 10)

    # Paragraph 1
    c.drawString(left_margin, info_y, "Sollten Sie den Betrag bereits überwiesen haben, betrachten Sie dieses Schreiben bitte als")
    info_y -= 12
    c.drawString(left_margin, info_y, "gegenstandslos. In diesem Fall bitten wir um Entschuldigung für die Unannehmlichkeiten.")
    info_y -= 20

    # Paragraph 2
    c.drawString(left_margin, info_y, "Falls Sie Fragen zu den Rechnungspositionen haben oder in einer finanziellen Notlage sind,")
    info_y -= 12
    c.drawString(left_margin, info_y, "bitten wir Sie, sich umgehend mit uns in Verbindung zu setzen. Wir sind gerne bereit, mit")
    info_y -= 12
    c.drawString(left_margin, info_y, "Ihnen eine Ratenzahlungsvereinbarung zu treffen.")
    info_y -= 20

    # Paragraph 3
    c.drawString(left_margin, info_y, "Bitte beachten Sie, dass bei Nichtzahlung weitere Kosten auf Sie zukommen können,")
    info_y -= 12
    c.drawString(left_margin, info_y, "einschließlich Zinsen, Anwaltskosten und Gerichtsgebühren. Diese können den")
    info_y -= 12
    c.drawString(left_margin, info_y, "ursprünglichen Rechnungsbetrag erheblich erhöhen.")
    info_y -= 20

    # Paragraph 4
    c.drawString(left_margin, info_y, "Wir möchten Sie darauf hinweisen, dass ein gerichtliches Mahnverfahren auch negative")
    info_y -= 12
    c.drawString(left_margin, info_y, "Auswirkungen auf Ihre Bonität haben kann. Dies kann zukünftige Geschäftsbeziehungen und")
    info_y -= 12
    c.drawString(left_margin, info_y, "Kreditwürdigkeitsprüfungen beeinflussen.")
    info_y -= 20

    # Paragraph 5
    c.drawString(left_margin, info_y, "Ihre Gesundheit liegt uns am Herzen, und wir möchten unsere gute Geschäftsbeziehung")
    info_y -= 12
    c.drawString(left_margin, info_y, "fortführen. Daher bitten wir Sie eindringlich, den offenen Betrag zu begleichen oder sich mit")
    info_y -= 12
    c.drawString(left_margin, info_y, "uns in Verbindung zu setzen, um eine Lösung zu finden.")

    # Footer on page 2
    c.setFont("Helvetica", 7)
    c.line(left_margin, footer_y + 15, width - 25 * mm, footer_y + 15)
    c.drawString(left_margin, footer_y, "Apotheke am Damm | Inh. Matthias Blüm, e.K. | Am Damm 17 | 55232 Alzey")
    c.drawString(left_margin, footer_y - 9, "Handelsregister: HRA 31710, Registergericht: Amtsgericht Mainz | USt-IdNr. DE814983365")

    c.save()
    buffer.seek(0)
    return buffer.getvalue()


@dataclass
class InvoiceRow:
    id: int
    invoice_number: Optional[str]
    invoice_date: str
    customer_name: str
    customer_address: str
    amount_cents: int
    status: str  # 'open' or 'paid'
    last_seen_snapshot: str  # Last snapshot where this invoice appeared
    first_seen_snapshot: str  # First snapshot where this invoice appeared
    file_path: Optional[str] = None  # Path in the latest/last snapshot
    in_collective_invoice: bool = False  # Whether this invoice is in a collective invoice

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
            reminded_and_paid AS (
                SELECT
                    r.reminder_level,
                    i.amount_cents,
                    r.created_at,
                    (SELECT snapshot_date FROM last_two_snapshots ORDER BY snapshot_date DESC LIMIT 1) as last_month,
                    (SELECT snapshot_date FROM last_two_snapshots ORDER BY snapshot_date DESC LIMIT 1 OFFSET 1) as second_last_month
                FROM reminders r
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

        # Fetch all invoices to calculate tab counts
        all_unbemahnt = fetch_invoices_with_reminders(app.config["DATABASE"], filter_reminded=False)
        # Show ALL unbemahnt invoices (including those without recommendation)
        unbemahnt_invoices = all_unbemahnt
        all_reminded = fetch_invoices_with_reminders(app.config["DATABASE"], filter_reminded=True)
        zahlungserinnerung_invoices = [inv for inv in all_reminded if inv.last_reminder_level == 0]
        mahnung_1_invoices = [inv for inv in all_reminded if inv.last_reminder_level == 1]
        mahnung_2_invoices = [inv for inv in all_reminded if inv.last_reminder_level == 2]

        # Calculate tab counts for badges
        tab_counts = {
            'unbemahnt': len(unbemahnt_invoices),
            'zahlungserinnerung': len(zahlungserinnerung_invoices),
            '1_mahnung': len(mahnung_1_invoices),
            '2_mahnung': len(mahnung_2_invoices),
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
            stats=stats
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

    @app.route("/api/customers/<customer_name>", methods=["PUT"])
    def update_customer(customer_name: str) -> Response:
        """Update customer details (salutation, email, notes, never_remind flag, and bank_debit flag)."""
        data = request.get_json()

        if not data:
            return jsonify({"success": False, "error": "Keine Daten empfangen"}), 400

        salutation = data.get("salutation", "")
        email = data.get("email", "")
        notes = data.get("notes", "")
        never_remind = 1 if data.get("never_remind", False) else 0
        bank_debit = 1 if data.get("bank_debit", False) else 0

        try:
            with sqlite3.connect(app.config["DATABASE"]) as conn:
                init_db(conn)
                # Insert or update customer details
                conn.execute(
                    """
                    INSERT INTO customer_details (customer_name, salutation, email, notes, never_remind, bank_debit, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
                    ON CONFLICT(customer_name) DO UPDATE SET
                        salutation = excluded.salutation,
                        email = excluded.email,
                        notes = excluded.notes,
                        never_remind = excluded.never_remind,
                        bank_debit = excluded.bank_debit,
                        updated_at = datetime('now')
                    """,
                    (customer_name, salutation, email, notes, never_remind, bank_debit)
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
                            VALUES (?, ?, datetime('now'))
                            ON CONFLICT(customer_name) DO UPDATE SET
                                salutation = excluded.salutation,
                                updated_at = datetime('now')
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

    @app.route("/invoices")
    def index() -> Response:
        query = request.args.get("q", "").strip()
        limit = clamp_limit(request.args.get("limit"), app.config["MAX_LIMIT"])
        grouped = request.args.get("grouped", "").lower() == "true"

        # Separate time and status filters
        time_filter = request.args.get("time", "all")  # 'all', 'current_month', or 'custom'
        status_filter = request.args.get("status", "open")  # 'all', 'open', or 'paid'
        email_filter = request.args.get("email", "all")  # 'all' or 'with_email'

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
            sort_by,
            sort_direction,
        )
        total_amount = sum(row.amount_eur for row in invoices)

        # Get latest snapshot and date range for display
        with sqlite3.connect(app.config["DATABASE"]) as conn:
            latest_snapshot_row = conn.execute(
                "SELECT MAX(snapshot_date) as latest FROM snapshots"
            ).fetchone()
            latest_snapshot = latest_snapshot_row[0] if latest_snapshot_row and latest_snapshot_row[0] else None

            # Get min and max invoice dates for custom date range helper
            date_range_row = conn.execute(
                "SELECT MIN(invoice_date) as min_date, MAX(invoice_date) as max_date FROM invoices"
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

        # Fetch LetterXpress status from database
        letterxpress_status = {}
        try:
            with sqlite3.connect(app.config["DATABASE"]) as conn:
                conn.row_factory = sqlite3.Row
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
                        # Format: Sammelrechnung_2025-11_Kundenname.pdf
                        filename = pdf_file.stem  # Remove .pdf extension
                        parts = filename.split("_", 2)  # Split into max 3 parts

                        if len(parts) >= 3:
                            customer_name = parts[2]  # Third part is customer name
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

                        collective_invoices.append({
                            "month": month,
                            "customer_name": customer_name,
                            "filename": pdf_file.name,
                            "created_at": created_at,
                            "file_size_kb": file_size_kb,
                            "relative_path": str(relative_path),
                            "letterxpress_status": lx_status
                        })

        # Group by month for better display
        grouped_by_month = defaultdict(list)
        for invoice in collective_invoices:
            grouped_by_month[invoice["month"]].append(invoice)

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
            sort_by,
            sort_direction,
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

        pdf_files = list(find_pdfs(root))
        new_count = 0
        errors = []

        with sqlite3.connect(db_path) as conn:
            init_db(conn)
            for pdf_path in pdf_files:
                try:
                    if process_pdf_file(conn, pdf_path, root):
                        new_count += 1
                except Exception as exc:
                    error_msg = f"{pdf_path.name}: {str(exc)}"
                    errors.append(error_msg)
                    logging.error("Fehler beim Verarbeiten von %s: %s", pdf_path, exc)
            conn.commit()

        return jsonify({
            "success": True,
            "new_invoices": new_count,
            "total_scanned": len(pdf_files),
            "errors": errors
        })

    @app.route("/api/scan-stream", methods=["GET"])
    def scan_new_invoices_stream() -> Response:
        """Scan the invoice directory for new PDFs with real-time progress using Server-Sent Events."""
        import json
        from flask import stream_with_context

        def generate():
            try:
                db_path = Path(app.config["DATABASE"])
                root = Path(app.config["INVOICE_ROOT"])

                if not root.exists():
                    yield f"data: {json.dumps({'type': 'error', 'message': f'Verzeichnis {root} nicht gefunden'})}\n\n"
                    return

                pdf_files = list(find_pdfs(root))
                total_pdfs = len(pdf_files)

                if total_pdfs == 0:
                    yield f"data: {json.dumps({'type': 'complete', 'success': 0, 'failed': 0, 'total': 0})}\n\n"
                    return

                # Send summary
                yield f"data: {json.dumps({'type': 'summary', 'total': total_pdfs})}\n\n"

                new_count = 0
                skipped_count = 0
                error_count = 0
                errors = []

                with sqlite3.connect(db_path) as conn:
                    init_db(conn)
                    for idx, pdf_path in enumerate(pdf_files, 1):
                        try:
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

                # Send completion message
                yield f"data: {json.dumps({'type': 'complete', 'success': new_count, 'skipped': skipped_count, 'failed': error_count, 'total': total_pdfs, 'errors': errors})}\n\n"

            except Exception as e:
                logging.error(f"Error in scan stream: {e}")
                yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"

        return Response(stream_with_context(generate()), mimetype="text/event-stream")

    @app.route("/api/print-invoices")
    def print_invoices() -> Response:
        """Combine all filtered invoice PDFs into a single PDF for printing."""
        query = request.args.get("q", "").strip()
        limit = clamp_limit(request.args.get("limit"), app.config["MAX_LIMIT"])
        time_filter = request.args.get("time", "all")
        status_filter = request.args.get("status", "open")
        email_filter = request.args.get("email", "all")
        from_month = request.args.get("from_month", "")
        to_month = request.args.get("to_month", "")

        invoices = fetch_invoices(app.config["DATABASE"], query, limit, time_filter, status_filter, from_month, to_month, email_filter)

        if not invoices:
            return jsonify({"error": "Keine Rechnungen zum Drucken gefunden"}), 404

        # Filter out invoices without file_path
        invoices_with_files = [inv for inv in invoices if inv.file_path]

        if not invoices_with_files:
            return jsonify({"error": "Keine PDF-Dateien für die ausgewählten Rechnungen gefunden"}), 404

        try:
            # Create PDF writer
            pdf_writer = PdfWriter()
            root = Path(app.config["INVOICE_ROOT"])

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
        from_month = request.args.get("from_month", "")
        to_month = request.args.get("to_month", "")

        def generate():
            try:
                invoices = fetch_invoices(app.config["DATABASE"], query, limit, time_filter, status_filter, from_month, to_month, email_filter)

                if not invoices:
                    yield f"data: {json.dumps({'type': 'error', 'message': 'Keine Rechnungen zum Versenden gefunden'})}\n\n"
                    return

                # Group invoices by customer and month
                grouped_invoices = defaultdict(list)

                for invoice in invoices:
                    if invoice.invoice_date:
                        month_year = invoice.invoice_date[:7]
                    else:
                        month_year = "unknown"
                    key = (invoice.customer_name, month_year)
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

                try:
                    ensure_smtp_connection()
                except Exception as exc:
                    logging.error(f"Unable to establish SMTP connection: {exc}")
                    yield f"data: {json.dumps({'type': 'error', 'message': 'SMTP-Verbindung konnte nicht aufgebaut werden'})}\n\n"
                    return

                # Get customer emails from database
                with sqlite3.connect(app.config["DATABASE"]) as conn:
                    conn.row_factory = sqlite3.Row
                    init_db(conn)

                    success_count = 0
                    failed_count = 0
                    processed_groups = 0
                    root = Path(app.config["INVOICE_ROOT"])

                    # Process each group
                    for (customer_name, month_year), invoice_list in grouped_invoices.items():
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
                        for invoice in invoice_list:
                            if not invoice.file_path:
                                continue
                            pdf_path = root / invoice.file_path
                            if pdf_path.exists():
                                pdf_paths.append(pdf_path)

                        if not pdf_paths:
                            error_msg = f"Keine gültigen PDF-Dateien gefunden"
                            yield f"data: {json.dumps({'type': 'error', 'customer': customer_name, 'message': error_msg})}\n\n"
                            failed_count += len(invoice_list)
                            processed_groups += 1
                            progress = int((processed_groups / total_groups) * 100)
                            yield f"data: {json.dumps({'type': 'progress', 'progress': progress, 'processed': processed_groups, 'total': total_groups})}\n\n"
                            continue

                        # Send info message
                        yield f"data: {json.dumps({'type': 'info', 'customer': customer_name, 'email': customer_email, 'count': len(pdf_paths)})}\n\n"

                        # Send email (retry once if the SMTP server disconnects)
                        send_success = send_invoices_batch_email(
                            customer_email,
                            customer_name,
                            pdf_paths,
                            month_year,
                            customer_salutation,
                            smtp_connection=ensure_smtp_connection(),
                            smtp_config=smtp_config,
                        )

                        if not send_success:
                            reset_smtp_connection()
                            try:
                                connection_for_retry = ensure_smtp_connection()
                            except Exception as exc:
                                logging.error(f"SMTP reconnect failed: {exc}")
                                reset_smtp_connection()
                                failed_count += len(invoice_list)
                                yield f"data: {json.dumps({'type': 'error', 'customer': customer_name, 'message': 'E-Mail-Versand fehlgeschlagen: SMTP-Verbindung getrennt'})}\n\n"
                                return

                            send_success = send_invoices_batch_email(
                                customer_email,
                                customer_name,
                                pdf_paths,
                                month_year,
                                customer_salutation,
                                smtp_connection=connection_for_retry,
                                smtp_config=smtp_config,
                            )

                        if send_success:
                            success_count += len(pdf_paths)
                            yield f"data: {json.dumps({'type': 'success', 'customer': customer_name, 'email': customer_email, 'count': len(pdf_paths)})}\n\n"
                        else:
                            failed_count += len(invoice_list)
                            yield f"data: {json.dumps({'type': 'error', 'customer': customer_name, 'message': 'E-Mail-Versand fehlgeschlagen (möglicherweise Rate Limit des SMTP-Servers)'})}\n\n"

                        processed_groups += 1
                        progress = int((processed_groups / total_groups) * 100)
                        yield f"data: {json.dumps({'type': 'progress', 'progress': progress, 'processed': processed_groups, 'total': total_groups})}\n\n"

                        # Add delay between emails to avoid rate limiting (2 seconds)
                        if processed_groups < total_groups:
                            time.sleep(2)

                # Close SMTP connection and send completion message
                reset_smtp_connection()
                yield f"data: {json.dumps({'type': 'complete', 'success': success_count, 'failed': failed_count, 'total': total_invoices})}\n\n"

            except Exception as e:
                logging.error(f"Error in email stream: {e}")
                yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"

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

                    inv_id, inv_number, inv_date, cust_name, cust_address, amount_cents, file_path = row

                    # Group by customer name and reminder level
                    key = (cust_name, cust_address, reminder_level)
                    grouped[key].append({
                        'id': inv_id,
                        'number': inv_number or f"#{inv_id}",
                        'date': inv_date,
                        'amount': amount_cents / 100.0,
                        'file_path': file_path
                    })

                # Generate PDFs for each group
                root = Path(app.config["INVOICE_ROOT"])

                for (customer_name, customer_address, reminder_level), invoice_list in grouped.items():
                    # Get salutation for customer
                    salutation = determine_salutation_for_customer(customer_name)

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
            # Get current month for folder name
            current_month = datetime.now().strftime("%Y-%m")
            output_folder = BASE_DIR / "Sammelrechnungen" / current_month
            output_folder.mkdir(parents=True, exist_ok=True)

            # Get filters from request (for selecting which customers to process)
            query = request.args.get("q", "").strip()
            limit = clamp_limit(request.args.get("limit"), app.config["MAX_LIMIT"])
            time_filter = request.args.get("time", "all")
            status_filter = "open"  # Only open invoices
            from_month = request.args.get("from_month", "")
            to_month = request.args.get("to_month", "")
            email_filter = request.args.get("email", "all")

            # First, get invoices based on user filters to determine which customers to process
            filtered_invoices = fetch_invoices(
                app.config["DATABASE"],
                query,
                limit,
                time_filter,
                status_filter,
                from_month,
                to_month,
                email_filter
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
                "all"  # all email statuses
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
                root = Path(app.config["INVOICE_ROOT"])

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

                    # Get customer salutation and address
                    customer_row = conn.execute(
                        "SELECT salutation FROM customer_details WHERE customer_name = ?",
                        (customer_name,)
                    ).fetchone()
                    salutation = customer_row["salutation"] if customer_row else None

                    # Use the address from the first invoice
                    customer_address = current_month_invoices[0].customer_address if current_month_invoices else customer_invoice_list[0].customer_address

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
                        customer_name=customer_name,
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

                    # Save combined PDF
                    # Sanitize filename
                    safe_customer_name = "".join(
                        c for c in customer_name if c.isalnum() or c in (' ', '-', '_')
                    ).strip()
                    filename = f"Sammelrechnung_{current_month}_{safe_customer_name}.pdf"
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
                                (inv.id, filename, current_month)
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
    time_filter: str = "all",
    status_filter: str = "all",
    from_month: str = "",
    to_month: str = "",
    email_filter: str = "all",
    sort_by: str = "date",
    sort_direction: str = "desc",
) -> List[InvoiceRow]:
    """
    Fetch invoices with their payment status based on snapshot tracking.

    Time filter:
    - 'all': All time periods
    - 'current_month': Only invoices from the latest import/snapshot
    - 'custom': Custom date range using from_month and to_month

    Status filter:
    - 'all': All statuses
    - 'open': Invoice appears in the latest snapshot
    - 'paid': Invoice doesn't appear in latest snapshot but appeared in earlier ones

    Custom date range:
    - from_month: Start month in YYYY-MM format
    - to_month: End month in YYYY-MM format
    """
    with sqlite3.connect(database_path) as conn:
        conn.row_factory = sqlite3.Row

        # Get the latest snapshot date
        latest_snapshot_row = conn.execute(
            "SELECT MAX(snapshot_date) as latest FROM snapshots"
        ).fetchone()

        if not latest_snapshot_row or not latest_snapshot_row["latest"]:
            # No snapshots yet
            return []

        latest_snapshot = latest_snapshot_row["latest"]

        # Build the main query
        sql = """
            WITH invoice_status AS (
                SELECT
                    i.id,
                    i.invoice_number,
                    i.invoice_date,
                    i.customer_name,
                    i.customer_address,
                    i.amount_cents,
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
            )
            SELECT
                ist.*,
                (
                    SELECT isnap.file_path
                    FROM invoice_snapshots isnap
                    JOIN snapshots s_sub ON isnap.snapshot_id = s_sub.id
                    WHERE isnap.invoice_id = ist.id
                      AND s_sub.snapshot_date = ist.last_seen_snapshot
                    ORDER BY s_sub.snapshot_date DESC
                    LIMIT 1
                ) as file_path,
                CASE
                    WHEN EXISTS (
                        SELECT 1 FROM collective_invoice_items cii
                        WHERE cii.invoice_id = ist.id
                    ) THEN 1
                    ELSE 0
                END as in_collective_invoice
            FROM invoice_status ist
            LEFT JOIN customer_details cd ON ist.customer_name = cd.customer_name
            WHERE 1=1
        """

        params = [latest_snapshot]

        # Apply search filter
        if query:
            sql += """
                AND (ist.customer_name LIKE ?
                     OR ist.invoice_number LIKE ?
                     OR ist.customer_address LIKE ?)
            """
            pattern = f"%{query}%"
            params.extend([pattern, pattern, pattern])

        # Apply time filter
        if time_filter == "current_month":
            # Filter by invoice_date for the month of the newest invoice
            # Get the latest invoice date
            latest_invoice_row = conn.execute(
                "SELECT MAX(invoice_date) as latest FROM invoices"
            ).fetchone()

            if latest_invoice_row and latest_invoice_row["latest"]:
                latest_date = latest_invoice_row["latest"]
                # Extract year and month from latest date (YYYY-MM-DD format)
                year_month = latest_date[:7]  # YYYY-MM
                year, month = map(int, year_month.split('-'))

                current_month_start = f"{year}-{month:02d}-01"
                if month == 12:
                    next_month_start = f"{year + 1}-01-01"
                else:
                    next_month_start = f"{year}-{month + 1:02d}-01"

                sql += " AND ist.invoice_date >= ? AND ist.invoice_date < ?"
                params.append(current_month_start)
                params.append(next_month_start)
        elif time_filter == "custom" and (from_month or to_month):
            # Custom date range filter based on invoice_date
            if from_month and to_month:
                sql += " AND ist.invoice_date >= ? AND ist.invoice_date <= ?"
                params.append(f"{from_month}-01")  # First day of from_month
                # Calculate last day of to_month
                year, month = map(int, to_month.split('-'))
                if month == 12:
                    next_month = f"{year+1}-01-01"
                else:
                    next_month = f"{year}-{month+1:02d}-01"
                params.append(next_month)  # Use next month's first day and < comparison
                # Adjust SQL to use < instead of <=
                sql = sql.replace(" AND ist.invoice_date <= ?", " AND ist.invoice_date < ?")
            elif from_month:
                sql += " AND ist.invoice_date >= ?"
                params.append(f"{from_month}-01")
            elif to_month:
                # Calculate first day of month after to_month
                year, month = map(int, to_month.split('-'))
                if month == 12:
                    next_month = f"{year+1}-01-01"
                else:
                    next_month = f"{year}-{month+1:02d}-01"
                sql += " AND ist.invoice_date < ?"
                params.append(next_month)

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

        sort_key, sort_dir = normalize_sort_params(sort_by, sort_direction)
        order_expression = SORT_COLUMN_MAP[sort_key]

        sql += f" ORDER BY {order_expression} {sort_dir.upper()}, ist.id DESC LIMIT ?"
        params.append(limit)

        rows = conn.execute(sql, params).fetchall()

    return [row_from_sql(row) for row in rows]


def row_from_sql(row: sqlite3.Row) -> InvoiceRow:
    return InvoiceRow(
        id=row["id"],
        invoice_number=row["invoice_number"],
        invoice_date=row["invoice_date"],
        customer_name=row["customer_name"],
        customer_address=row["customer_address"],
        amount_cents=row["amount_cents"],
        status=row["status"],
        last_seen_snapshot=row["last_seen_snapshot"],
        first_seen_snapshot=row["first_seen_snapshot"],
        file_path=row["file_path"] if "file_path" in row.keys() else None,
        in_collective_invoice=bool(row["in_collective_invoice"]) if "in_collective_invoice" in row.keys() else False,
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
    """
    with sqlite3.connect(database_path) as conn:
        conn.row_factory = sqlite3.Row
        init_db(conn)

        # Get all unique customers from invoices with their details
        sql = """
            SELECT
                i.customer_name,
                i.customer_address,
                cd.salutation,
                cd.email,
                cd.notes,
                cd.never_remind,
                cd.bank_debit,
                COUNT(DISTINCT i.id) as invoice_count,
                SUM(i.amount_cents) as total_amount_cents
            FROM invoices i
            LEFT JOIN customer_details cd ON i.customer_name = cd.customer_name
            GROUP BY i.customer_name, i.customer_address, cd.salutation, cd.email, cd.notes, cd.never_remind, cd.bank_debit
            ORDER BY i.customer_name
        """

        rows = conn.execute(sql).fetchall()

    customers = []
    for row in rows:
        customers.append({
            "customer_name": row["customer_name"],
            "customer_address": row["customer_address"],
            "salutation": row["salutation"] or "",
            "email": row["email"] or "",
            "notes": row["notes"] or "",
            "never_remind": row["never_remind"] or 0,
            "bank_debit": row["bank_debit"] or 0,
            "invoice_count": row["invoice_count"],
            "total_amount_eur": row["total_amount_cents"] / 100.0 if row["total_amount_cents"] else 0.0,
        })

    return customers


def fetch_invoices_with_reminders(database_path: str, filter_reminded: Optional[bool] = None) -> List[InvoiceWithReminder]:
    """
    Fetch open invoices with their reminder information.

    Args:
        database_path: Path to the database
        filter_reminded: If True, only show invoices with reminders. If False, only show invoices without reminders.
                        If None, show all open invoices.
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
                    i.amount_cents,
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
                        WHEN isnap.file_path IS NOT NULL THEN 'Rechnungen/' || isnap.file_path
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
                COALESCE(rgc.invoices_in_group, 1) as invoices_in_group
            FROM invoice_status ist
            LEFT JOIN invoice_files if ON ist.id = if.id
            LEFT JOIN last_reminder lr ON ist.id = lr.invoice_id
            LEFT JOIN reminder_group_counts rgc ON lr.pdf_path = rgc.pdf_path
            WHERE 1=1
        """

        params = [latest_snapshot]

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

        invoice = InvoiceWithReminder(
            id=row["id"],
            invoice_number=row["invoice_number"],
            invoice_date=row["invoice_date"],
            customer_name=row["customer_name"],
            customer_address=row["customer_address"],
            amount_cents=row["amount_cents"],
            status=row["status"],
            last_seen_snapshot=row["last_seen_snapshot"],
            first_seen_snapshot=row["first_seen_snapshot"],
            file_path=row["file_path"] if "file_path" in row.keys() else None,
            months_open=months_open,
            recommended_level=recommended_level,
            last_reminder_level=row["last_reminder_level"],
            last_reminder_date=row["last_reminder_date"],
            letterexpress_status=row["letterexpress_status"],
            has_reminders=bool(row["has_reminders"]),
            reminder_pdf_path=row["reminder_pdf_path"] if "reminder_pdf_path" in row.keys() else None,
            invoices_in_group=row["invoices_in_group"] if "invoices_in_group" in row.keys() else 1,
        )
        result.append(invoice)

    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Start the invoice web dashboard.")
    parser.add_argument("--host", default="127.0.0.1", help="Bind address (default: %(default)s)")
    parser.add_argument("--port", type=int, default=5000, help="Port (default: %(default)s)")
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
