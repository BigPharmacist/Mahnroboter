"""SMTP/IMAP e-mail sending and PDF attachment helpers.

Extracted verbatim from web_app.py. No behaviour change — only the location of
these functions moved. The proactive per-session reconnect logic lives in the
calling route in web_app.py.
"""

from __future__ import annotations

import imaplib
import logging
import smtplib
import time
import unicodedata
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import encode_rfc2231, formatdate
from pathlib import Path
from typing import List, Optional

from config import (
    ASCII_FALLBACK_MAP,
    IMAPConfig,
    SMTPConfig,
    load_imap_config,
    load_smtp_config,
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


def save_email_to_sent_folder(msg: MIMEMultipart, imap_config: Optional[IMAPConfig] = None) -> bool:
    """
    Save a sent email to the IMAP 'Sent' folder.

    Args:
        msg: The email message to save
        imap_config: IMAP configuration (optional, will load from env if not provided)

    Returns:
        True if successful, False otherwise
    """
    try:
        config = imap_config or load_imap_config()

        # Connect to IMAP server
        imap = imaplib.IMAP4_SSL(config.server, config.port)
        imap.login(config.user, config.password)

        # Add Date header if not present
        if 'Date' not in msg:
            msg['Date'] = formatdate(localtime=True)

        # Convert message to bytes
        email_bytes = msg.as_bytes()

        # Try common "Sent" folder names
        sent_folder_names = ['Sent', 'INBOX.Sent', 'Gesendet', 'INBOX.Gesendet', 'Sent Items']

        # List all folders to find the correct Sent folder
        status, folders = imap.list()
        if status == 'OK':
            folder_list = [f.decode().split('"')[-2] for f in folders if f]
            logging.debug(f"Available IMAP folders: {folder_list}")

        sent_folder = None

        # First, try to find any folder containing 'sent' or 'gesendet' in the folder list
        for folder in folder_list:
            if 'sent' in folder.lower() or 'gesendet' in folder.lower():
                try:
                    # Quote folder name if it contains spaces
                    folder_to_select = f'"{folder}"' if ' ' in folder else folder
                    status, _ = imap.select(folder_to_select)
                    if status == 'OK':
                        sent_folder = folder
                        logging.info(f"Found Sent folder: {sent_folder}")
                        break
                except:
                    continue

        # Fallback: try exact names
        if not sent_folder:
            for name in sent_folder_names:
                try:
                    folder_to_select = f'"{name}"' if ' ' in name else name
                    status, _ = imap.select(folder_to_select)
                    if status == 'OK':
                        sent_folder = name
                        break
                except:
                    continue

        if not sent_folder:
            logging.error("Could not find 'Sent' folder on IMAP server")
            imap.logout()
            return False

        # Append the message to the Sent folder
        # Quote folder name if it contains spaces
        folder_to_append = f'"{sent_folder}"' if ' ' in sent_folder else sent_folder
        imap.append(folder_to_append, '\\Seen', imaplib.Time2Internaldate(time.time()), email_bytes)
        imap.logout()

        logging.info(f"Email saved to IMAP folder: {sent_folder}")
        return True

    except Exception as e:
        logging.error(f"Failed to save email to IMAP Sent folder: {e}")
        return False


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

            # Save email to IMAP Sent folder
            try:
                save_email_to_sent_folder(msg)
            except Exception as imap_error:
                logging.warning(f"Failed to save email to IMAP Sent folder: {imap_error}")
                # Don't fail the whole operation if IMAP save fails
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
    prescription_count: int = 0,
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

        # Hinweis zu beigefuegten Rezept-Scans (nur wenn Rezepte angehaengt sind)
        prescription_notice = ""
        if prescription_count and prescription_count > 0:
            prescription_notice = (
                "\nInformation für Privatversicherte\n"
                "Damit Sie alle Unterlagen sofort zur Hand haben, fügen wir Ihrer Abrechnung "
                "ab sofort einen Scan Ihrer Originalrezepte im Anhang bei. Erfahrungsgemäß "
                "erkennen die meisten privaten Krankenversicherungen diese Kopien für die "
                "Erstattung an. Möchten Sie die Originale dennoch per Post erhalten, genügt "
                "eine kurze Nachricht an uns – wir schicken sie Ihnen dann umgehend zu.\n"
            )

        email_body = f"""{greeting},

{invoice_text}{invoice_details}{other_open_details}{prescription_notice}
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

            # Save email to IMAP Sent folder
            try:
                save_email_to_sent_folder(msg)
            except Exception as imap_error:
                logging.warning(f"Failed to save email to IMAP Sent folder: {imap_error}")
                # Don't fail the whole operation if IMAP save fails
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
