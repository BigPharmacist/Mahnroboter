"""PDF document generation: cover letters, reminders, SEPA mandates,
invoice history and the e-mail consent form, plus their drawing helpers.

Extracted verbatim from web_app.py. No behaviour change. Hard-coded company
details (address, bank, colours) intentionally moved along unchanged.
"""

from __future__ import annotations

import io
import sqlite3
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib.utils import ImageReader
from reportlab.platypus import Paragraph
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_JUSTIFY


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
        c.setFont("Helvetica", 6)
        c.drawString(col3_x, y, "IBAN: DE51 5535 0010 0033 7173 83")

        y -= 3*mm
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
    salutation: Optional[str] = None,
    include_prescription_notice: bool = False
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
    c.drawString(left_margin, content_y, "Wichtig nur für gesetzlich Versicherte mit Zuzahlungsbefreiung:")

    content_y -= 15
    text_width = right_margin - left_margin
    c.setFillColor(black)
    text = ("Trotz Ihrer Befreiung von der Rezeptgebühr ist dieser Rechnungsbetrag fällig, da Ihr Arzt das Rezept "
            "als \"gebührenpflichtig\" gekennzeichnet hat. Sie erhalten den Betrag von Ihrer Krankenkasse erstattet, "
            "wenn Sie dort diese Rechnung zusammen mit dem Zahlungsnachweis einreichen. Bitte senden Sie uns "
            "auch eine Kopie Ihres Befreiungsausweises zu. Bei Fragen helfen wir gerne weiter.")
    content_y = draw_justified_paragraph(c, text, left_margin, content_y, text_width, font_size=9)

    # === HINWEIS PRIVATVERSICHERTE (nur wenn Rezepte beigefuegt sind) ===
    if include_prescription_notice:
        content_y -= 25
        c.setFillColor(primary_color)
        c.setFont("Helvetica-Bold", 11)
        c.drawString(left_margin, content_y, "Information für Privatversicherte")

        content_y -= 15
        c.setFillColor(black)
        rx_text = ("Damit Sie alle Unterlagen sofort zur Hand haben, fügen wir Ihrer Abrechnung ab sofort "
                   "einen Scan Ihrer Originalrezepte auf den folgenden Seiten bei. Erfahrungsgemäß erkennen "
                   "die meisten privaten Krankenversicherungen diese Kopien für die Erstattung an. Möchten Sie "
                   "die Originale dennoch per Post erhalten, genügt eine kurze Nachricht an uns – wir schicken "
                   "sie Ihnen dann umgehend zu.")
        content_y = draw_justified_paragraph(c, rx_text, left_margin, content_y, text_width, font_size=9)

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
    c.drawString(left_margin, info_y, "Wichtig nur für gesetzlich Versicherte mit Zuzahlungsbefreiung:")

    info_y -= 15
    text_width = right_margin - left_margin
    c.setFillColor(black)
    text = ("Trotz Ihrer Befreiung von der Rezeptgebühr ist dieser Rechnungsbetrag fällig, da Ihr Arzt das Rezept "
            "als \"gebührenpflichtig\" gekennzeichnet hat. Sie erhalten den Betrag von Ihrer Krankenkasse erstattet, "
            "wenn Sie dort diese Rechnung zusammen mit dem Zahlungsnachweis einreichen. Bitte senden Sie uns "
            "auch eine Kopie Ihres Befreiungsausweises zu. Bei Fragen helfen wir gerne weiter.")
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


def create_invoice_history_pdf(
    customer_name: str,
    customer_street: str,
    customer_city: str,
    invoice_number: str,
    invoice_date: str,
    amount_eur: float,
    events: list
) -> bytes:
    """
    Create a printable PDF of the invoice history/timeline.

    Args:
        customer_name: Name of the customer
        customer_street: Street address of the customer
        customer_city: City (PLZ + Ort) of the customer
        invoice_number: Invoice number
        invoice_date: Invoice date string
        amount_eur: Invoice amount in EUR
        events: List of event dicts with event_type, timestamp, metadata

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

    # Event type translations and explanations
    event_translations = {
        'IMPORT': ('Import', 'Rechnung wurde aus Importdatei eingelesen'),
        'EMAIL_SENT': ('E-Mail versendet', 'Rechnung wurde per E-Mail an Kunden gesendet'),
        'COLLECTIVE_INVOICE_CREATED': ('Sammelrechnung erstellt', 'Rechnung wurde in Sammelrechnung aufgenommen'),
        'COLLECTIVE_INVOICE_SENT': ('Sammelrechnung versendet', 'Sammelrechnung wurde an Versanddienstleister uebertragen'),
        'REMINDER_CREATED': ('Mahnung erstellt', 'Mahnschreiben wurde als PDF erstellt'),
        'REMINDER_SENT': ('Mahnung versendet', 'Mahnschreiben wurde an Versanddienstleister uebertragen'),
        'MARKED_UNCOLLECTIBLE': ('Als uneinbringbar markiert', 'Rechnung wurde als uneinbringbar gekennzeichnet'),
        'UNMARKED_UNCOLLECTIBLE': ('Uneinbringbar-Status aufgehoben', 'Uneinbringbar-Markierung wurde entfernt')
    }

    reminder_level_names = {
        0: 'Zahlungserinnerung',
        1: '1. Mahnung',
        2: '2. Mahnung'
    }

    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4

    # Farben
    primary_color = HexColor("#123C69")
    black = HexColor("#000000")
    gray = HexColor("#666666")
    light_gray = HexColor("#f5f5f5")
    green = HexColor("#4CAF50")

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

    # ===== UEBERSCHRIFT =====
    c.setFillColor(primary_color)
    c.setFont("Helvetica-Bold", 14)
    c.drawCentredString(width/2, y_pos, "Rechnungs-Verlauf")

    y_pos -= 15*mm

    # ===== RECHNUNGS-INFO BOX =====
    box_height = 32*mm
    c.setFillColor(light_gray)
    c.rect(20*mm, y_pos - box_height + 5*mm, 170*mm, box_height, stroke=0, fill=1)

    info_y = y_pos
    c.setFillColor(black)

    # Zeile 1: Rechnungsnummer (prominent)
    c.setFont("Helvetica-Bold", 11)
    c.drawString(25*mm, info_y, f"Rechnungsnr.: {invoice_number or '-'}")

    # Rechte Spalte: Datum und Betrag
    c.setFont("Helvetica-Bold", 9)
    c.drawString(130*mm, info_y, "Datum:")
    c.drawString(130*mm, info_y - 5*mm, "Betrag:")
    c.setFont("Helvetica", 9)
    c.drawString(150*mm, info_y, invoice_date or "-")
    c.drawString(150*mm, info_y - 5*mm, f"{amount_eur:.2f} EUR")

    # Zeile 2-4: Rechnungsempfaenger mit Anschrift
    c.setFont("Helvetica-Bold", 9)
    c.drawString(25*mm, info_y - 8*mm, "Rechnungsempfaenger:")
    c.setFont("Helvetica", 9)
    c.drawString(25*mm, info_y - 13*mm, customer_name or "-")
    if customer_street:
        c.drawString(25*mm, info_y - 18*mm, customer_street)
    if customer_city:
        c.drawString(25*mm, info_y - 23*mm, customer_city)

    y_pos -= box_height + 10*mm

    # ===== VERLAUF-UEBERSCHRIFT =====
    c.setFillColor(primary_color)
    c.setFont("Helvetica-Bold", 11)
    c.drawString(20*mm, y_pos, "Chronologischer Verlauf")

    y_pos -= 10*mm

    # ===== TIMELINE (kompaktes, modernes Design) =====
    if not events:
        c.setFillColor(gray)
        c.setFont("Helvetica-Oblique", 9)
        c.drawString(25*mm, y_pos, "Noch keine Ereignisse fuer diese Rechnung.")
    else:
        # Sortiere Events chronologisch (aelteste zuerst fuer Druck)
        sorted_events = sorted(events, key=lambda e: e.get('timestamp', ''))

        # Timeline-Konstanten
        dot_x = 22*mm
        dot_radius = 2*mm
        content_x = 28*mm
        line_height = 12*mm  # Kompakter Abstand zwischen Events

        for i, event in enumerate(sorted_events):
            event_type = event.get('event_type', '')
            timestamp = event.get('timestamp', '')
            metadata = event.get('metadata', {})

            # Format timestamp
            try:
                from datetime import datetime
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                formatted_time = dt.strftime('%d.%m.%Y, %H:%M')
            except:
                formatted_time = timestamp

            # Get translation and explanation
            translation, explanation = event_translations.get(event_type, (event_type, ''))

            # Check if we need a new page
            if y_pos < 40*mm:
                c.showPage()
                y_pos = height - 25*mm
                c.setFillColor(primary_color)
                c.setFont("Helvetica-Bold", 12)
                c.drawString(20*mm, y_pos, "Rechnungs-Verlauf (Fortsetzung)")
                y_pos -= 12*mm

            # Draw timeline line FIRST (unter dem Dot, für saubere Optik)
            if i < len(sorted_events) - 1:
                c.setStrokeColor(HexColor("#e0e0e0"))
                c.setLineWidth(1.5)
                c.line(dot_x, y_pos - dot_radius - 1*mm, dot_x, y_pos - line_height + dot_radius + 1*mm)

            # Draw timeline dot (kleiner, mit Outline für modernen Look)
            c.setStrokeColor(HexColor("#3d8c40"))
            c.setFillColor(green)
            c.setLineWidth(1.5)
            c.circle(dot_x, y_pos, dot_radius, stroke=1, fill=1)

            # Erste Zeile: Event-Name + Timestamp (horizontal, kompakt)
            c.setFillColor(black)
            c.setFont("Helvetica-Bold", 9)
            c.drawString(content_x, y_pos + 1*mm, translation)

            # Timestamp direkt nach dem Event-Namen
            name_width = c.stringWidth(translation, "Helvetica-Bold", 9)
            c.setFillColor(HexColor("#888888"))
            c.setFont("Helvetica", 8)
            c.drawString(content_x + name_width + 3*mm, y_pos + 1*mm, f"({formatted_time})")

            # Metadata details als zweite Zeile (falls vorhanden)
            details = []
            if metadata.get('email'):
                details.append(f"E-Mail: {metadata['email']}")
            if metadata.get('letterxpress_job_id'):
                details.append(f"LetterXpress Job: #{metadata['letterxpress_job_id']}")
            if metadata.get('price') is not None:
                details.append(f"Kosten: {metadata['price']:.2f} EUR")
            if metadata.get('reminder_level') is not None:
                level_name = reminder_level_names.get(metadata['reminder_level'], str(metadata['reminder_level']))
                details.append(f"Stufe: {level_name}")

            # Dateiname separat (kann lang sein)
            filename = metadata.get('filename')

            extra_lines = 0
            if details:
                c.setFillColor(HexColor("#666666"))
                c.setFont("Helvetica", 7)
                c.drawString(content_x, y_pos - 4*mm, " · ".join(details))
                extra_lines += 1

            if filename:
                c.setFillColor(HexColor("#666666"))
                c.setFont("Helvetica", 7)
                c.drawString(content_x, y_pos - 4*mm - (extra_lines * 3.5*mm), f"Datei: {filename}")
                extra_lines += 1

            y_pos -= line_height + (extra_lines - 1) * 3.5*mm if extra_lines > 1 else line_height

    # ===== FUSSBEREICH (feste Position am unteren Rand) =====
    footer_y = 15*mm

    # Trennlinie
    c.setStrokeColor(HexColor("#cccccc"))
    c.setDash(2, 2)
    c.line(20*mm, footer_y + 8*mm, width - 20*mm, footer_y + 8*mm)
    c.setDash()
    c.setStrokeColor(black)

    # Fusszeile
    c.setFillColor(primary_color)
    c.setFont("Helvetica-Bold", 9)
    c.drawString(20*mm, footer_y, APOTHEKE_NAME)

    c.setFillColor(black)
    c.setFont("Helvetica", 8)
    footer_text = f"{APOTHEKE_STRASSE}, {APOTHEKE_PLZ_ORT}  |  Tel: {APOTHEKE_TELEFON}  |  {APOTHEKE_EMAIL}"
    c.drawString(65*mm, footer_y, footer_text)

    # Druckdatum rechts unten
    c.setFillColor(gray)
    c.setFont("Helvetica", 7)
    from datetime import datetime
    c.drawRightString(190*mm, footer_y - 5*mm, f"Erstellt am {datetime.now().strftime('%d.%m.%Y, %H:%M')}")

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
