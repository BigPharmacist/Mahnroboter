"""
Modernisierte PDF-Erstellungsfunktionen mit neuem Design
Basiert auf dem Stil des E-Mail-Einwilligungsformulars
"""

import io
from typing import List, Dict, Optional
from datetime import datetime
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas
from reportlab.lib.colors import HexColor
from reportlab.platypus import Paragraph
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_JUSTIFY


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


def draw_justified_paragraph(c, text, x, y, width, font_size=10, font_name='Helvetica'):
    """Draw a justified paragraph at given position."""
    styles = getSampleStyleSheet()
    justified_style = ParagraphStyle(
        'Justified',
        parent=styles['BodyText'],
        fontSize=font_size,
        fontName=font_name,
        alignment=TA_JUSTIFY,
        leading=font_size * 1.2
    )
    p = Paragraph(text, justified_style)
    w, h = p.wrap(width, 1000)
    p.drawOn(c, x, y - h)
    return y - h


def create_cover_letter_pdf_new(
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


def create_reminder_pdf_new(
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

    c.save()
    buffer.seek(0)
    return buffer.getvalue()


# Test
if __name__ == "__main__":
    customer_name = "Müller, Anna"
    customer_address = "Musterstraße 123, 55232 Alzey"
    salutation = "Frau"

    # === SAMMELRECHNUNG ===
    current_month_invoices = [
        {'date': '2025-01-05', 'number': 'RE-2025-001', 'amount': 45.80},
        {'date': '2025-01-12', 'number': 'RE-2025-015', 'amount': 28.90},
        {'date': '2025-01-20', 'number': 'RE-2025-032', 'amount': 62.50}
    ]

    older_open_invoices = [
        {'date': '2024-12-10', 'number': 'RE-2024-987', 'amount': 35.20}
    ]

    pdf_bytes = create_cover_letter_pdf_new(customer_name, customer_address, current_month_invoices, older_open_invoices, salutation)

    with open("Sammelrechnung_NEU.pdf", "wb") as f:
        f.write(pdf_bytes)

    print("✅ Sammelrechnung_NEU.pdf erstellt!")

    # === ZAHLUNGSERINNERUNG ===
    invoices = [
        {'date': '2024-12-15', 'number': 'RE-2024-998', 'amount': 78.40}
    ]

    pdf_bytes = create_reminder_pdf_new(customer_name, customer_address, invoices, 0, salutation)
    with open("Zahlungserinnerung_NEU.pdf", "wb") as f:
        f.write(pdf_bytes)
    print("✅ Zahlungserinnerung_NEU.pdf erstellt!")

    # === 1. MAHNUNG ===
    pdf_bytes = create_reminder_pdf_new(customer_name, customer_address, invoices, 1, salutation)
    with open("1_Mahnung_NEU.pdf", "wb") as f:
        f.write(pdf_bytes)
    print("✅ 1_Mahnung_NEU.pdf erstellt!")

    # === 2. MAHNUNG ===
    pdf_bytes = create_reminder_pdf_new(customer_name, customer_address, invoices, 2, salutation)
    with open("2_Mahnung_NEU.pdf", "wb") as f:
        f.write(pdf_bytes)
    print("✅ 2_Mahnung_NEU.pdf erstellt!")
