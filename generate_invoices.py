#!/usr/bin/env python3
"""
Generiert realistisch wirkende Apothekenrechnungen als PDF-Dateien, die sich eng
am Layout der gelieferten Vorlage orientieren. Für jeden Monat 2024 entsteht ein
Unterordner in `Rechnungen_Test`, gefüllt mit 20-50 PDFs, in denen jede Person
mit Nachnamen Blüm höchstens eine Rechnung pro Monat erhält, jedoch über das
Jahr verteilt mindestens zwei Rechnungen bekommt. Texte enthalten die nötigen
Umlaute (z. B. Blüm, Käfiggasse, €).
"""

from __future__ import annotations

import calendar
import random
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List

BASE_DIR = Path(__file__).resolve().parent
TARGET_ROOT = BASE_DIR / "Rechnungen_Test"

PAGE_HEIGHT_MM = 297.0
MM_TO_PT = 72 / 25.4
PAGE_WIDTH_PT = 595
PAGE_HEIGHT_PT = 842  # approx for DIN A4


def mm_to_pt(mm: float) -> float:
    return mm * MM_TO_PT


def y_from_top(mm: float) -> float:
    return PAGE_HEIGHT_PT - mm_to_pt(mm)


MONTHS = [
    ("Jan", "Januar"),
    ("Feb", "Februar"),
    ("Mrz", "März"),
    ("Apr", "April"),
    ("Mai", "Mai"),
    ("Jun", "Juni"),
    ("Jul", "Juli"),
    ("Aug", "August"),
    ("Sep", "September"),
    ("Okt", "Oktober"),
    ("Nov", "November"),
    ("Dez", "Dezember"),
]

FIRST_NAMES = [
    "Anton",
    "Ludwig",
    "Anna",
    "Mara",
    "Felix",
    "Klara",
    "Jonas",
    "Larissa",
    "Paul",
    "Helene",
    "Lea",
    "Emil",
    "Oskar",
    "Nina",
    "Franz",
    "Ida",
    "Karl",
    "Sophie",
    "Maximilian",
    "Luisa",
    "Leon",
    "Greta",
    "Moritz",
    "Eva",
    "Jakob",
    "Marlene",
    "Sebastian",
    "Theresa",
    "Niklas",
    "Paula",
    "Matthias",
    "Johanna",
    "Simon",
    "Maja",
    "David",
    "Clara",
    "Henrik",
    "Lina",
    "Florian",
    "Miriam",
    "Julian",
    "Jule",
    "Hannah",
    "Tobias",
    "Carla",
    "Benedikt",
    "Sarah",
    "Konstantin",
    "Elisa",
    "Finn",
    "Martha",
    "Philipp",
    "Zoe",
    "Bastian",
    "Mila",
    "Hannes",
    "Emily",
    "Rafael",
    "Victoria",
]

FEMALE_NAMES = {
    "Anna",
    "Mara",
    "Klara",
    "Larissa",
    "Helene",
    "Lea",
    "Nina",
    "Ida",
    "Sophie",
    "Luisa",
    "Greta",
    "Eva",
    "Marlene",
    "Theresa",
    "Paula",
    "Johanna",
    "Maja",
    "Clara",
    "Lina",
    "Miriam",
    "Jule",
    "Hannah",
    "Carla",
    "Sarah",
    "Elisa",
    "Martha",
    "Zoe",
    "Mila",
    "Emily",
    "Victoria",
}

PACK_SIZES = [
    "N1",
    "N2",
    "N3",
    "10 St",
    "20 St",
    "50 St",
    "100 ml",
    "6 St",
    "4 St",
    "6x3,2 ml",
]


@dataclass
class Product:
    name: str
    base_price: float
    category: str  # "Medikament" oder "Apothekenartikel"
    vat_rate: int


MEDICATIONS: List[Product] = [
    Product("SILAPO 10.000 I.E./1,0 ml Inj.-Lösung", 460.99, "Medikament", 19),
    Product("ARANESP 130 µg Injektionslösung", 1167.49, "Medikament", 19),
    Product("FERRLECIT 40 mg Injektionslösung", 38.04, "Medikament", 19),
    Product("Ibuprofen 400 mg Filmtabletten", 7.90, "Medikament", 7),
    Product("Paracetamol 500 mg Tabletten", 4.85, "Medikament", 7),
    Product("Pantoprazol 20 mg magensaftresistente Tabletten", 12.40, "Medikament", 7),
    Product("Cetirizin 10 mg Filmtabletten", 6.10, "Medikament", 7),
    Product("Simvastatin 40 mg Tabletten", 17.60, "Medikament", 7),
    Product("Metformin 850 mg Tabletten", 14.75, "Medikament", 7),
    Product("Losartan 100 mg Tabletten", 18.50, "Medikament", 7),
    Product("Atorvastatin 20 mg Tabletten", 24.30, "Medikament", 7),
]

APO_ARTICLES: List[Product] = [
    Product("Nasenspray Classic 20 ml", 5.40, "Apothekenartikel", 19),
    Product("Erkältungsbad 125 ml", 8.90, "Apothekenartikel", 19),
    Product("Vitamin C Brausetabletten 20 Stück", 6.30, "Apothekenartikel", 19),
    Product("La Roche-Posay Lippenbalsam", 9.10, "Apothekenartikel", 19),
    Product("Wund- und Heilsalbe 50 g", 7.50, "Apothekenartikel", 19),
    Product("Desinfektionsgel 100 ml", 4.10, "Apothekenartikel", 19),
    Product("Blutzucker-Teststreifen 50 Stück", 27.80, "Apothekenartikel", 19),
    Product("Kältekompresse wiederverwendbar", 11.20, "Apothekenartikel", 19),
    Product("Kompressionsstrümpfe Klasse I", 32.50, "Apothekenartikel", 19),
    Product("Reiseapotheke-Set komplett", 21.40, "Apothekenartikel", 19),
]

CLOSING_NOTES = [
    "Mit freundlichen Grüßen, Ihre Apotheke am Damm.",
    "Bitte begleichen Sie den Rechnungsbetrag innerhalb von 14 Tagen.",
    "Wir bedanken uns für Ihr Vertrauen in die Apotheke am Damm.",
]

PHARMACY_LINES = [
    "Apotheke am Damm",
    "Am Damm 17",
    "55232 Alzey",
]

RECIPIENT_ADDRESS = [
    "Käfiggasse 10",
    "55232 Alzey",
]

RECIPIENT_LEFT_MM = 25
RETURN_TOP_MM = 20
# Address block must stay between 66 mm and 88 mm from top edge
RECIPIENT_TOP_MM = 66
RECIPIENT_HEIGHT_MM = 20
TITLE_TOP_MM = RECIPIENT_TOP_MM + RECIPIENT_HEIGHT_MM + 4  # 106 mm
INFO_START_MM = TITLE_TOP_MM + 6
CONTENT_LEFT_X = mm_to_pt(RECIPIENT_LEFT_MM)
CONTENT_RIGHT_MARGIN_MM = 25
CONTENT_RIGHT_X = PAGE_WIDTH_PT - mm_to_pt(CONTENT_RIGHT_MARGIN_MM)
CONTENT_WIDTH = CONTENT_RIGHT_X - CONTENT_LEFT_X
COLUMN_RELATIVE_POSITIONS = {
    "qty": 0.00,
    "desc": 0.06,
    "pzn": 0.29,
    "pack": 0.42,
    "price": 0.56,
    "vk": 0.68,
    "adj": 0.79,
    "vat": 0.89,
}


def column_x(key: str) -> float:
    return CONTENT_LEFT_X + COLUMN_RELATIVE_POSITIONS[key] * CONTENT_WIDTH

INFO_FIELDS = ("Datum:", "Kunden-Nr:", "Rechnungs-Nr:")
ADDITIONAL_INFO_TEMPLATES = (
    "Apotheke am Damm, Am Damm 17, 55232 Alzey",
    "Abrechnungszeitraum: vom {period_start} bis {period_end}",
    "Medikation von: {salutation} {first_name} Blüm",
)

INFO_LINE_GAP_PT = 15
POST_INFO_GAP_PT = 10
ADDITIONAL_INFO_GAP_PT = 15
HEADER_GAP_PT = 15
ROW_START_GAP_PT = 20

HEADER_OFFSET_PT = (
    (len(INFO_FIELDS) - 1) * INFO_LINE_GAP_PT
    + POST_INFO_GAP_PT
    + (len(ADDITIONAL_INFO_TEMPLATES) - 1) * ADDITIONAL_INFO_GAP_PT
    + HEADER_GAP_PT
)
HEADER_Y = y_from_top(INFO_START_MM) - HEADER_OFFSET_PT
ROW_START_Y = HEADER_Y - ROW_START_GAP_PT
MIN_ROW_Y = 210
ITEM_HEIGHT = 40

CHAR_WIDTH_OVERRIDES = {
    **{str(d): 556 for d in range(10)},
    ",": 278,
    ".": 278,
    " ": 278,
    "€": 556,
    "%": 556,
}
DEFAULT_CHAR_WIDTH = 500


class PDFCanvas:
    """Erzeugt Content-Stream-Befehle für eine einzelne PDF-Seite."""

    def __init__(self) -> None:
        self.ops: List[str] = []

    @staticmethod
    def _escape(text: str) -> str:
        return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")

    def _text_width(self, text: str, size: float) -> float:
        total_units = sum(CHAR_WIDTH_OVERRIDES.get(ch, DEFAULT_CHAR_WIDTH) for ch in text)
        return (total_units / 1000.0) * size

    def text(self, x: float, y: float, text: str, size: float = 10, bold: bool = False) -> None:
        font = "F2" if bold else "F1"
        safe = self._escape(text)
        self.ops.append(
            f"BT /{font} {size:.2f} Tf 1 0 0 1 {x:.2f} {y:.2f} Tm ({safe}) Tj ET"
        )

    def text_right(self, x: float, y: float, text: str, size: float = 10, bold: bool = False) -> None:
        width = self._text_width(text, size)
        self.text(x - width, y, text, size=size, bold=bold)

    def line(self, x1: float, y1: float, x2: float, y2: float, width: float = 0.5) -> None:
        self.ops.append(f"{width:.2f} w {x1:.2f} {y1:.2f} m {x2:.2f} {y2:.2f} l S")


class SimplePDF:
    def __init__(self, width: int = 595, height: int = 842) -> None:
        self.width = width
        self.height = height

    def build(self, canvases: List[PDFCanvas]) -> bytes:
        from io import BytesIO

        objects: List[List[bytes]] = []

        def add_object(data: bytes = b"") -> int:
            obj_id = len(objects) + 1
            objects.append([data])
            return obj_id

        catalog_id = add_object()
        pages_id = add_object()
        font_regular_id = add_object(
            b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>"
        )
        font_bold_id = add_object(
            b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold /Encoding /WinAnsiEncoding >>"
        )

        page_ids: List[int] = []
        content_ids: List[int] = []

        for canvas in canvases:
            stream_content = "\n".join(canvas.ops).encode("cp1252", errors="replace")
            content_bytes = (
                f"<< /Length {len(stream_content)} >>\nstream\n".encode("ascii")
                + stream_content
                + b"\nendstream\n"
            )
            content_id = add_object(content_bytes)
            content_ids.append(content_id)

            page_id = add_object()
            page_ids.append(page_id)
            objects[page_id - 1][0] = (
                f"<< /Type /Page /Parent {pages_id} 0 R /MediaBox [0 0 {self.width} {self.height}] "
                f"/Resources << /Font << /F1 {font_regular_id} 0 R /F2 {font_bold_id} 0 R >> >> "
                f"/Contents {content_id} 0 R >>"
            ).encode("ascii")

        kids = " ".join(f"{pid} 0 R" for pid in page_ids)
        objects[pages_id - 1][0] = (
            f"<< /Type /Pages /Kids [{kids}] /Count {len(page_ids)} >>".encode("ascii")
        )
        objects[catalog_id - 1][0] = f"<< /Type /Catalog /Pages {pages_id} 0 R >>".encode("ascii")

        output = BytesIO()
        output.write(b"%PDF-1.4\n")
        offsets = [0]
        for idx, (data,) in enumerate(objects, start=1):
            offsets.append(output.tell())
            output.write(f"{idx} 0 obj\n".encode("ascii"))
            output.write(data)
            if not data.endswith(b"\n"):
                output.write(b"\n")
            output.write(b"endobj\n")
        xref_offset = output.tell()
        output.write(f"xref\n0 {len(objects) + 1}\n".encode("ascii"))
        output.write(b"0000000000 65535 f \n")
        for off in offsets[1:]:
            output.write(f"{off:010} 00000 n \n".encode("ascii"))
        output.write(b"trailer\n")
        output.write(f"<< /Size {len(objects)+1} /Root {catalog_id} 0 R >>\n".encode("ascii"))
        output.write(b"startxref\n")
        output.write(f"{xref_offset}\n".encode("ascii"))
        output.write(b"%%EOF")
        return output.getvalue()


def salutation_for(name: str) -> str:
    return "Frau" if name in FEMALE_NAMES else "Herr"


def format_currency(value: float) -> str:
    s = f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"{s} €"


def build_month_targets(rng: random.Random) -> Dict[int, int]:
    max_per_month = min(50, len(FIRST_NAMES))
    return {idx: rng.randint(20, max_per_month) for idx, _ in enumerate(MONTHS, start=1)}


def generate_person_month_plan(
    rng: random.Random, month_targets: Dict[int, int]
) -> Dict[int, List[str]]:
    month_assignments: Dict[int, List[str]] = {idx: [] for idx, _ in enumerate(MONTHS, start=1)}
    remaining = {idx: month_targets[idx] for idx in month_assignments}
    person_months: Dict[str, set[int]] = {name: set() for name in FIRST_NAMES}

    for _ in range(200):
        for idx in month_assignments:
            month_assignments[idx].clear()
            remaining[idx] = month_targets[idx]
        for name in person_months:
            person_months[name].clear()

        try:
            shuffled = FIRST_NAMES[:]
            rng.shuffle(shuffled)
            for person in shuffled:
                avail = [idx for idx, slots in remaining.items() if slots > 0]
                if len(avail) < 2:
                    raise RuntimeError("Nicht genug Kapazität für Mindestanzahl.")
                chosen = rng.sample(avail, 2)
                for month_idx in chosen:
                    month_assignments[month_idx].append(person)
                    person_months[person].add(month_idx)
                    remaining[month_idx] -= 1

            months_with_slots = [idx for idx, slots in remaining.items() if slots > 0]
            while months_with_slots:
                month_idx = rng.choice(months_with_slots)
                eligible = [
                    person
                    for person in FIRST_NAMES
                    if month_idx not in person_months[person] and len(person_months[person]) < 12
                ]
                if not eligible:
                    raise RuntimeError("Keine verfügbarer Empfänger mehr für Monat." )
                person = rng.choice(eligible)
                month_assignments[month_idx].append(person)
                person_months[person].add(month_idx)
                remaining[month_idx] -= 1
                if remaining[month_idx] == 0:
                    months_with_slots.remove(month_idx)

            return month_assignments
        except RuntimeError:
            continue

    raise RuntimeError("Verteilungsplan konnte nicht erzeugt werden.")


def pick_items(rng: random.Random, invoice_date: date) -> List[dict]:
    total_lines = rng.randint(4, 12)
    med_count = rng.randint(1, total_lines - 1)
    medications = [rng.choice(MEDICATIONS) for _ in range(med_count)]
    apo_items = [rng.choice(APO_ARTICLES) for _ in range(total_lines - med_count)]
    combined = medications + apo_items
    rng.shuffle(combined)

    items = []
    for product in combined:
        qty = rng.randint(1, 3 if product.category == "Medikament" else 4)
        gross_unit = round(product.base_price + rng.uniform(-1.5, 3.2), 2)
        gross_total = round(gross_unit * qty, 2)
        vat_rate = product.vat_rate
        net_unit = round(gross_unit / (1 + vat_rate / 100), 2)
        delivery_offset = rng.randint(0, min(25, invoice_date.day - 1 if invoice_date.day > 1 else 0))
        delivery_date = invoice_date - timedelta(days=delivery_offset)
        items.append(
            {
                "delivery_note": f"Lieferschein {rng.randint(2400000, 2499999)} vom {delivery_date.strftime('%d.%m.%Y')}",
                "name": product.name,
                "category": product.category,
                "pzn": f"{rng.randint(10000000, 99999999)}",
                "pack": rng.choice(PACK_SIZES),
                "quantity": qty,
                "price_net": net_unit,
                "price_gross": gross_unit,
                "line_total": gross_total,
                "vat_rate": vat_rate,
                "delivery_date": delivery_date,
            }
        )
    return items


def paginate_items(items: List[dict]) -> List[List[dict]]:
    pages: List[List[dict]] = []
    current: List[dict] = []
    row_y = ROW_START_Y
    for item in items:
        if current and row_y - ITEM_HEIGHT < MIN_ROW_Y:
            pages.append(current)
            current = []
            row_y = ROW_START_Y
        current.append(item)
        row_y -= ITEM_HEIGHT
    if current:
        pages.append(current)
    return pages


def compute_totals(items: List[dict]) -> Dict[str, float]:
    vat_totals = {7: 0.0, 19: 0.0}
    gross_totals = {7: 0.0, 19: 0.0}
    for item in items:
        vat_amount = round(item["line_total"] - item["price_net"] * item["quantity"], 2)
        vat_totals[item["vat_rate"]] += vat_amount
        gross_totals[item["vat_rate"]] += item["line_total"]
    total_vat = round(sum(vat_totals.values()), 2)
    total_gross = round(sum(gross_totals.values()), 2)
    total_net = round(total_gross - total_vat, 2)
    return {
        "vat_totals": vat_totals,
        "gross_totals": gross_totals,
        "total_vat": total_vat,
        "total_gross": total_gross,
        "total_net": total_net,
    }


def draw_header(canvas: PDFCanvas, data: dict, include_recipient: bool) -> None:
    contact_y = 795
    canvas.text(360, contact_y, "Tel.:", bold=True)
    canvas.text(400, contact_y, "06731-548846")
    canvas.text(360, contact_y - 15, "Fax:", bold=True)
    canvas.text(400, contact_y - 15, "06731-548847")
    for idx, line in enumerate(PHARMACY_LINES):
        canvas.text(360, 750 - idx * 15, line, bold=(idx == 0))

    sender_x = CONTENT_LEFT_X
    sender_y = y_from_top(RETURN_TOP_MM)
    sender_gap = mm_to_pt(4)
    for idx, line in enumerate(PHARMACY_LINES):
        canvas.text(sender_x, sender_y - idx * sender_gap, line, size=9)

    if include_recipient:
        recipient_lines = [
            salutation_for(data["first_name"]),
            f"{data['first_name']} Blüm",
            RECIPIENT_ADDRESS[0],
            RECIPIENT_ADDRESS[1],
        ]
        recip_y = y_from_top(RECIPIENT_TOP_MM)
        recip_gap = mm_to_pt(4.5)
        for idx, line in enumerate(recipient_lines):
            canvas.text(sender_x, recip_y - idx * recip_gap, line, size=11, bold=(idx == 1))
    else:
        canvas.text(sender_x, y_from_top(RECIPIENT_TOP_MM), f"{data['first_name']} Blüm", bold=True)

    title_y = y_from_top(TITLE_TOP_MM)
    canvas.text(sender_x, title_y, "Rechnung", size=20, bold=True)

    info_y_start = y_from_top(INFO_START_MM)
    info_values = [
        data["invoice_date"].strftime("%d.%m.%Y"),
        data["customer_id"],
        data["invoice_number"],
    ]
    for idx, (label, value) in enumerate(zip(INFO_FIELDS, info_values)):
        y = info_y_start - idx * INFO_LINE_GAP_PT
        canvas.text(360, y, label, bold=True)
        canvas.text(450, y, value)

    extra_context = {
        "period_start": data["period_start"],
        "period_end": data["period_end"],
        "salutation": salutation_for(data["first_name"]),
        "first_name": data["first_name"],
    }
    extra_start = info_y_start - ((len(INFO_FIELDS) - 1) * INFO_LINE_GAP_PT + POST_INFO_GAP_PT)
    for idx, template in enumerate(ADDITIONAL_INFO_TEMPLATES):
        y = extra_start - idx * ADDITIONAL_INFO_GAP_PT
        canvas.text(sender_x, y, template.format(**extra_context))

    columns = [
        ("Menge", column_x("qty"), True),
        ("Artikelname", column_x("desc"), False),
        ("PZN", column_x("pzn"), False),
        ("Pack", column_x("pack"), False),
        ("Preis", column_x("price"), True),
        ("VK", column_x("vk"), True),
        ("Auf/Ab", column_x("adj"), True),
        ("MwSt", column_x("vat"), True),
    ]
    for title, x, align_right in columns:
        if align_right:
            canvas.text_right(x, HEADER_Y, title, bold=True)
        else:
            canvas.text(x, HEADER_Y, title, bold=True)
    canvas.line(sender_x, HEADER_Y - 5, CONTENT_RIGHT_X, HEADER_Y - 5)


def draw_items(canvas: PDFCanvas, items: List[dict]) -> float:
    qty_x = column_x("qty")
    desc_x = column_x("desc")
    pzn_x = column_x("pzn")
    pack_x = column_x("pack")
    price_x = column_x("price")
    vk_x = column_x("vk")
    adj_x = column_x("adj")
    vat_x = column_x("vat")
    row_y = ROW_START_Y
    for item in items:
        canvas.text(desc_x, row_y + 10, item["delivery_note"], bold=True)
        row_y -= 18
        canvas.text_right(qty_x, row_y, f"{item['quantity']}")
        canvas.text(desc_x, row_y, item["name"][:20])
        canvas.text(pzn_x, row_y, item["pzn"])
        canvas.text(pack_x, row_y, item["pack"])
        canvas.text_right(price_x, row_y, format_currency(item["price_net"]))
        canvas.text_right(vk_x, row_y, format_currency(item["price_gross"]))
        canvas.text_right(adj_x, row_y, format_currency(0.0))
        canvas.text_right(vat_x, row_y, f"{item['vat_rate']} %")
        row_y -= 22
    canvas.line(CONTENT_LEFT_X, row_y + 10, CONTENT_RIGHT_X, row_y + 10)
    return row_y


def draw_summary_and_footer(canvas: PDFCanvas, row_y: float, totals: Dict[str, float], note: str) -> float:
    label_x = CONTENT_LEFT_X + CONTENT_WIDTH * 0.5
    value_x = CONTENT_LEFT_X + CONTENT_WIDTH * 0.82
    summary_y = row_y - 10
    entries = [
        ("Rechnungsbetrag", None, True),
        ("7 %", format_currency(totals["gross_totals"][7]), False),
        ("19 %", format_currency(totals["gross_totals"][19]), False),
        ("Auf-/Abschlag Gesamt", format_currency(0.0), False),
        ("MwSt-Betrag", format_currency(totals["total_vat"]), False),
        ("Nettobetrag", format_currency(totals["total_net"]), False),
    ]
    current_y = summary_y
    for label, value, bold in entries:
        canvas.text(label_x, current_y, label, bold=bold)
        if value:
            canvas.text_right(value_x, current_y, value)
        current_y -= 15

    totals_label_x = CONTENT_LEFT_X
    totals_value_x = value_x
    canvas.text(totals_label_x, current_y - 15, "Rezeptzuzahlungen (kein Vorsteuerabzug)")
    canvas.text_right(totals_value_x, current_y - 15, format_currency(0.0))
    canvas.text(totals_label_x, current_y - 30, "Summe Vorst. abziehbar")
    canvas.text_right(totals_value_x, current_y - 30, format_currency(totals["total_vat"]))
    canvas.text(totals_label_x, current_y - 45, "Gesamtsumme", bold=True)
    canvas.text_right(
        totals_value_x,
        current_y - 45,
        format_currency(totals["total_gross"]),
        bold=True,
    )

    note_y = current_y - 60
    canvas.text(CONTENT_LEFT_X, note_y, note)

    footer_line_y = note_y - 20
    canvas.line(CONTENT_LEFT_X, footer_line_y, CONTENT_RIGHT_X, footer_line_y)
    canvas.text(
        CONTENT_LEFT_X,
        footer_line_y - 20,
        "Bankverbindung: Sparkasse Worms-Alzey-Ried, IBAN: DE51 5535 0010 0033 7173 83, BIC: MALADE51WOR",
    )
    canvas.text(CONTENT_LEFT_X, footer_line_y - 35, "Inhaber/in: Matthias Blüm")
    canvas.text(CONTENT_LEFT_X, footer_line_y - 50, "Gerichtsstand: Alzey | HRA-Nummer: 31710 | Steuernummer: 44/567/12345")
    return footer_line_y - 65


def render_invoice(data: dict) -> bytes:
    pages = paginate_items(data["items"])
    if not pages:
        pages = [[]]
    totals = compute_totals(data["items"])
    total_pages = len(pages) or 1

    canvases: List[PDFCanvas] = []
    pdf = SimplePDF()

    for idx, page_items in enumerate(pages, start=1):
        canvas = PDFCanvas()
        draw_header(canvas, data, include_recipient=(idx == 1))
        row_y = draw_items(canvas, page_items)
        if idx == total_pages:
            page_number_y = draw_summary_and_footer(canvas, row_y, totals, data["note"])
        else:
            canvas.text(CONTENT_LEFT_X, row_y - 20, "Fortsetzung auf nächster Seite ...")
            page_number_y = 55.0
        canvas.text(CONTENT_LEFT_X, page_number_y, f"Seite {idx} von {total_pages}")
        canvases.append(canvas)

    return pdf.build(canvases)


def create_invoices() -> None:
    rng = random.Random(20240602)
    ensure_folder(TARGET_ROOT)

    # Pool aller erstellten Rechnungen (werden über Zeit akkumuliert)
    all_invoices = []
    # Pool der aktiven Personen
    active_people = set(FIRST_NAMES[:10])  # Starten mit 10 Personen

    # Zeitraum: Januar 2024 bis Oktober 2025 (22 Monate)
    start_year, start_month = 2024, 1
    end_year, end_month = 2025, 10

    current_year, current_month = start_year, start_month

    while (current_year, current_month) <= (end_year, end_month):
        month_index_in_year = current_month
        month_abbr, month_full = MONTHS[current_month - 1]
        month_dir = TARGET_ROOT / f"{current_year}-{current_month:02d}_{month_abbr}"
        ensure_folder(month_dir)

        # Neue Rechnungen für diesen Monat erstellen
        num_new_invoices = rng.randint(20, 35)
        max_day = calendar.monthrange(current_year, current_month)[1]

        for i in range(num_new_invoices):
            # Zufällige Person aus dem aktiven Pool
            first_name = rng.choice(list(active_people))

            invoice_number = f"S-{current_year}{current_month:02d}-{i+1:04d}"
            invoice_date = date(current_year, current_month, rng.randint(1, max_day))
            items = pick_items(rng, invoice_date)
            period_start = min(item["delivery_date"] for item in items).strftime("%d.%m.%Y")
            period_end = max(item["delivery_date"] for item in items).strftime("%d.%m.%Y")
            customer_id = f"{70000 + hash(first_name) % 10000:05d}"
            note = rng.choice(CLOSING_NOTES)

            invoice_data = {
                "invoice_date": invoice_date,
                "invoice_number": invoice_number,
                "customer_id": customer_id,
                "first_name": first_name,
                "items": items,
                "period_start": period_start,
                "period_end": period_end,
                "note": note,
            }

            all_invoices.append(invoice_data)

        # Zahlungssimulation: Entscheiden welche Rechnungen "bezahlt" werden (verschwinden)
        unpaid_invoices = []
        for inv in all_invoices:
            # Berechne Alter der Rechnung in Monaten
            inv_year, inv_month = inv["invoice_date"].year, inv["invoice_date"].month
            age_months = (current_year - inv_year) * 12 + (current_month - inv_month)

            # Zahlungswahrscheinlichkeit basierend auf Alter
            if age_months == 0:
                # Neue Rechnungen: bleiben alle offen
                prob_paid = 0.0
            elif age_months == 1:
                # Nach 1 Monat: 50% bezahlt
                prob_paid = 0.50
            elif age_months == 2:
                # Nach 2 Monaten: weitere 30% bezahlt
                prob_paid = 0.30
            elif age_months == 3:
                # Nach 3 Monaten: weitere 15% bezahlt
                prob_paid = 0.15
            elif age_months <= 6:
                # Nach 4-6 Monaten: weitere 4% pro Monat bezahlt
                prob_paid = 0.04
            else:
                # Nach 6+ Monaten: nur noch 1% pro Monat bezahlt (chronische Mahnfälle)
                prob_paid = 0.01

            # Entscheidung: bleibt Rechnung offen?
            if rng.random() > prob_paid:
                unpaid_invoices.append(inv)

        all_invoices = unpaid_invoices

        # Snapshot: Alle offenen Rechnungen in diesen Monatsordner schreiben
        for inv in all_invoices:
            pdf_bytes = render_invoice(inv)
            filename = f"{inv['invoice_date'].strftime('%Y%m%d')}_{inv['invoice_number']}_{inv['first_name']}_Blüm.pdf"
            (month_dir / filename).write_bytes(pdf_bytes)

        print(f"{current_year}-{current_month:02d}_{month_abbr}: {len(all_invoices)} offene Rechnungen ({num_new_invoices} neue)")

        # Manchmal neue Personen hinzufügen
        if rng.random() < 0.3 and len(active_people) < len(FIRST_NAMES):
            available = [n for n in FIRST_NAMES if n not in active_people]
            if available:
                new_person = rng.choice(available)
                active_people.add(new_person)

        # Nächster Monat
        current_month += 1
        if current_month > 12:
            current_month = 1
            current_year += 1

    print(f"\nFertig! Rechnungen wurden in '{TARGET_ROOT.name}' abgelegt.")


def ensure_folder(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def main() -> None:
    create_invoices()


if __name__ == "__main__":
    main()
