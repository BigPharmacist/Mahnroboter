#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SEPA-Lastschriftmandat PDF Generator
Erstellt eine Vorlage basierend auf dem Original-Dokument
"""

from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas
from reportlab.lib.colors import black
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

def create_sepa_mandate_pdf(filename="SEPA_Lastschriftmandat_Vorlage.pdf"):
    """Erstellt ein SEPA-Lastschriftmandat PDF"""

    # PDF erstellen
    c = canvas.Canvas(filename, pagesize=A4)
    width, height = A4

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

    # Ermächtigungstext - Links (Deutsch)
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

    # Zahlungspflichtiger Felder
    c.setFont("Helvetica", 7)

    # Name
    c.drawString(20*mm, y_pos, "Zahlungspflichtiger")
    y_pos -= 3*mm
    c.rect(20*mm, y_pos - 5*mm, 170*mm, 7*mm, stroke=1, fill=0)
    y_pos -= 10*mm

    # Straße und Hausnummer
    c.drawString(20*mm, y_pos, "Straße und Hausnummer")
    y_pos -= 3*mm
    c.rect(20*mm, y_pos - 5*mm, 170*mm, 7*mm, stroke=1, fill=0)
    y_pos -= 10*mm

    # PLZ und Ort
    c.drawString(20*mm, y_pos, "PLZ und Ort")
    y_pos -= 3*mm
    c.rect(20*mm, y_pos - 5*mm, 170*mm, 7*mm, stroke=1, fill=0)
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

    # PDF speichern
    c.save()
    print(f"PDF erfolgreich erstellt: {filename}")

if __name__ == "__main__":
    create_sepa_mandate_pdf()
