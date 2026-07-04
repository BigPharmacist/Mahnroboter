"""Privatrezepte: Monats-Scans (DIN A6) importieren, in Einzelseiten splitten,
drehen und Kunden zuordnen.

Ein Monats-Scan ist ein PDF mit vielen Seiten (z.B. 55 Rezepte = 55 Seiten).
Beim Import wird das PDF in Einzelseiten zerlegt (pypdf, keine Bild-Rasterung)
und unter ``DATA_DIR/Rezepte/<Monat>/batch_<id>/`` abgelegt. Im
Zuordnungsbildschirm kann jede Seite einzeln gedreht und einem Kunden aus der
Kundendatei zugeordnet werden. Der Abrechnungsmonat haengt am Stapel und wird je
Seite gespeichert (auch fuer spaetere Einzelzuordnung).

Die zugeordneten Seiten werden bei der Sammelrechnungs-Erzeugung automatisch an
die Sammelrechnung des jeweiligen Kunden fuer den Monat angehaengt
(siehe ``assigned_pages_for`` / ``rotated_page_bytes``).
"""

from __future__ import annotations

import io
import logging
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

from flask import (
    Flask,
    Response,
    abort,
    current_app,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)
from pypdf import PdfReader, PdfWriter, PageObject, Transformation

from config import get_data_dir
from data_access import fetch_all_customers

# Wurzelordner fuer alle Rezept-Scans (relativ zu DATA_DIR)
REZEPTE_DIRNAME = "Rezepte"


# --------------------------------------------------------------------------- #
# Schema
# --------------------------------------------------------------------------- #
def init_rezepte_schema(conn: sqlite3.Connection) -> None:
    """Lege die Tabellen fuer Rezept-Stapel und -Seiten an (idempotent)."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS rezept_batch (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            billing_month TEXT NOT NULL,            -- YYYY-MM (Abrechnungsmonat)
            original_filename TEXT,                 -- urspruenglicher Dateiname
            original_path TEXT,                     -- Pfad des Original-PDFs (relativ zu DATA_DIR)
            page_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS rezept_page (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id INTEGER NOT NULL,
            page_number INTEGER NOT NULL,           -- 1-basiert innerhalb des Stapels
            pdf_path TEXT NOT NULL,                 -- Einzelseiten-PDF (relativ zu DATA_DIR)
            rotation INTEGER NOT NULL DEFAULT 0,    -- 0/90/180/270 (im Uhrzeigersinn)
            customer_name TEXT,                     -- zugeordneter Kunde (Schluessel aus invoices)
            billing_month TEXT,                     -- denormalisiert vom Stapel, einzeln aenderbar
            updated_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
            FOREIGN KEY (batch_id) REFERENCES rezept_batch(id) ON DELETE CASCADE,
            UNIQUE(batch_id, page_number)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_rezept_page_customer_month "
        "ON rezept_page(customer_name, billing_month)"
    )
    # Migration: Rezept-Seite direkt an eine konkrete Rechnung binden.
    # invoice_id ist die primaere Verknuepfung; billing_month bleibt Fallback/Anzeige.
    try:
        conn.execute("ALTER TABLE rezept_page ADD COLUMN invoice_id INTEGER")
    except sqlite3.OperationalError:
        pass  # Spalte existiert bereits
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_rezept_page_invoice ON rezept_page(invoice_id)"
    )


# --------------------------------------------------------------------------- #
# PDF-Helfer
# --------------------------------------------------------------------------- #
def _normalize_rotation(value: Optional[int]) -> int:
    """Auf 0/90/180/270 normalisieren."""
    try:
        v = int(value or 0)
    except (TypeError, ValueError):
        v = 0
    return v % 360


def rotated_page_bytes(abs_path: Path, rotation: int = 0) -> bytes:
    """Gib das (einseitige) PDF unter ``abs_path`` als Bytes zurueck, optional
    um ``rotation`` Grad im Uhrzeigersinn gedreht. Das Original bleibt
    unveraendert -- die Drehung wird nur on-the-fly angewendet.
    """
    reader = PdfReader(str(abs_path))
    writer = PdfWriter()
    rot = _normalize_rotation(rotation)
    for page in reader.pages:
        if rot:
            page.rotate(rot)
        writer.add_page(page)
    out = io.BytesIO()
    writer.write(out)
    return out.getvalue()


def _split_pdf_into_pages(src_pdf: Path, dest_dir: Path) -> List[Path]:
    """Zerlege ein mehrseitiges PDF in einzelne Seiten-PDFs in ``dest_dir``.
    Gibt die Liste der erzeugten Dateipfade (sortiert) zurueck.
    """
    reader = PdfReader(str(src_pdf))
    dest_dir.mkdir(parents=True, exist_ok=True)
    paths: List[Path] = []
    for i, page in enumerate(reader.pages, start=1):
        writer = PdfWriter()
        writer.add_page(page)
        page_path = dest_dir / f"seite_{i:04d}.pdf"
        with open(page_path, "wb") as fh:
            writer.write(fh)
        paths.append(page_path)
    return paths


# --------------------------------------------------------------------------- #
# Abfrage-Helfer (auch fuer die Sammelrechnungs-Integration)
# --------------------------------------------------------------------------- #
def assigned_pages_for(
    conn: sqlite3.Connection, customer_name: str, billing_month: str
) -> List[sqlite3.Row]:
    """Alle einem Kunden fuer einen Abrechnungsmonat zugeordneten Rezeptseiten,
    nach Stapel und Seitennummer sortiert. ``conn`` muss row_factory=Row haben.
    (Monatsbasiert -- fuer Vorschau/Fallback.)
    """
    return conn.execute(
        """
        SELECT id, pdf_path, rotation, page_number, batch_id, invoice_id
        FROM rezept_page
        WHERE customer_name = ? AND billing_month = ?
        ORDER BY batch_id, page_number
        """,
        (customer_name, billing_month),
    ).fetchall()


def assigned_pages_for_invoices(
    conn: sqlite3.Connection, invoice_ids
) -> List[sqlite3.Row]:
    """Alle Rezeptseiten, die direkt an eine der Rechnungen ``invoice_ids``
    gebunden sind (primaere Verknuepfung). ``conn`` muss row_factory=Row haben.
    """
    ids = [int(i) for i in invoice_ids if i is not None]
    if not ids:
        return []
    placeholders = ",".join("?" * len(ids))
    return conn.execute(
        f"""
        SELECT id, pdf_path, rotation, page_number, batch_id, invoice_id
        FROM rezept_page
        WHERE invoice_id IN ({placeholders})
        ORDER BY batch_id, page_number
        """,
        ids,
    ).fetchall()


def fetch_customer_invoices(
    conn: sqlite3.Connection, customer_name: str, limit: int = 50
) -> List[sqlite3.Row]:
    """Rechnungen eines Kunden fuer das Zuordnungs-Pulldown -- neueste (zuletzt
    importierte) zuerst.
    """
    return conn.execute(
        """
        SELECT id, invoice_number, invoice_date, amount_cents
        FROM invoices
        WHERE customer_name = ?
        ORDER BY created_at DESC, invoice_date DESC, id DESC
        LIMIT ?
        """,
        (customer_name, limit),
    ).fetchall()


def _invoice_option(r: sqlite3.Row) -> dict:
    """rezept-invoice-Zeile -> Dict fuer das Pulldown (deutsches Datum, Betrag)."""
    date_de = r["invoice_date"]
    try:
        y, m, d = r["invoice_date"].split("-")
        date_de = f"{d}.{m}.{y}"
    except Exception:
        pass
    return {
        "id": r["id"],
        "number": r["invoice_number"] or "ohne Nr.",
        "date": date_de,
        "amount": f"{(r['amount_cents'] or 0) / 100:.2f} €",
    }


# DIN A4 in PDF-Punkten (1 mm = 2.834645 pt); A4 = exakt 4x DIN A6
A4_WIDTH_PT = 595.2756
A4_HEIGHT_PT = 841.8898
_QUADRANT_MARGIN_PT = 2.0  # minimaler Steg je Kachel (Rezepte fuellen das A4 nahezu ganz)


def _collect_source_pages_from_rows(rows) -> Tuple[List[PageObject], int]:
    """Sammle die (gedrehten, hochkant orientierten) Einzelseiten aus rezept_page-
    Zeilen. Gibt (Liste der PageObjects, Anzahl Rezept-Scans) zurueck.
    """
    data_dir = get_data_dir()
    sources: List[PageObject] = []
    count = 0
    for row in rows:
        abs_path = data_dir / row["pdf_path"]
        if not abs_path.exists():
            logging.warning("Rezeptseite fehlt auf der Platte: %s", abs_path)
            continue
        try:
            reader = PdfReader(io.BytesIO(rotated_page_bytes(abs_path, row["rotation"])))
            for page in reader.pages:
                # /Rotate in den Inhalt uebertragen -> MediaBox spiegelt Ausrichtung
                try:
                    page.transfer_rotation_to_content()
                except Exception:
                    pass
                # Fuer das 4-auf-1: quer eingescannte Rezepte automatisch hochkant
                # drehen, damit ein hochkantes A6 die A6-Kachel voll ausfuellt.
                try:
                    box = page.mediabox
                    if float(box.width) > float(box.height):
                        page.rotate(90)
                        page.transfer_rotation_to_content()
                except Exception:
                    pass
                sources.append(page)
            count += 1
        except Exception as exc:  # pragma: no cover - defensiv
            logging.error("Fehler beim Lesen der Rezeptseite %s: %s", abs_path, exc)
    return sources, count


def _build_4up_pages_from_rows(rows) -> Tuple[List[PageObject], int]:
    """Erzeuge A4-Seiten mit je 4 Rezept-Scans (2x2, Leserichtung oben-links,
    oben-rechts, unten-links, unten-rechts). Jeder Scan wird proportional in
    seine Kachel eingepasst. Gibt (A4-Seiten, Anzahl Scans) zurueck.
    """
    sources, count = _collect_source_pages_from_rows(rows)
    if not sources:
        return [], 0

    qw, qh = A4_WIDTH_PT / 2.0, A4_HEIGHT_PT / 2.0
    quadrants = [(0.0, qh), (qw, qh), (0.0, 0.0), (qw, 0.0)]  # OL, OR, UL, UR

    out_pages: List[PageObject] = []
    for i in range(0, len(sources), 4):
        sheet = PageObject.create_blank_page(width=A4_WIDTH_PT, height=A4_HEIGHT_PT)
        for j, src in enumerate(sources[i : i + 4]):
            qx, qy = quadrants[j]
            box = src.mediabox
            sx0, sy0 = float(box.left), float(box.bottom)
            sw, sh = float(box.width), float(box.height)
            if sw <= 0 or sh <= 0:
                continue
            avail_w = qw - 2 * _QUADRANT_MARGIN_PT
            avail_h = qh - 2 * _QUADRANT_MARGIN_PT
            scale = min(avail_w / sw, avail_h / sh)
            tx = qx + (qw - sw * scale) / 2.0 - sx0 * scale
            ty = qy + (qh - sh * scale) / 2.0 - sy0 * scale
            op = Transformation().scale(scale).translate(tx, ty)
            try:
                sheet.merge_transformed_page(src, op)
            except Exception as exc:  # pragma: no cover - defensiv
                logging.error("Fehler beim Zusammensetzen einer Rezept-Kachel: %s", exc)
        out_pages.append(sheet)
    return out_pages, count


def _pages_to_bytes(pages: List[PageObject], count: int) -> Tuple[Optional[bytes], int]:
    if not pages:
        return None, 0
    writer = PdfWriter()
    for page in pages:
        writer.add_page(page)
    buf = io.BytesIO()
    writer.write(buf)
    return buf.getvalue(), count


def _simple_bytes_from_rows(rows) -> Tuple[Optional[bytes], int]:
    """Jeder Scan (gedreht) als eigene Seite in Originalgroesse -- ohne 4-auf-1."""
    data_dir = get_data_dir()
    writer = PdfWriter()
    count = 0
    for row in rows:
        abs_path = data_dir / row["pdf_path"]
        if not abs_path.exists():
            logging.warning("Rezeptseite fehlt auf der Platte: %s", abs_path)
            continue
        try:
            reader = PdfReader(io.BytesIO(rotated_page_bytes(abs_path, row["rotation"])))
            for page in reader.pages:
                writer.add_page(page)
            count += 1
        except Exception as exc:  # pragma: no cover - defensiv
            logging.error("Fehler beim Anhaengen der Rezeptseite %s: %s", abs_path, exc)
    if count == 0:
        return None, 0
    buf = io.BytesIO()
    writer.write(buf)
    return buf.getvalue(), count


def _scan_pdfs_from_rows(rows) -> List[bytes]:
    """Je Scan ein eigenstaendiges (gedrehtes) Einzel-PDF als Bytes."""
    data_dir = get_data_dir()
    out: List[bytes] = []
    for row in rows:
        abs_path = data_dir / row["pdf_path"]
        if not abs_path.exists():
            logging.warning("Rezeptseite fehlt auf der Platte: %s", abs_path)
            continue
        try:
            out.append(rotated_page_bytes(abs_path, row["rotation"]))
        except Exception as exc:  # pragma: no cover - defensiv
            logging.error("Fehler beim Lesen der Rezeptseite %s: %s", abs_path, exc)
    return out


def prescription_basename(billing_month: str) -> str:
    """Dateinamens-Basis fuer Rezept-Anhaenge, OHNE Namen des Versicherten.
    '2026-06' -> 'Rezepte_2026_06'.
    """
    return "Rezepte_" + (billing_month or "").replace("-", "_")


# ---- Monatsbasiert (Vorschau / Fallback) --------------------------------- #
def build_prescriptions_4up_pages(conn, customer_name, billing_month):
    return _build_4up_pages_from_rows(assigned_pages_for(conn, customer_name, billing_month))


def build_prescriptions_4up_bytes(conn, customer_name, billing_month):
    return _pages_to_bytes(*build_prescriptions_4up_pages(conn, customer_name, billing_month))


def build_prescriptions_simple_bytes(conn, customer_name, billing_month):
    return _simple_bytes_from_rows(assigned_pages_for(conn, customer_name, billing_month))


def build_prescription_scan_pdfs(conn, customer_name, billing_month):
    return _scan_pdfs_from_rows(assigned_pages_for(conn, customer_name, billing_month))


def append_prescriptions_to_writer(conn, writer, customer_name, billing_month):
    pages, count = build_prescriptions_4up_pages(conn, customer_name, billing_month)
    for page in pages:
        writer.add_page(page)
    return count


# ---- Rechnungsbasiert (echter Versand: Brief + E-Mail) ------------------- #
def append_prescriptions_for_invoices(conn, writer, invoice_ids) -> int:
    """Haenge die an eine der ``invoice_ids`` gebundenen Rezepte (4-auf-1) an
    ``writer`` an. Gibt die Anzahl der Rezept-Scans zurueck.
    """
    pages, count = _build_4up_pages_from_rows(assigned_pages_for_invoices(conn, invoice_ids))
    for page in pages:
        writer.add_page(page)
    return count


def build_scan_pdfs_for_invoices(conn, invoice_ids) -> List[bytes]:
    """Je gebundenem Rezept ein Einzel-PDF (Bytes) fuer nummerierte E-Mail-Anhaenge."""
    return _scan_pdfs_from_rows(assigned_pages_for_invoices(conn, invoice_ids))


def billing_month_for_invoices(conn, invoice_ids) -> Optional[str]:
    """Abrechnungsmonat der an diese Rechnungen gebundenen Rezepte (fuer den
    Dateinamen des E-Mail-Anhangs). Neuester zugeordneter Monat, sonst None.
    """
    ids = [int(i) for i in invoice_ids if i is not None]
    if not ids:
        return None
    placeholders = ",".join("?" * len(ids))
    row = conn.execute(
        f"SELECT billing_month FROM rezept_page WHERE invoice_id IN ({placeholders}) "
        "AND billing_month IS NOT NULL AND billing_month != '' "
        "ORDER BY billing_month DESC LIMIT 1",
        ids,
    ).fetchone()
    return row["billing_month"] if row else None


# --------------------------------------------------------------------------- #
# DB-Verbindung im Request-Kontext
# --------------------------------------------------------------------------- #
def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(current_app.config["DATABASE"])
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


_MONTH_RE = re.compile(r"^\d{4}-\d{2}$")

_MONTHS_DE = {
    "01": "Januar", "02": "Februar", "03": "März", "04": "April",
    "05": "Mai", "06": "Juni", "07": "Juli", "08": "August",
    "09": "September", "10": "Oktober", "11": "November", "12": "Dezember",
}


def _valid_month(value: str) -> bool:
    return bool(value and _MONTH_RE.match(value.strip()))


def _month_label(ym: str) -> str:
    """'2026-06' -> 'Juni 2026'."""
    try:
        year, month = ym.split("-")
        return f"{_MONTHS_DE.get(month, month)} {year}"
    except Exception:
        return ym


def _available_invoice_months(conn: sqlite3.Connection) -> List[dict]:
    """Vorhandene Rechnungsmonate (YYYY-MM) absteigend, mit deutschem Label."""
    rows = conn.execute(
        """
        SELECT DISTINCT substr(invoice_date, 1, 7) AS m
        FROM invoices
        WHERE invoice_date IS NOT NULL AND invoice_date != ''
        ORDER BY m DESC
        """
    ).fetchall()
    return [{"value": r["m"], "label": _month_label(r["m"])} for r in rows if r["m"]]


# --------------------------------------------------------------------------- #
# Routen
# --------------------------------------------------------------------------- #
def register_rezepte_routes(app: Flask) -> None:
    """Registriere alle /rezepte-Routen auf der Flask-App."""

    @app.route("/rezepte")
    def rezepte():
        conn = _connect()
        try:
            batches = conn.execute(
                """
                SELECT
                    b.id, b.billing_month, b.original_filename, b.page_count, b.created_at,
                    COUNT(p.id) AS pages,
                    SUM(CASE WHEN p.customer_name IS NOT NULL AND p.customer_name != '' THEN 1 ELSE 0 END) AS assigned
                FROM rezept_batch b
                LEFT JOIN rezept_page p ON p.batch_id = b.id
                GROUP BY b.id
                ORDER BY b.billing_month DESC, b.created_at DESC
                """
            ).fetchall()
            months = _available_invoice_months(conn)
        finally:
            conn.close()
        # Vorgabe = letzter Rechnungsmonat (nicht der heutige Monat)
        default_month = months[0]["value"] if months else datetime.now().strftime("%Y-%m")
        return render_template(
            "rezepte.html",
            batches=batches,
            default_month=default_month,
            months=months,
        )

    @app.route("/rezepte/upload", methods=["POST"])
    def rezepte_upload():
        month = (request.form.get("billing_month") or "").strip()
        if not _valid_month(month):
            flash("Bitte einen gueltigen Abrechnungsmonat (JJJJ-MM) waehlen.", "error")
            return redirect(url_for("rezepte"))

        file = request.files.get("pdf")
        if not file or not file.filename:
            flash("Bitte eine PDF-Datei auswaehlen.", "error")
            return redirect(url_for("rezepte"))
        if not file.filename.lower().endswith(".pdf"):
            flash("Nur PDF-Dateien werden unterstuetzt.", "error")
            return redirect(url_for("rezepte"))

        data_dir = get_data_dir()
        conn = _connect()
        try:
            # Stapel-Datensatz anlegen, um die ID fuer den Ordnernamen zu erhalten
            cur = conn.execute(
                "INSERT INTO rezept_batch (billing_month, original_filename, page_count) VALUES (?, ?, 0)",
                (month, file.filename),
            )
            batch_id = cur.lastrowid

            batch_dir = data_dir / REZEPTE_DIRNAME / month / f"batch_{batch_id}"
            batch_dir.mkdir(parents=True, exist_ok=True)

            # Original speichern
            original_path = batch_dir / "original.pdf"
            file.save(str(original_path))

            # In Einzelseiten splitten
            try:
                page_paths = _split_pdf_into_pages(original_path, batch_dir)
            except Exception as exc:
                conn.execute("DELETE FROM rezept_batch WHERE id = ?", (batch_id,))
                conn.commit()
                logging.error("PDF konnte nicht gesplittet werden: %s", exc)
                flash("Die PDF-Datei konnte nicht gelesen werden.", "error")
                return redirect(url_for("rezepte"))

            if not page_paths:
                conn.execute("DELETE FROM rezept_batch WHERE id = ?", (batch_id,))
                conn.commit()
                flash("Die PDF-Datei enthaelt keine Seiten.", "error")
                return redirect(url_for("rezepte"))

            for i, page_path in enumerate(page_paths, start=1):
                rel = page_path.relative_to(data_dir).as_posix()
                conn.execute(
                    """
                    INSERT INTO rezept_page (batch_id, page_number, pdf_path, billing_month)
                    VALUES (?, ?, ?, ?)
                    """,
                    (batch_id, i, rel, month),
                )

            conn.execute(
                "UPDATE rezept_batch SET page_count = ?, original_path = ? WHERE id = ?",
                (len(page_paths), original_path.relative_to(data_dir).as_posix(), batch_id),
            )
            conn.commit()
        finally:
            conn.close()

        flash(f"{len(page_paths)} Rezepte importiert.", "success")
        return redirect(url_for("rezepte_zuordnung", batch_id=batch_id))

    @app.route("/rezepte/<int:batch_id>")
    def rezepte_zuordnung(batch_id: int):
        conn = _connect()
        try:
            batch = conn.execute(
                "SELECT * FROM rezept_batch WHERE id = ?", (batch_id,)
            ).fetchone()
            if not batch:
                abort(404)
            pages = conn.execute(
                "SELECT * FROM rezept_page WHERE batch_id = ? ORDER BY page_number",
                (batch_id,),
            ).fetchall()
            months = _available_invoice_months(conn)
            assignment_rows = conn.execute(
                """
                SELECT customer_name, billing_month, COUNT(*) AS n
                FROM rezept_page
                WHERE batch_id = ? AND customer_name IS NOT NULL AND customer_name != ''
                GROUP BY customer_name, billing_month
                ORDER BY customer_name
                """,
                (batch_id,),
            ).fetchall()
            # Rechnungen der bereits zugeordneten Kunden vorladen (fuer Pulldowns)
            invoice_map = {}
            for cn in {p["customer_name"] for p in pages if p["customer_name"]}:
                invoice_map[cn] = [
                    _invoice_option(r) for r in fetch_customer_invoices(conn, cn)
                ]
        finally:
            conn.close()

        # Sicherstellen, dass die Monate der Seiten/des Stapels im Pulldown sind
        mvals = {m["value"] for m in months}
        extra = {batch["billing_month"]} | {p["billing_month"] for p in pages if p["billing_month"]}
        for mv in sorted((v for v in extra if v and v not in mvals), reverse=True):
            months.insert(0, {"value": mv, "label": _month_label(mv)})

        # Kundenliste fuer das Suchfeld (Schluessel + Anzeigename), dedupliziert
        # nach internem Namen (ein Kunde kann mehrere Rechnungsadressen haben).
        customers = []
        seen = set()
        for c in fetch_all_customers(current_app.config["DATABASE"]):
            key = c["customer_name"]
            if key in seen:
                continue
            seen.add(key)
            display = (c.get("custom_name") or "").strip() or key
            customers.append({"name": key, "display": display})

        name_to_display = {c["name"]: c["display"] for c in customers}
        assignments = [
            {
                "name": r["customer_name"],
                "display": name_to_display.get(r["customer_name"], r["customer_name"]),
                "month": r["billing_month"],
                "month_label": _month_label(r["billing_month"]),
                "n": r["n"],
            }
            for r in assignment_rows
        ]

        return render_template(
            "rezepte_zuordnung.html",
            batch=batch,
            pages=pages,
            customers=customers,
            months=months,
            assignments=assignments,
            invoice_map=invoice_map,
        )

    @app.route("/rezepte/vorschau")
    def rezepte_vorschau():
        """Reine Anzeige (kein Versand!): zeigt die zugeordneten Rezepte eines
        Kunden fuer einen Monat als PDF im Browser.
        mode=brief -> 4-auf-1 (A4, wie im LetterXpress-Brief),
        mode=email -> Einzelseiten (wie im E-Mail-Anhang).
        """
        customer = (request.args.get("customer") or "").strip()
        month = (request.args.get("month") or "").strip()
        mode = (request.args.get("mode") or "brief").strip()
        if not customer or not _valid_month(month):
            abort(400)
        conn = _connect()
        try:
            base = prescription_basename(month)  # z.B. Rezepte_2026_06
            if mode == "email":
                data, n = build_prescriptions_simple_bytes(conn, customer, month)
                fname = f"{base}.pdf"
            else:
                data, n = build_prescriptions_4up_bytes(conn, customer, month)
                fname = f"{base}_4auf1.pdf"
        finally:
            conn.close()
        if not data:
            abort(404)
        return Response(
            data,
            mimetype="application/pdf",
            headers={
                "Content-Disposition": f"inline; filename={fname}",
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            },
        )

    @app.route("/rezepte/page/<int:page_id>/pdf")
    def rezepte_page_pdf(page_id: int):
        conn = _connect()
        try:
            row = conn.execute(
                "SELECT pdf_path, rotation FROM rezept_page WHERE id = ?", (page_id,)
            ).fetchone()
        finally:
            conn.close()
        if not row:
            abort(404)
        abs_path = (get_data_dir() / row["pdf_path"]).resolve()
        # Pfad-Schutz
        try:
            abs_path.relative_to(get_data_dir().resolve())
        except ValueError:
            abort(404)
        if not abs_path.exists():
            abort(404)
        try:
            data = rotated_page_bytes(abs_path, row["rotation"])
        except Exception as exc:
            logging.error("Fehler beim Rendern der Rezeptseite %s: %s", abs_path, exc)
            abort(500)
        return Response(
            data,
            mimetype="application/pdf",
            headers={
                "Content-Disposition": "inline; filename=rezept.pdf",
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            },
        )

    @app.route("/rezepte/page/<int:page_id>/rotate", methods=["POST"])
    def rezepte_page_rotate(page_id: int):
        payload = request.get_json(silent=True) or {}
        direction = payload.get("direction", "right")
        delta = -90 if direction == "left" else 90
        conn = _connect()
        try:
            row = conn.execute(
                "SELECT rotation FROM rezept_page WHERE id = ?", (page_id,)
            ).fetchone()
            if not row:
                abort(404)
            new_rotation = _normalize_rotation(row["rotation"] + delta)
            conn.execute(
                "UPDATE rezept_page SET rotation = ?, updated_at = datetime('now','localtime') WHERE id = ?",
                (new_rotation, page_id),
            )
            conn.commit()
        finally:
            conn.close()
        return jsonify({"ok": True, "rotation": new_rotation})

    @app.route("/rezepte/page/<int:page_id>/assign", methods=["POST"])
    def rezepte_page_assign(page_id: int):
        payload = request.get_json(silent=True) or {}

        set_customer = "customer_name" in payload
        set_month = "billing_month" in payload
        set_invoice = "invoice_id" in payload

        new_customer = None
        if set_customer:
            new_customer = (payload.get("customer_name") or "").strip() or None

        conn = _connect()
        try:
            row = conn.execute(
                "SELECT id FROM rezept_page WHERE id = ?", (page_id,)
            ).fetchone()
            if not row:
                abort(404)

            # Nur die gesendeten Felder aktualisieren (Teil-Update).
            sets, params = [], []
            if set_customer:
                sets.append("customer_name = ?")
                params.append(new_customer)
            if set_month:
                bm = (payload.get("billing_month") or "").strip()
                if not _valid_month(bm):
                    return jsonify({"ok": False, "error": "ungueltiger Monat"}), 400
                sets.append("billing_month = ?")
                params.append(bm)
            if set_invoice:
                iv = payload.get("invoice_id")
                iv = int(iv) if iv not in (None, "", "null") else None
                sets.append("invoice_id = ?")
                params.append(iv)

            # Bei Kundenwechsel ohne explizite Rechnung: letzte Rechnung vorbelegen
            # (bzw. Rechnung leeren, wenn der Kunde entfernt wird).
            if set_customer and not set_invoice:
                default_iv = None
                if new_customer:
                    latest = conn.execute(
                        "SELECT id FROM invoices WHERE customer_name = ? "
                        "ORDER BY created_at DESC, invoice_date DESC, id DESC LIMIT 1",
                        (new_customer,),
                    ).fetchone()
                    default_iv = latest["id"] if latest else None
                sets.append("invoice_id = ?")
                params.append(default_iv)

            if not sets:
                return jsonify({"ok": False, "error": "nichts zu aendern"}), 400

            params.append(page_id)
            conn.execute(
                f"UPDATE rezept_page SET {', '.join(sets)}, "
                "updated_at = datetime('now','localtime') WHERE id = ?",
                params,
            )
            conn.commit()
            updated = conn.execute(
                "SELECT customer_name, billing_month, invoice_id FROM rezept_page WHERE id = ?",
                (page_id,),
            ).fetchone()
        finally:
            conn.close()
        return jsonify(
            {
                "ok": True,
                "customer_name": updated["customer_name"],
                "billing_month": updated["billing_month"],
                "invoice_id": updated["invoice_id"],
            }
        )

    @app.route("/rezepte/api/customer-invoices")
    def rezepte_customer_invoices():
        """Rechnungen eines Kunden fuer das Zuordnungs-Pulldown (neueste zuerst)."""
        customer = (request.args.get("customer") or "").strip()
        if not customer:
            return jsonify({"invoices": []})
        conn = _connect()
        try:
            rows = fetch_customer_invoices(conn, customer)
        finally:
            conn.close()
        return jsonify({"invoices": [_invoice_option(r) for r in rows]})

    @app.route("/rezepte/<int:batch_id>/delete", methods=["POST"])
    def rezepte_delete(batch_id: int):
        import shutil

        conn = _connect()
        try:
            batch = conn.execute(
                "SELECT billing_month FROM rezept_batch WHERE id = ?", (batch_id,)
            ).fetchone()
            if not batch:
                abort(404)
            conn.execute("DELETE FROM rezept_page WHERE batch_id = ?", (batch_id,))
            conn.execute("DELETE FROM rezept_batch WHERE id = ?", (batch_id,))
            conn.commit()
        finally:
            conn.close()

        # Dateien entfernen (best effort)
        batch_dir = get_data_dir() / REZEPTE_DIRNAME / batch["billing_month"] / f"batch_{batch_id}"
        try:
            if batch_dir.exists():
                shutil.rmtree(batch_dir)
        except Exception as exc:
            logging.warning("Konnte Rezept-Ordner nicht loeschen %s: %s", batch_dir, exc)

        flash("Rezept-Stapel geloescht.", "success")
        return redirect(url_for("rezepte"))
