"""Data access layer: invoice/reminder dataclasses and the read queries
used by the Flask routes.

Extracted verbatim from web_app.py. No behaviour change. fetch_invoices still
registers the LAST_WORD SQLite UDF (config.sql_last_word) itself.
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional, Tuple

from config import SORT_COLUMN_MAP, normalize_sort_params, sql_last_word
from invoice_tracker import init_db


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
    collective_filter: str = "all",
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

    Collective invoice filter (Sammelrechnung):
    - 'all': Show all invoices
    - 'in': Show only invoices in a collective invoice
    - 'not_in': Show only invoices not in a collective invoice

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

        # Apply hide_before_date filter (hide invoices older than customer's hide_before_date)
        sql += " AND (cd.hide_before_date IS NULL OR ist.invoice_date >= cd.hide_before_date)"

        # Apply collective invoice filter
        if collective_filter == "in":
            sql += " AND EXISTS (SELECT 1 FROM collective_invoice_items cii WHERE cii.invoice_id = ist.id)"
        elif collective_filter == "not_in":
            sql += " AND NOT EXISTS (SELECT 1 FROM collective_invoice_items cii WHERE cii.invoice_id = ist.id)"
        # If collective_filter == "all", don't add any filter (show all)

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
                cd.always_rx,
                cd.hide_before_date,
                cd.custom_name,
                cd.custom_street,
                cd.custom_city,
                COUNT(DISTINCT i.id) as invoice_count,
                SUM(i.amount_cents) as total_amount_cents
            FROM invoices i
            LEFT JOIN customer_details cd ON i.customer_name = cd.customer_name
            GROUP BY i.customer_name, i.customer_address, i.customer_street, i.customer_city, cd.salutation, cd.email, cd.notes, cd.never_remind, cd.bank_debit, cd.print_only, cd.always_rx, cd.hide_before_date, cd.custom_name, cd.custom_street, cd.custom_city
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
            "always_rx": row["always_rx"] or 0,
            "hide_before_date": row["hide_before_date"] or "",
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

        # Apply hide_before_date filter (hide invoices older than customer's hide_before_date)
        sql += " AND (cd.hide_before_date IS NULL OR ist.invoice_date >= cd.hide_before_date)"

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
