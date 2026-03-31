"""
Fact Pipeline  [Manual]
========================

Core incremental load logic for fact_bookingcountries and fact_bookingtags.

Implements the five-case hash-based change detection:
  A — new booking (not in tracking table)
  B — no change (hash matches)
  C — changed, same partition (hash differs, BookingMonth unchanged)
  D — changed, partition move (hash differs, BookingMonth changed)
  E — hard delete (BookingValue=0 and NoOfPax=0, found in tracking)

Design rules:
- EnquiryParquetFile is always written AFTER the Parquet write succeeds.
- All Parquet writes are atomic (temp file → rename, handled by parquet_writer).
- The SPs guarantee: if any data for an EnquiryNo changes, all 3 SPs return it.
- NULL values are represented as the literal "NULL" in hash concatenation.
- SP column order is fixed (from mssql_connector) for hash determinism.
"""

import hashlib
import logging
from datetime import date, datetime
from typing import Optional

import polars as pl
import pyodbc

from src.connectors.db_connector import SP1_COLUMNS, SP2_COLUMNS, SP3_COLUMNS
from src.io.parquet_writer import (
    delete_enquiry_from_partition,
    upsert_enquiry_in_partition,
)
from src.tracking.tracking_store import (
    TrackingRecord,
    bulk_lookup_enquiries,
    delete_enquiry,
    insert_enquiry,
    update_enquiry,
)
from src.utils.logging_config import TenantLoggerAdapter, get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Financial year helpers
# ---------------------------------------------------------------------------

def _booking_fy(booking_date: date) -> str:
    """Return the financial year label (Aug–Jul) for a given date."""
    if booking_date.month >= 8:
        return f"{booking_date.year}-{booking_date.year + 1}"
    return f"{booking_date.year - 1}-{booking_date.year}"


def _departure_fy(departure_date: date) -> str:
    """Return the financial year label (Aug–Jul) for a given date."""
    return _booking_fy(departure_date)


# ---------------------------------------------------------------------------
# Hash computation
# ---------------------------------------------------------------------------

def _val(v) -> str:
    """Convert a value to its pipe-delimited string representation."""
    return "NULL" if v is None else str(v)


def compute_data_hash(
    sp1_row: dict,
    sp2_rows: list[dict],
    sp3_rows: list[dict],
) -> str:
    """
    Compute the SHA-256 hash covering all SP values for an EnquiryNo.

    Includes ALL columns from all three SPs (even those excluded from the
    fact table) so any upstream change is detected.

    Hash structure:
        SP1 columns (fixed order, pipe-delimited)
        || (double pipe separator between SPs)
        SP2 rows sorted ascending by CountryCode (all columns, pipe-delimited)
        ||
        SP3 rows sorted ascending by TagName (all columns, pipe-delimited)

    NULL values are represented as the literal string "NULL".

    Args:
        sp1_row: Single SP1 row dict.
        sp2_rows: List of SP2 row dicts for this EnquiryNo.
        sp3_rows: List of SP3 row dicts for this EnquiryNo.

    Returns:
        SHA-256 hex digest string (64 characters).
    """
    sp1_part = "|".join(_val(sp1_row.get(col)) for col in SP1_COLUMNS)

    sp2_sorted = sorted(sp2_rows, key=lambda r: _val(r.get("CountryCode")))
    sp2_part = "||".join(
        "|".join(_val(r.get(col)) for col in SP2_COLUMNS)
        for r in sp2_sorted
    )

    sp3_sorted = sorted(sp3_rows, key=lambda r: _val(r.get("TagName")))
    sp3_part = "||".join(
        "|".join(_val(r.get(col)) for col in SP3_COLUMNS)
        for r in sp3_sorted
    )

    combined = f"{sp1_part}|||{sp2_part}|||{sp3_part}"
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Fact row builders
# ---------------------------------------------------------------------------

def _build_countries_rows(sp1_row: dict, sp2_rows: list[dict]) -> pl.DataFrame:
    """
    Build fact_bookingcountries rows from SP1 + SP2 data for one EnquiryNo.

    Grain: EnquiryNo × CountryCode
    Measures: BookingValue, NoOfPax (from SP2 — country-distributed values)
    Pass-through from SP1: BookingDate, DepartureDate, TourType, TourGenericCode
    Pass-through from SP2: CountryName
    Derived: BookingMonth, BookingFY, DepartureMonth, DepartureFY
    """
    booking_date: date = sp1_row["BookingDate"]
    departure_date: date = sp1_row["DepartureDate"]

    booking_month = booking_date.strftime("%Y-%m")
    departure_month = departure_date.strftime("%Y-%m")
    booking_fy = _booking_fy(booking_date)
    departure_fy = _departure_fy(departure_date)

    rows = []
    for sp2 in sp2_rows:
        rows.append({
            "EnquiryNo": sp1_row["EnquiryNo"],
            "CountryCode": sp2["CountryCode"],
            "CountryName": sp2["CountryName"],
            "BookingValue": sp2["BookingValue"],
            "NoOfPax": sp2["NoOfPax"],
            "BookingDate": booking_date,
            "DepartureDate": departure_date,
            "TourType": sp1_row["TourType"],
            "TourGenericCode": sp1_row["TourGenericCode"],
            "BookingMonth": booking_month,
            "BookingFY": booking_fy,
            "DepartureMonth": departure_month,
            "DepartureFY": departure_fy,
        })

    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows)


def _build_tags_rows(sp1_row: dict, sp3_rows: list[dict]) -> pl.DataFrame:
    """
    Build fact_bookingtags rows from SP1 + SP3 data for one EnquiryNo.

    Grain: EnquiryNo × TagName
    Measures: BookingValue, NoOfPax (from SP3 — full enquiry-level values)
    Pass-through from SP1: BookingDate, DepartureDate, TourType, TourGenericCode
    Derived: BookingMonth, BookingFY, DepartureMonth, DepartureFY
    """
    if not sp3_rows:
        return pl.DataFrame()

    booking_date: date = sp1_row["BookingDate"]
    departure_date: date = sp1_row["DepartureDate"]

    booking_month = booking_date.strftime("%Y-%m")
    departure_month = departure_date.strftime("%Y-%m")
    booking_fy = _booking_fy(booking_date)
    departure_fy = _departure_fy(departure_date)

    rows = []
    for sp3 in sp3_rows:
        rows.append({
            "EnquiryNo": sp1_row["EnquiryNo"],
            "TagName": sp3["TagName"],
            "BookingValue": sp3["BookingValue"],
            "NoOfPax": sp3["NoOfPax"],
            "BookingDate": booking_date,
            "DepartureDate": departure_date,
            "TourType": sp1_row["TourType"],
            "TourGenericCode": sp1_row["TourGenericCode"],
            "BookingMonth": booking_month,
            "BookingFY": booking_fy,
            "DepartureMonth": departure_month,
            "DepartureFY": departure_fy,
        })

    return pl.DataFrame(rows)


# ---------------------------------------------------------------------------
# Case handlers
# ---------------------------------------------------------------------------

def _parquet_file_for(booking_month: str) -> str:
    """Build the relative ParquetFile path for EnquiryParquetFile tracking."""
    return f"fact_bookingcountries/BookingMonth={booking_month}"


def _tags_path_from_countries_path(countries_parquet_file: str) -> str:
    """Derive the fact_bookingtags relative path from the stored countries path."""
    return countries_parquet_file.replace(
        "fact_bookingcountries", "fact_bookingtags", 1
    )


def _handle_case_a(
    enquiry_no: int,
    sp1_row: dict,
    sp2_rows: list[dict],
    sp3_rows: list[dict],
    data_hash: str,
    data_root: str,
    tenant_id: str,
    dhruvlog_conn: pyodbc.Connection,
    adapter: logging.LoggerAdapter,
) -> None:
    """Case A: New booking — write to both partitions, then INSERT tracking."""
    booking_month = sp1_row["BookingDate"].strftime("%Y-%m")

    countries_df = _build_countries_rows(sp1_row, sp2_rows)
    tags_df = _build_tags_rows(sp1_row, sp3_rows)

    upsert_enquiry_in_partition(
        data_root, tenant_id, "fact_bookingcountries", booking_month, countries_df, enquiry_no
    )
    upsert_enquiry_in_partition(
        data_root, tenant_id, "fact_bookingtags", booking_month, tags_df, enquiry_no
    )

    record = TrackingRecord(
        tenant_id=tenant_id,
        enquiry_no=enquiry_no,
        parquet_file=_parquet_file_for(booking_month),
        data_hash=data_hash,
    )
    insert_enquiry(dhruvlog_conn, record)
    adapter.debug(f"Case A: inserted EnquiryNo={enquiry_no} BookingMonth={booking_month}")


def _handle_case_c(
    enquiry_no: int,
    sp1_row: dict,
    sp2_rows: list[dict],
    sp3_rows: list[dict],
    data_hash: str,
    booking_month: str,
    data_root: str,
    tenant_id: str,
    dhruvlog_conn: pyodbc.Connection,
    adapter: logging.LoggerAdapter,
) -> None:
    """Case C: Changed, same partition — remove+rewrite both, UPDATE hash."""
    countries_df = _build_countries_rows(sp1_row, sp2_rows)
    tags_df = _build_tags_rows(sp1_row, sp3_rows)

    upsert_enquiry_in_partition(
        data_root, tenant_id, "fact_bookingcountries", booking_month, countries_df, enquiry_no
    )
    upsert_enquiry_in_partition(
        data_root, tenant_id, "fact_bookingtags", booking_month, tags_df, enquiry_no
    )

    update_enquiry(dhruvlog_conn, tenant_id, enquiry_no, parquet_file=None, data_hash=data_hash)
    adapter.debug(f"Case C: updated EnquiryNo={enquiry_no} BookingMonth={booking_month}")


def _handle_case_d(
    enquiry_no: int,
    sp1_row: dict,
    sp2_rows: list[dict],
    sp3_rows: list[dict],
    data_hash: str,
    old_parquet_file: str,
    data_root: str,
    tenant_id: str,
    dhruvlog_conn: pyodbc.Connection,
    adapter: logging.LoggerAdapter,
) -> None:
    """Case D: Partition move — remove from old, write to new, UPDATE tracking."""
    old_booking_month = old_parquet_file.split("BookingMonth=")[-1]
    new_booking_month = sp1_row["BookingDate"].strftime("%Y-%m")

    old_tags_file = _tags_path_from_countries_path(old_parquet_file)
    old_tags_month = old_tags_file.split("BookingMonth=")[-1]

    # Remove from old partitions (self-healing: silently ignores missing rows)
    delete_enquiry_from_partition(
        data_root, tenant_id, "fact_bookingcountries", old_booking_month, enquiry_no
    )
    delete_enquiry_from_partition(
        data_root, tenant_id, "fact_bookingtags", old_tags_month, enquiry_no
    )

    # Write to new partitions
    countries_df = _build_countries_rows(sp1_row, sp2_rows)
    tags_df = _build_tags_rows(sp1_row, sp3_rows)

    upsert_enquiry_in_partition(
        data_root, tenant_id, "fact_bookingcountries", new_booking_month, countries_df, enquiry_no
    )
    upsert_enquiry_in_partition(
        data_root, tenant_id, "fact_bookingtags", new_booking_month, tags_df, enquiry_no
    )

    new_parquet_file = _parquet_file_for(new_booking_month)
    update_enquiry(dhruvlog_conn, tenant_id, enquiry_no, parquet_file=new_parquet_file, data_hash=data_hash)
    adapter.info(
        f"Case D: partition move EnquiryNo={enquiry_no} "
        f"{old_booking_month} → {new_booking_month}"
    )


def _handle_case_e(
    enquiry_no: int,
    stored_parquet_file: str,
    data_root: str,
    tenant_id: str,
    dhruvlog_conn: pyodbc.Connection,
    adapter: logging.LoggerAdapter,
) -> None:
    """Case E: Hard delete — remove from both fact files, DELETE tracking row."""
    booking_month = stored_parquet_file.split("BookingMonth=")[-1]
    tags_file = _tags_path_from_countries_path(stored_parquet_file)
    tags_month = tags_file.split("BookingMonth=")[-1]

    delete_enquiry_from_partition(
        data_root, tenant_id, "fact_bookingcountries", booking_month, enquiry_no
    )
    delete_enquiry_from_partition(
        data_root, tenant_id, "fact_bookingtags", tags_month, enquiry_no
    )

    delete_enquiry(dhruvlog_conn, tenant_id, enquiry_no)
    adapter.info(f"Case E: hard deleted EnquiryNo={enquiry_no} from BookingMonth={booking_month}")


# ---------------------------------------------------------------------------
# Main pipeline entry point
# ---------------------------------------------------------------------------

class FactPipeline:
    """
    Incremental fact load pipeline for a single tenant.

    Processes each EnquiryNo returned by the SPs through the five-case
    hash-based change detection logic.
    """

    def __init__(
        self,
        tenant_id: str,
        data_root: str,
        adapter: logging.LoggerAdapter,
    ) -> None:
        self.tenant_id = tenant_id
        self.data_root = data_root
        self.adapter = adapter

    def run(
        self,
        sp1_rows: list[dict],
        sp2_rows: list[dict],
        sp3_rows: list[dict],
        dhruvlog_conn: pyodbc.Connection,
    ) -> dict:
        """
        Process validated SP rows through Cases A–E.

        Args:
            sp1_rows: Validated SP1 rows.
            sp2_rows: Validated SP2 rows.
            sp3_rows: Validated SP3 rows.
            dhruvlog_conn: dhruvlog database connection.

        Returns:
            Summary dict with counts per case.
        """
        # Index SP2 and SP3 by EnquiryNo for O(1) lookup
        sp2_by_enquiry: dict[int, list[dict]] = {}
        for row in sp2_rows:
            sp2_by_enquiry.setdefault(row["EnquiryNo"], []).append(row)

        sp3_by_enquiry: dict[int, list[dict]] = {}
        for row in sp3_rows:
            sp3_by_enquiry.setdefault(row["EnquiryNo"], []).append(row)

        sp1_by_enquiry: dict[int, dict] = {row["EnquiryNo"]: row for row in sp1_rows}
        all_enquiry_nos = list(sp1_by_enquiry.keys())

        # Bulk load tracking records (single DB round-trip)
        tracking = bulk_lookup_enquiries(dhruvlog_conn, self.tenant_id, all_enquiry_nos)

        counts = {"A": 0, "B": 0, "C": 0, "D": 0, "E": 0, "skipped": 0, "errors": 0}

        for enquiry_no, sp1_row in sp1_by_enquiry.items():
            try:
                self._process_enquiry(
                    enquiry_no=enquiry_no,
                    sp1_row=sp1_row,
                    sp2_rows=sp2_by_enquiry.get(enquiry_no, []),
                    sp3_rows=sp3_by_enquiry.get(enquiry_no, []),
                    tracking=tracking,
                    dhruvlog_conn=dhruvlog_conn,
                    counts=counts,
                )
            except Exception as e:
                self.adapter.error(
                    f"Error processing EnquiryNo={enquiry_no}: {e}",
                    exc_info=True,
                    extra={"enquiry_no": enquiry_no},
                )
                counts["errors"] += 1

        self.adapter.info("Fact pipeline complete", extra={"counts": counts})
        return counts

    def _process_enquiry(
        self,
        enquiry_no: int,
        sp1_row: dict,
        sp2_rows: list[dict],
        sp3_rows: list[dict],
        tracking: dict[int, TrackingRecord],
        dhruvlog_conn: pyodbc.Connection,
        counts: dict,
    ) -> None:
        """Process a single EnquiryNo through the five-case logic."""

        # --- Pre-filter: zero-value records ---
        is_zero = (
            (sp1_row.get("BookingValue") or 0) == 0
            and (sp1_row.get("NoOfPax") or 0) == 0
        )

        if is_zero:
            if enquiry_no not in tracking:
                counts["skipped"] += 1
                return  # Never seen — ignore entirely
            # Found in tracking → Case E (hard delete)
            _handle_case_e(
                enquiry_no=enquiry_no,
                stored_parquet_file=tracking[enquiry_no].parquet_file,
                data_root=self.data_root,
                tenant_id=self.tenant_id,
                dhruvlog_conn=dhruvlog_conn,
                adapter=self.adapter,
            )
            counts["E"] += 1
            return

        # --- Compute hash ---
        data_hash = compute_data_hash(sp1_row, sp2_rows, sp3_rows)

        # --- Case lookup ---
        if enquiry_no not in tracking:
            # Case A: New booking
            _handle_case_a(
                enquiry_no=enquiry_no,
                sp1_row=sp1_row,
                sp2_rows=sp2_rows,
                sp3_rows=sp3_rows,
                data_hash=data_hash,
                data_root=self.data_root,
                tenant_id=self.tenant_id,
                dhruvlog_conn=dhruvlog_conn,
                adapter=self.adapter,
            )
            counts["A"] += 1
            return

        record = tracking[enquiry_no]

        if record.data_hash == data_hash:
            # Case B: No change
            counts["B"] += 1
            return

        # Hash has changed — determine C or D
        current_booking_month = sp1_row["BookingDate"].strftime("%Y-%m")
        stored_booking_month = record.parquet_file.split("BookingMonth=")[-1]

        if stored_booking_month == current_booking_month:
            # Case C: Same partition
            _handle_case_c(
                enquiry_no=enquiry_no,
                sp1_row=sp1_row,
                sp2_rows=sp2_rows,
                sp3_rows=sp3_rows,
                data_hash=data_hash,
                booking_month=current_booking_month,
                data_root=self.data_root,
                tenant_id=self.tenant_id,
                dhruvlog_conn=dhruvlog_conn,
                adapter=self.adapter,
            )
            counts["C"] += 1
        else:
            # Case D: Partition move
            _handle_case_d(
                enquiry_no=enquiry_no,
                sp1_row=sp1_row,
                sp2_rows=sp2_rows,
                sp3_rows=sp3_rows,
                data_hash=data_hash,
                old_parquet_file=record.parquet_file,
                data_root=self.data_root,
                tenant_id=self.tenant_id,
                dhruvlog_conn=dhruvlog_conn,
                adapter=self.adapter,
            )
            counts["D"] += 1
