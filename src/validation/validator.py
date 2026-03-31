"""
Validation  [AI Generated]
===========================

Validates incoming SP data immediately after extraction, before hash
computation or Parquet writes.

Invalid records are logged and skipped — they do not abort the pipeline.
Returns only the valid rows from each SP result set.
"""

import logging
from typing import Optional

from src.utils.logging_config import TenantLoggerAdapter, get_logger

logger = get_logger(__name__)


def _log_invalid(
    adapter: logging.LoggerAdapter,
    enquiry_no: Optional[int],
    reason: str,
    source: str,
) -> None:
    adapter.warning(
        f"Skipping invalid record from {source}",
        extra={"enquiry_no": enquiry_no, "reason": reason},
    )


def validate_sp1_rows(
    rows: list[dict],
    adapter: logging.LoggerAdapter,
) -> list[dict]:
    """
    Validate SP1 (enquiry lifecycle) rows.

    Rules:
        - EnquiryNo must be > 0
        - BookingDate must not be null (null = unconverted enquiry, excluded)
        - BookingValue must be >= 0

    Args:
        rows: Raw SP1 rows.
        adapter: Tenant-aware logger.

    Returns:
        List of valid rows only.
    """
    valid: list[dict] = []
    for row in rows:
        enquiry_no = row.get("EnquiryNo")

        if not enquiry_no or enquiry_no <= 0:
            _log_invalid(adapter, enquiry_no, "EnquiryNo must be > 0", "SP1")
            continue

        if row.get("BookingDate") is None:
            # Unconverted enquiries are excluded — not an error, just filtered
            continue

        if row.get("BookingValue") is not None and row["BookingValue"] < 0:
            _log_invalid(adapter, enquiry_no, "BookingValue must be >= 0", "SP1")
            continue

        valid.append(row)

    return valid


def validate_sp2_rows(
    rows: list[dict],
    valid_enquiry_nos: set[int],
    adapter: logging.LoggerAdapter,
) -> list[dict]:
    """
    Validate SP2 (country-level) rows.

    Rules:
        - EnquiryNo must appear in the validated SP1 set
        - No duplicate (EnquiryNo, CountryCode) pairs

    Args:
        rows: Raw SP2 rows.
        valid_enquiry_nos: EnquiryNos that passed SP1 validation.
        adapter: Tenant-aware logger.

    Returns:
        List of valid rows with duplicates removed (first occurrence wins).
    """
    valid: list[dict] = []
    seen: set[tuple] = set()

    for row in rows:
        enquiry_no = row.get("EnquiryNo")
        country_code = row.get("CountryCode")
        key = (enquiry_no, country_code)

        if enquiry_no not in valid_enquiry_nos:
            continue  # SP1 filtered it out

        if key in seen:
            _log_invalid(
                adapter, enquiry_no,
                f"Duplicate (EnquiryNo, CountryCode)=({enquiry_no}, {country_code})", "SP2"
            )
            continue

        seen.add(key)
        valid.append(row)

    return valid


def validate_sp3_rows(
    rows: list[dict],
    valid_enquiry_nos: set[int],
    adapter: logging.LoggerAdapter,
) -> list[dict]:
    """
    Validate SP3 (tag-level) rows.

    Rules:
        - EnquiryNo must appear in the validated SP1 set
        - No duplicate (EnquiryNo, TagName) pairs

    Args:
        rows: Raw SP3 rows.
        valid_enquiry_nos: EnquiryNos that passed SP1 validation.
        adapter: Tenant-aware logger.

    Returns:
        List of valid rows with duplicates removed (first occurrence wins).
    """
    valid: list[dict] = []
    seen: set[tuple] = set()

    for row in rows:
        enquiry_no = row.get("EnquiryNo")
        tag_name = row.get("TagName")
        key = (enquiry_no, tag_name)

        if enquiry_no not in valid_enquiry_nos:
            continue

        if key in seen:
            _log_invalid(
                adapter, enquiry_no,
                f"Duplicate (EnquiryNo, TagName)=({enquiry_no}, {tag_name})", "SP3"
            )
            continue

        seen.add(key)
        valid.append(row)

    return valid


def validate_all(
    sp1_rows: list[dict],
    sp2_rows: list[dict],
    sp3_rows: list[dict],
    adapter: logging.LoggerAdapter,
) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Validate all three SP result sets and return only valid rows.

    Validation is applied in SP1 → SP2 → SP3 order. SP2 and SP3 are filtered
    to only include EnquiryNos that survived SP1 validation.

    Args:
        sp1_rows: Raw SP1 rows.
        sp2_rows: Raw SP2 rows.
        sp3_rows: Raw SP3 rows.
        adapter: Tenant-aware logger.

    Returns:
        Tuple of (valid_sp1, valid_sp2, valid_sp3).
    """
    valid_sp1 = validate_sp1_rows(sp1_rows, adapter)
    valid_enquiry_nos = {row["EnquiryNo"] for row in valid_sp1}

    valid_sp2 = validate_sp2_rows(sp2_rows, valid_enquiry_nos, adapter)
    valid_sp3 = validate_sp3_rows(sp3_rows, valid_enquiry_nos, adapter)

    adapter.info(
        "Validation complete",
        extra={
            "sp1_in": len(sp1_rows), "sp1_valid": len(valid_sp1),
            "sp2_in": len(sp2_rows), "sp2_valid": len(valid_sp2),
            "sp3_in": len(sp3_rows), "sp3_valid": len(valid_sp3),
        },
    )
    return valid_sp1, valid_sp2, valid_sp3
