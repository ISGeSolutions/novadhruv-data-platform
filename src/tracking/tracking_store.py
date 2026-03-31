"""
Tracking Store  [AI Generated]
================================

All reads and writes to dhruvlog.dbo.LastRunTracking and
dhruvlog.dbo.EnquiryParquetFile.

TenantId is always included in queries to maintain tenant isolation.
All operations use the dhruvlog connection.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import pyodbc

from src.utils.logging_config import get_logger

logger = get_logger(__name__)

FIRST_RUN_DATE = datetime(1900, 1, 1)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class TrackingRecord:
    """Represents a row in dhruvlog.dbo.EnquiryParquetFile."""
    tenant_id: str
    enquiry_no: int
    parquet_file: str   # relative path, e.g. fact_bookingcountries/BookingMonth=2025-11
    data_hash: str      # SHA-256 hex string


# ---------------------------------------------------------------------------
# LastRunTracking operations
# ---------------------------------------------------------------------------

def get_last_run_date(
    conn: pyodbc.Connection,
    tenant_id: str,
    process_name: str,
) -> datetime:
    """
    Return the last successful run datetime for the given tenant and process.

    Returns FIRST_RUN_DATE (1900-01-01) if no prior record exists, so the
    SPs return the full dataset on first run.

    Args:
        conn: dhruvlog database connection.
        tenant_id: Tenant identifier.
        process_name: 'YOYFactPipeline', 'YOYSnapshotPipeline', or 'YOYReconPipeline'.

    Returns:
        datetime of the last successful run, or 1900-01-01 on first run.
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT TOP 1 LastRunDateTime
        FROM dbo.LastRunTracking
        WHERE TenantId = ? AND ProcessName = ?
        ORDER BY LastRunDateTime DESC
        """,
        tenant_id,
        process_name,
    )
    row = cursor.fetchone()
    if row:
        return row[0]
    logger.info(
        f"No prior run found for tenant={tenant_id} process={process_name}; "
        "using FIRST_RUN_DATE"
    )
    return FIRST_RUN_DATE


def record_successful_run(
    conn: pyodbc.Connection,
    tenant_id: str,
    process_name: str,
    run_datetime: datetime,
) -> None:
    """
    Insert a new row into LastRunTracking to record a successful pipeline run.

    A new row is always inserted (append-only). The most recent row is used
    on the next call to get_last_run_date.

    Args:
        conn: dhruvlog database connection.
        tenant_id: Tenant identifier.
        process_name: Pipeline process name.
        run_datetime: Timestamp of the successful run.
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO dbo.LastRunTracking (TenantId, ProcessName, LastRunDateTime)
        VALUES (?, ?, ?)
        """,
        tenant_id,
        process_name,
        run_datetime,
    )
    conn.commit()
    logger.debug(
        f"Recorded successful run: tenant={tenant_id} process={process_name} "
        f"at={run_datetime.isoformat()}"
    )


# ---------------------------------------------------------------------------
# EnquiryParquetFile — bulk lookup
# ---------------------------------------------------------------------------

def bulk_lookup_enquiries(
    conn: pyodbc.Connection,
    tenant_id: str,
    enquiry_nos: list[int],
) -> dict[int, TrackingRecord]:
    """
    Fetch all EnquiryParquetFile rows for the given tenant and EnquiryNos.

    Batches the lookup into chunks of 1000 to avoid SQL parameter limits.

    Args:
        conn: dhruvlog database connection.
        tenant_id: Tenant identifier.
        enquiry_nos: List of EnquiryNos to look up.

    Returns:
        Dict mapping EnquiryNo → TrackingRecord (only for EnquiryNos found).
    """
    result: dict[int, TrackingRecord] = {}
    if not enquiry_nos:
        return result

    chunk_size = 1000
    for i in range(0, len(enquiry_nos), chunk_size):
        chunk = enquiry_nos[i : i + chunk_size]
        placeholders = ",".join("?" * len(chunk))
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT EnquiryNo, ParquetFile, DataHash
            FROM dbo.EnquiryParquetFile
            WHERE TenantId = ? AND EnquiryNo IN ({placeholders})
            """,
            tenant_id,
            *chunk,
        )
        for row in cursor.fetchall():
            result[row[0]] = TrackingRecord(
                tenant_id=tenant_id,
                enquiry_no=row[0],
                parquet_file=row[1],
                data_hash=row[2],
            )

    return result


# ---------------------------------------------------------------------------
# EnquiryParquetFile — single-row operations
# ---------------------------------------------------------------------------

def insert_enquiry(
    conn: pyodbc.Connection,
    record: TrackingRecord,
) -> None:
    """
    Insert a new tracking record after a successful Parquet write (Case A).

    Args:
        conn: dhruvlog database connection.
        record: TrackingRecord to insert.
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO dbo.EnquiryParquetFile (TenantId, EnquiryNo, ParquetFile, DataHash)
        VALUES (?, ?, ?, ?)
        """,
        record.tenant_id,
        record.enquiry_no,
        record.parquet_file,
        record.data_hash,
    )
    conn.commit()


def update_enquiry(
    conn: pyodbc.Connection,
    tenant_id: str,
    enquiry_no: int,
    parquet_file: Optional[str],
    data_hash: str,
) -> None:
    """
    Update hash (and optionally the partition path) for an existing record.

    Used for Cases C (hash-only update) and D (partition move + hash update).

    Args:
        conn: dhruvlog database connection.
        tenant_id: Tenant identifier.
        enquiry_no: EnquiryNo to update.
        parquet_file: New partition path (None = keep existing, Case C).
        data_hash: New SHA-256 hash.
    """
    cursor = conn.cursor()
    if parquet_file is not None:
        cursor.execute(
            """
            UPDATE dbo.EnquiryParquetFile
            SET ParquetFile = ?, DataHash = ?, UpdatedOn = GETUTCDATE()
            WHERE TenantId = ? AND EnquiryNo = ?
            """,
            parquet_file,
            data_hash,
            tenant_id,
            enquiry_no,
        )
    else:
        cursor.execute(
            """
            UPDATE dbo.EnquiryParquetFile
            SET DataHash = ?, UpdatedOn = GETUTCDATE()
            WHERE TenantId = ? AND EnquiryNo = ?
            """,
            data_hash,
            tenant_id,
            enquiry_no,
        )
    conn.commit()


def delete_enquiry(
    conn: pyodbc.Connection,
    tenant_id: str,
    enquiry_no: int,
) -> None:
    """
    Delete a tracking record (Case E — hard delete).

    Args:
        conn: dhruvlog database connection.
        tenant_id: Tenant identifier.
        enquiry_no: EnquiryNo to delete.
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        DELETE FROM dbo.EnquiryParquetFile
        WHERE TenantId = ? AND EnquiryNo = ?
        """,
        tenant_id,
        enquiry_no,
    )
    conn.commit()
