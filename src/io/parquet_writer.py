"""
Parquet Writer  [AI Generated]
================================

Atomic partition read-modify-write helpers for fact and snapshot tables.

All writes use a temp-file-then-rename pattern to prevent partial/corrupt
partitions from being visible to readers during an in-progress write.
"""

import os
import uuid
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from src.utils.logging_config import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _atomic_write(table: pa.Table, partition_path: Path) -> None:
    """
    Write a PyArrow table to a Parquet file atomically.

    Writes to a temp file in the same directory, then renames to the final
    path. This ensures readers never see a partial file.

    Args:
        table: PyArrow table to write.
        partition_path: Final destination path (directory).
    """
    partition_path.mkdir(parents=True, exist_ok=True)
    final_file = partition_path / "data.parquet"
    tmp_file = partition_path / f"_tmp_{uuid.uuid4().hex}.parquet"

    try:
        pq.write_table(table, str(tmp_file), compression="snappy")
        os.replace(str(tmp_file), str(final_file))
        logger.debug(f"Atomic write complete: {final_file}")
    except Exception:
        if tmp_file.exists():
            tmp_file.unlink()
        raise


def _partition_path(data_root: str, tenant_id: str, table_name: str, booking_month: str) -> Path:
    """Build the full partition path for a fact table partition."""
    return Path(data_root) / f"tenant_id={tenant_id}" / table_name / f"BookingMonth={booking_month}"


def _snapshot_partition_path(
    data_root: str, tenant_id: str, table_name: str, snapshot_date: str
) -> Path:
    """Build the full partition path for a snapshot table partition."""
    return Path(data_root) / f"tenant_id={tenant_id}" / table_name / f"SnapshotDate={snapshot_date}"


# ---------------------------------------------------------------------------
# Fact partition write operations
# ---------------------------------------------------------------------------

def read_partition(
    data_root: str,
    tenant_id: str,
    table_name: str,
    booking_month: str,
) -> pl.DataFrame:
    """
    Read an existing fact partition into a Polars DataFrame.

    Returns an empty DataFrame (with no schema) if the partition does not exist.

    Args:
        data_root: Root data directory.
        tenant_id: Tenant identifier.
        table_name: 'fact_bookingcountries' or 'fact_bookingtags'.
        booking_month: Partition key in YYYY-MM format.

    Returns:
        Polars DataFrame (may be empty).
    """
    parquet_file = _partition_path(data_root, tenant_id, table_name, booking_month) / "data.parquet"
    if not parquet_file.exists():
        logger.debug(f"Partition not found (returning empty): {parquet_file}")
        return pl.DataFrame()
    return pl.read_parquet(str(parquet_file))


def write_partition(
    df: pl.DataFrame,
    data_root: str,
    tenant_id: str,
    table_name: str,
    booking_month: str,
) -> None:
    """
    Atomically write a Polars DataFrame to a fact partition.

    Replaces the entire partition file. Callers must merge existing data
    before calling this function (use remove_enquiry_from_partition + append).

    Args:
        df: DataFrame to write.
        data_root: Root data directory.
        tenant_id: Tenant identifier.
        table_name: Fact table name.
        booking_month: BookingMonth partition key (YYYY-MM).
    """
    path = _partition_path(data_root, tenant_id, table_name, booking_month)
    _atomic_write(df.to_arrow(), path)
    logger.debug(f"Written partition: {path} ({len(df)} rows)")


def remove_enquiry_from_partition(
    df: pl.DataFrame,
    enquiry_no: int,
) -> pl.DataFrame:
    """
    Remove all rows for an EnquiryNo from a DataFrame.

    Returns the DataFrame unchanged if EnquiryNo is not present (self-healing
    for Case C/D sync gaps).

    Args:
        df: Existing partition DataFrame.
        enquiry_no: EnquiryNo to remove.

    Returns:
        Filtered DataFrame.
    """
    if df.is_empty():
        return df
    return df.filter(pl.col("EnquiryNo") != enquiry_no)


def upsert_enquiry_in_partition(
    data_root: str,
    tenant_id: str,
    table_name: str,
    booking_month: str,
    new_rows: pl.DataFrame,
    enquiry_no: int,
) -> None:
    """
    Remove existing rows for enquiry_no then append new_rows to the partition.

    Used for Cases C and D after moving to the new partition.

    Args:
        data_root: Root data directory.
        tenant_id: Tenant identifier.
        table_name: Fact table name.
        booking_month: Target partition key.
        new_rows: Rows to write for this enquiry.
        enquiry_no: EnquiryNo being updated.
    """
    existing = read_partition(data_root, tenant_id, table_name, booking_month)
    cleaned = remove_enquiry_from_partition(existing, enquiry_no)

    if new_rows.is_empty():
        merged = cleaned
    elif cleaned.is_empty():
        merged = new_rows
    else:
        merged = pl.concat([cleaned, new_rows], how="diagonal")

    write_partition(merged, data_root, tenant_id, table_name, booking_month)


def delete_enquiry_from_partition(
    data_root: str,
    tenant_id: str,
    table_name: str,
    booking_month: str,
    enquiry_no: int,
) -> None:
    """
    Remove all rows for an EnquiryNo from an existing partition and rewrite.

    Used for Case E (hard delete).

    Args:
        data_root: Root data directory.
        tenant_id: Tenant identifier.
        table_name: Fact table name.
        booking_month: Partition key.
        enquiry_no: EnquiryNo to delete.
    """
    existing = read_partition(data_root, tenant_id, table_name, booking_month)
    cleaned = remove_enquiry_from_partition(existing, enquiry_no)
    write_partition(cleaned, data_root, tenant_id, table_name, booking_month)
    logger.debug(f"Deleted EnquiryNo={enquiry_no} from {table_name}/BookingMonth={booking_month}")


# ---------------------------------------------------------------------------
# Snapshot partition write operations
# ---------------------------------------------------------------------------

def write_snapshot_partition(
    df: pl.DataFrame,
    data_root: str,
    tenant_id: str,
    table_name: str,
    snapshot_date: str,
) -> None:
    """
    Atomically write a snapshot DataFrame to a SnapshotDate partition.

    Each call is a full rebuild for that date.

    Args:
        df: Aggregated snapshot DataFrame.
        data_root: Root data directory.
        tenant_id: Tenant identifier.
        table_name: Snapshot table name.
        snapshot_date: SnapshotDate in YYYY-MM-DD format.
    """
    path = _snapshot_partition_path(data_root, tenant_id, table_name, snapshot_date)
    _atomic_write(df.to_arrow(), path)
    logger.debug(f"Snapshot partition written: {path} ({len(df)} rows)")


def delete_old_snapshot_partitions(
    data_root: str,
    tenant_id: str,
    table_name: str,
    retention_days: int,
    snapshot_date: str,
) -> None:
    """
    Delete snapshot partitions older than retention_days.

    Partitions are named SnapshotDate=YYYY-MM-DD. Any partition whose date
    is more than retention_days before snapshot_date is deleted.

    Args:
        data_root: Root data directory.
        tenant_id: Tenant identifier.
        table_name: Snapshot table name.
        retention_days: Number of days to retain.
        snapshot_date: The current snapshot date (YYYY-MM-DD) as the reference.
    """
    from datetime import date, timedelta

    cutoff = date.fromisoformat(snapshot_date) - timedelta(days=retention_days)
    table_dir = Path(data_root) / f"tenant_id={tenant_id}" / table_name

    if not table_dir.exists():
        return

    for partition_dir in table_dir.iterdir():
        if not partition_dir.is_dir():
            continue
        name = partition_dir.name
        if not name.startswith("SnapshotDate="):
            continue
        try:
            partition_date = date.fromisoformat(name.replace("SnapshotDate=", ""))
        except ValueError:
            continue

        if partition_date < cutoff:
            import shutil
            shutil.rmtree(str(partition_dir))
            logger.info(f"Deleted old snapshot partition: {partition_dir}")
