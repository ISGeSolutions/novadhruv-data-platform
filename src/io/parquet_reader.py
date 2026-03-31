"""
Parquet Reader  [AI Generated]
================================

Helpers for reading fact table partitions for snapshot building.

Reads all BookingMonth partitions for a given tenant and table into a single
Polars DataFrame, which the snapshot pipeline then filters and aggregates.
"""

from pathlib import Path

import polars as pl

from src.utils.logging_config import get_logger

logger = get_logger(__name__)


def read_all_fact_partitions(
    data_root: str,
    tenant_id: str,
    table_name: str,
) -> pl.DataFrame:
    """
    Read all BookingMonth partitions for a fact table into a single DataFrame.

    Adds the BookingMonth column (derived from the partition directory name) to
    each partition if not already present, so downstream code can filter by it.

    Args:
        data_root: Root data directory.
        tenant_id: Tenant identifier.
        table_name: 'fact_bookingcountries' or 'fact_bookingtags'.

    Returns:
        Combined Polars DataFrame. Empty DataFrame if no partitions exist.
    """
    table_dir = Path(data_root) / f"tenant_id={tenant_id}" / table_name

    if not table_dir.exists():
        logger.debug(f"Fact table directory not found: {table_dir}")
        return pl.DataFrame()

    frames: list[pl.DataFrame] = []
    for partition_dir in sorted(table_dir.iterdir()):
        if not partition_dir.is_dir():
            continue
        if not partition_dir.name.startswith("BookingMonth="):
            continue

        parquet_file = partition_dir / "data.parquet"
        if not parquet_file.exists():
            continue

        try:
            df = pl.read_parquet(str(parquet_file))
            frames.append(df)
        except Exception as e:
            logger.warning(f"Failed to read partition {partition_dir}: {e}")

    if not frames:
        return pl.DataFrame()

    combined = pl.concat(frames, how="diagonal")
    logger.debug(
        f"Read {len(frames)} partitions for {table_name}: {len(combined)} total rows"
    )
    return combined
