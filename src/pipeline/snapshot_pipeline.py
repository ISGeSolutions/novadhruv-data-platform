"""
Snapshot Pipeline  [Manual]
============================

Builds daily snapshot tables from the current fact Parquet files.

Snapshot A — fact_bookingcountries_snapshot
    Source: fact_bookingcountries
    Filter: BookingDate ≤ SnapshotDate AND DepartureDate ≥ SnapshotDate
    Group by: SnapshotDate, BookingFY, DepartureFY, RelativeDepartureMonth,
              TourType, TourGenericCode, CountryCode, CountryName

Snapshot B — fact_bookingcountries_snapshot_country_tag
    Source: INNER JOIN of fact_bookingcountries + fact_bookingtags on EnquiryNo
    Measures: fact_bookingcountries.BookingValue, fact_bookingcountries.NoOfPax
    Group by: same as A + TagName

RelativeDepartureMonth is computed at snapshot build time:
    = (DepartureMonth year-month) - (SnapshotDate year-month) + 1
    Month 1 = current calendar month (the month containing SnapshotDate)

Horizon: only rows where RelativeDepartureMonth is between 1 and 24 inclusive.

Each run is a full rebuild for the given SnapshotDate (idempotent).
Old snapshot partitions are pruned using a multi-year retention policy
(snapshot_retention_days window per year, snapshot_retention_years prior years).
"""

import logging
from datetime import date
from pathlib import Path

import polars as pl

from src.io.parquet_reader import read_all_fact_partitions
from src.io.parquet_writer import (
    delete_old_snapshot_partitions,
    write_snapshot_partition,
)
from src.utils.logging_config import get_logger

logger = get_logger(__name__)

SNAPSHOT_TABLE_A = "fact_bookingcountries_snapshot"
SNAPSHOT_TABLE_B = "fact_bookingcountries_snapshot_country_tag"


# ---------------------------------------------------------------------------
# Snapshot existence check
# ---------------------------------------------------------------------------

def _snapshot_partition_exists(
    data_root: str,
    tenant_id: str,
    table_name: str,
    snapshot_date: date,
) -> bool:
    """Return True if a data.parquet file exists for this snapshot partition."""
    return (
        Path(data_root)
        / f"tenant_id={tenant_id}"
        / table_name
        / f"SnapshotDate={snapshot_date.isoformat()}"
        / "data.parquet"
    ).exists()


# ---------------------------------------------------------------------------
# RelativeDepartureMonth
# ---------------------------------------------------------------------------

def _compute_relative_departure_month(
    snapshot_date: date,
    departure_month_str: str,
) -> int:
    """
    Compute RelativeDepartureMonth for a row.

    Month 1 = the calendar month containing SnapshotDate.
    Month 2 = the following calendar month.

    Args:
        snapshot_date: The SnapshotDate for this pipeline run.
        departure_month_str: DepartureMonth in 'YYYY-MM' format.

    Returns:
        Integer offset (1 = current month, 2 = next month, etc.)
    """
    snap_year, snap_month = snapshot_date.year, snapshot_date.month
    dep_year, dep_month_num = map(int, departure_month_str.split("-"))
    delta_months = (dep_year - snap_year) * 12 + (dep_month_num - snap_month)
    return delta_months + 1


def _add_relative_departure_month(df: pl.DataFrame, snapshot_date: date) -> pl.DataFrame:
    """Add the RelativeDepartureMonth column to a DataFrame using Polars expressions."""
    snap_total_months = snapshot_date.year * 12 + snapshot_date.month

    # DepartureMonth is stored as 'YYYY-MM'
    return df.with_columns(
        (
            pl.col("DepartureMonth").str.slice(0, 4).cast(pl.Int32) * 12
            + pl.col("DepartureMonth").str.slice(5, 2).cast(pl.Int32)
            - snap_total_months
            + 1
        ).alias("RelativeDepartureMonth")
    )


# ---------------------------------------------------------------------------
# Snapshot filter
# ---------------------------------------------------------------------------

def _apply_snapshot_filter(df: pl.DataFrame, snapshot_date: date) -> pl.DataFrame:
    """
    Apply the snapshot filter and horizon:
        BookingDate ≤ SnapshotDate
        DepartureDate ≥ SnapshotDate
        RelativeDepartureMonth between 1 and 24 inclusive

    Args:
        df: Fact DataFrame (must already have RelativeDepartureMonth column).
        snapshot_date: The SnapshotDate for this run.

    Returns:
        Filtered DataFrame.
    """
    snap_pl = pl.lit(snapshot_date)
    return df.filter(
        (pl.col("BookingDate") <= snap_pl)
        & (pl.col("DepartureDate") >= snap_pl)
        & (pl.col("RelativeDepartureMonth") >= 1)
        & (pl.col("RelativeDepartureMonth") <= 24)
    )


# ---------------------------------------------------------------------------
# Snapshot A builder
# ---------------------------------------------------------------------------

def _build_snapshot_a(
    countries_df: pl.DataFrame,
    snapshot_date: date,
) -> pl.DataFrame:
    """
    Build Snapshot A: Country snapshot.

    Args:
        countries_df: Full fact_bookingcountries DataFrame.
        snapshot_date: Snapshot date.

    Returns:
        Aggregated snapshot DataFrame.
    """
    snapshot_date_str = snapshot_date.isoformat()

    enriched = _add_relative_departure_month(countries_df, snapshot_date)
    filtered = _apply_snapshot_filter(enriched, snapshot_date)

    if filtered.is_empty():
        return pl.DataFrame()

    return (
        filtered
        .with_columns(pl.lit(snapshot_date_str).alias("SnapshotDate"))
        .group_by([
            "SnapshotDate",
            "BookingFY",
            "DepartureFY",
            "RelativeDepartureMonth",
            "TourType",
            "TourGenericCode",
            "CountryCode",
            "CountryName",
        ])
        .agg([
            pl.sum("BookingValue").alias("BookingValue"),
            pl.sum("NoOfPax").alias("NoOfPax"),
        ])
        .sort(["RelativeDepartureMonth", "CountryCode"])
    )


# ---------------------------------------------------------------------------
# Snapshot B builder
# ---------------------------------------------------------------------------

def _build_snapshot_b(
    countries_df: pl.DataFrame,
    tags_df: pl.DataFrame,
    snapshot_date: date,
) -> pl.DataFrame:
    """
    Build Snapshot B: Country + Tag snapshot.

    Uses INNER JOIN on EnquiryNo — only EnquiryNos present in both tables
    are included. Bookings with no tags do not appear in this snapshot.

    Measures come from fact_bookingcountries (country-distributed BookingValue),
    NOT from fact_bookingtags (which holds full enquiry-level values).

    Args:
        countries_df: Full fact_bookingcountries DataFrame.
        tags_df: Full fact_bookingtags DataFrame.
        snapshot_date: Snapshot date.

    Returns:
        Aggregated snapshot DataFrame.
    """
    snapshot_date_str = snapshot_date.isoformat()

    # Apply RelativeDepartureMonth and snapshot filter to both fact tables
    countries_enriched = _add_relative_departure_month(countries_df, snapshot_date)
    countries_filtered = _apply_snapshot_filter(countries_enriched, snapshot_date)

    tags_enriched = _add_relative_departure_month(tags_df, snapshot_date)
    tags_filtered = _apply_snapshot_filter(tags_enriched, snapshot_date)

    if countries_filtered.is_empty() or tags_filtered.is_empty():
        return pl.DataFrame()

    # INNER JOIN on EnquiryNo
    # Only pull TagName from the tags side; all measures come from countries side.
    tags_for_join = tags_filtered.select(["EnquiryNo", "TagName"]).unique()

    joined = countries_filtered.join(tags_for_join, on="EnquiryNo", how="inner")

    if joined.is_empty():
        return pl.DataFrame()

    return (
        joined
        .with_columns(pl.lit(snapshot_date_str).alias("SnapshotDate"))
        .group_by([
            "SnapshotDate",
            "BookingFY",
            "DepartureFY",
            "RelativeDepartureMonth",
            "TourType",
            "TourGenericCode",
            "CountryCode",
            "CountryName",
            "TagName",
        ])
        .agg([
            pl.sum("BookingValue").alias("BookingValue"),
            pl.sum("NoOfPax").alias("NoOfPax"),
        ])
        .sort(["RelativeDepartureMonth", "CountryCode", "TagName"])
    )


# ---------------------------------------------------------------------------
# Pipeline class
# ---------------------------------------------------------------------------

class SnapshotPipeline:
    """
    Builds daily snapshot partitions for a single tenant.

    Each run creates a fresh SnapshotDate=YYYY-MM-DD partition for both
    Snapshot A and B, then prunes partitions outside the multi-year
    retention windows.
    """

    def __init__(
        self,
        tenant_id: str,
        data_root: str,
        snapshot_date: date,
        retention_days: int,
        retention_years: int,
        adapter: logging.LoggerAdapter,
    ) -> None:
        self.tenant_id = tenant_id
        self.data_root = data_root
        self.snapshot_date = snapshot_date
        self.retention_days = retention_days
        self.retention_years = retention_years
        self.adapter = adapter

    def run(self) -> dict:
        """
        Build Snapshot A and B for the configured SnapshotDate, then
        auto-backfill any prior-year equivalent dates that are missing on disk.

        Fact partitions are read once and reused across all dates (today +
        backfill). Prior-year dates are only generated when absent, so reruns
        are safe and idempotent. Old snapshot partitions are pruned after all
        writes.

        Returns:
            Summary dict with row counts for today's snapshot and the number
            of prior-year dates that were backfilled.
        """
        snapshot_date_str = self.snapshot_date.isoformat()
        self.adapter.info(f"Building snapshots for SnapshotDate={snapshot_date_str}")

        # Read fact tables once — reused for today and all backfill dates
        countries_df = read_all_fact_partitions(
            self.data_root, self.tenant_id, "fact_bookingcountries"
        )
        tags_df = read_all_fact_partitions(
            self.data_root, self.tenant_id, "fact_bookingtags"
        )

        if countries_df.is_empty():
            self.adapter.warning("fact_bookingcountries is empty — skipping snapshots")
            return {"snapshot_a_rows": 0, "snapshot_b_rows": 0, "backfill_dates_generated": 0}

        # Build today's snapshot
        snap_a, snap_b = self._build_and_write(countries_df, tags_df, self.snapshot_date)

        # Auto-backfill missing prior-year equivalent dates
        missing_dates = self._get_missing_prior_year_dates()
        for prior_date in missing_dates:
            self.adapter.info(f"Backfilling snapshot for SnapshotDate={prior_date.isoformat()}")
            self._build_and_write(countries_df, tags_df, prior_date)

        if missing_dates:
            self.adapter.info(
                f"Backfilled {len(missing_dates)} prior-year snapshot date(s): "
                + ", ".join(d.isoformat() for d in missing_dates)
            )

        # Prune partitions outside retention windows
        for table in [SNAPSHOT_TABLE_A, SNAPSHOT_TABLE_B]:
            delete_old_snapshot_partitions(
                self.data_root, self.tenant_id, table,
                self.retention_days, self.retention_years, snapshot_date_str,
            )

        return {
            "snapshot_a_rows": len(snap_a),
            "snapshot_b_rows": len(snap_b),
            "backfill_dates_generated": len(missing_dates),
        }

    def _build_and_write(
        self,
        countries_df: pl.DataFrame,
        tags_df: pl.DataFrame,
        target_date: date,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Build and write Snapshot A and B for a single target date."""
        target_date_str = target_date.isoformat()

        snap_a = _build_snapshot_a(countries_df, target_date)
        if not snap_a.is_empty():
            write_snapshot_partition(
                snap_a, self.data_root, self.tenant_id, SNAPSHOT_TABLE_A, target_date_str
            )
        self.adapter.info(f"Snapshot A ({target_date_str}): {len(snap_a)} rows")

        snap_b = _build_snapshot_b(countries_df, tags_df, target_date)
        if not snap_b.is_empty():
            write_snapshot_partition(
                snap_b, self.data_root, self.tenant_id, SNAPSHOT_TABLE_B, target_date_str
            )
        self.adapter.info(f"Snapshot B ({target_date_str}): {len(snap_b)} rows")

        return snap_a, snap_b

    def _get_missing_prior_year_dates(self) -> list[date]:
        """
        Return the subset of prior-year equivalent dates not yet on disk.

        Checks the same calendar day for each of the prior retention_years years.
        A date is included when either Snapshot A or B partition is absent,
        so both tables are (re)generated together.
        """
        missing: list[date] = []
        for y in range(1, self.retention_years + 1):
            prior_year = self.snapshot_date.year - y
            try:
                prior_date = date(prior_year, self.snapshot_date.month, self.snapshot_date.day)
            except ValueError:
                # Feb 29 in a non-leap year — use Feb 28
                prior_date = date(prior_year, self.snapshot_date.month, 28)

            a_exists = _snapshot_partition_exists(
                self.data_root, self.tenant_id, SNAPSHOT_TABLE_A, prior_date
            )
            b_exists = _snapshot_partition_exists(
                self.data_root, self.tenant_id, SNAPSHOT_TABLE_B, prior_date
            )
            if not a_exists or not b_exists:
                missing.append(prior_date)

        return missing
