"""
backfill_snapshots.py  [Manual]
================================

Backfills snapshot Parquet files for all (or one) tenant across the date
range declared in config.yaml under the backfill section:

    backfill:
      enabled: true
      from_date: "2021-04-24"
      to_date:   "2026-04-23"

Partitions that already exist on disk are skipped — safe to re-run.
Fact tables must already be populated by the daily pipeline before running.

Progress is shown as a single updating line. Press x at any time to stop
gracefully (the current date finishes, then the script exits cleanly).

Usage:
    python Manual/backfill_snapshots.py
    python Manual/backfill_snapshots.py --config config.yaml
    python Manual/backfill_snapshots.py --tenant acme
"""

import argparse
import select
import sys
from datetime import date, timedelta

import yaml

from src.io.parquet_reader import read_all_fact_partitions
from src.models.tenant import TenantConfig
from src.pipeline.snapshot_pipeline import SnapshotPipeline
from src.utils.logging_config import TenantLoggerAdapter, get_logger, setup_logging

try:
    import termios
    import tty
    _HAS_TTY = True
except ImportError:
    _HAS_TTY = False  # Windows — abort-on-x unavailable, everything else works


# ---------------------------------------------------------------------------
# Config / tenant loading
# ---------------------------------------------------------------------------

def _load_config(config_path: str) -> dict:
    with open(config_path) as f:
        return yaml.safe_load(f)


def _load_tenants(path: str, tenant_filter: str | None) -> list[TenantConfig]:
    with open(path) as f:
        raw = yaml.safe_load(f)
    tenants = [
        TenantConfig(
            tenant_id=t["tenant_id"],
            db_type=t["db_type"],
            encrypted_connection_string=t["encrypted_connection_string"],
            encryption_key_path=t["encryption_key_path"],
        )
        for t in raw["tenants"]
    ]
    if tenant_filter:
        tenants = [t for t in tenants if t.tenant_id == tenant_filter]
        if not tenants:
            raise ValueError(f"Tenant '{tenant_filter}' not found in {path}")
    return tenants


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def _date_range(from_date: date, to_date: date) -> list[date]:
    dates, current = [], from_date
    while current <= to_date:
        dates.append(current)
        current += timedelta(days=1)
    return dates


# ---------------------------------------------------------------------------
# Keyboard abort (cbreak mode — per-keystroke, Ctrl-C still works)
# ---------------------------------------------------------------------------

def _x_pressed() -> bool:
    """Non-blocking: True when the user has typed x or X."""
    if not sys.stdin.isatty():
        return False
    ready, _, _ = select.select([sys.stdin], [], [], 0)
    if ready:
        ch = sys.stdin.read(1)
        return ch.lower() == "x"
    return False


# ---------------------------------------------------------------------------
# Per-tenant backfill
# ---------------------------------------------------------------------------

def _backfill_tenant(
    tenant: TenantConfig,
    config: dict,
    dates: list[date],
    base_logger,
) -> dict:
    data_root = config["data_root"]
    total = len(dates)

    print(f"\n[{tenant.tenant_id}]  Loading fact tables from disk ...")
    countries_df = read_all_fact_partitions(data_root, tenant.tenant_id, "fact_bookingcountries")
    tags_df = read_all_fact_partitions(data_root, tenant.tenant_id, "fact_bookingtags")

    if countries_df.is_empty():
        print(f"[{tenant.tenant_id}]  No fact data found on disk — skipping tenant.")
        return {"built": 0, "skipped": 0, "aborted": False}

    adapter = TenantLoggerAdapter(base_logger, tenant_id=tenant.tenant_id, stage="backfill")
    pipeline = SnapshotPipeline(
        tenant_id=tenant.tenant_id,
        data_root=data_root,
        snapshot_date=date.today(),
        retention_days=config.get("snapshot_retention_days", 30),
        retention_years=config.get("snapshot_retention_years", 5),
        adapter=adapter,
    )

    print(f"[{tenant.tenant_id}]  {total} dates to process  ({dates[0]} → {dates[-1]})")

    built = skipped = 0
    aborted = False
    i = 0

    for i, target_date in enumerate(dates, 1):
        if _x_pressed():
            aborted = True
            break

        sys.stdout.write(
            f"\r  Building snapshot: {target_date}  [{i}/{total}]"
            f"  built={built}  skipped={skipped}"
            f"  | press x to abort   "
        )
        sys.stdout.flush()

        a_rows, b_rows = pipeline.build_for_date(countries_df, tags_df, target_date)
        if a_rows == 0 and b_rows == 0:
            skipped += 1
        else:
            built += 1

    # Clear progress line and print final status
    sys.stdout.write("\n")
    sys.stdout.flush()

    label = "ABORTED" if aborted else "COMPLETE"
    print(
        f"  [{tenant.tenant_id}]  {label} — "
        f"built={built}  skipped={skipped}  processed={i}/{total}"
    )
    return {"built": built, "skipped": skipped, "aborted": aborted}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(config_path: str, tenant_filter: str | None) -> None:
    config = _load_config(config_path)
    setup_logging(log_level=config.get("log_level"), log_file=config.get("log_file"))
    base_logger = get_logger(__name__)

    backfill_cfg = config.get("backfill", {})
    if not backfill_cfg.get("enabled", False):
        print(
            "Backfill is disabled (backfill.enabled=false in config.yaml). "
            "Set enabled: true to run."
        )
        return

    from_date = date.fromisoformat(str(backfill_cfg["from_date"]))
    to_date = date.fromisoformat(str(backfill_cfg["to_date"]))
    if from_date > to_date:
        raise ValueError(
            f"backfill.from_date ({from_date}) must be before backfill.to_date ({to_date})"
        )

    dates = _date_range(from_date, to_date)
    tenants = _load_tenants(config.get("tenants_file", "tenants.yaml"), tenant_filter)

    print("=" * 62)
    print("  Snapshot Backfill")
    print(f"  Date range : {from_date} → {to_date}  ({len(dates)} days)")
    print(f"  Tenants    : {', '.join(t.tenant_id for t in tenants)}")
    if _HAS_TTY and sys.stdin.isatty():
        print("  Tip        : Press x at any time to abort gracefully")
    print("=" * 62)

    # Switch stdin to cbreak mode: each keypress is readable without Enter
    old_settings = None
    if _HAS_TTY and sys.stdin.isatty():
        old_settings = termios.tcgetattr(sys.stdin.fileno())
        tty.setcbreak(sys.stdin.fileno())

    try:
        for tenant in tenants:
            result = _backfill_tenant(tenant, config, dates, base_logger)
            if result.get("aborted"):
                print("\nAborted by user — stopped after current date.")
                break
    finally:
        if old_settings is not None:
            termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, old_settings)

    print("\n" + "=" * 62)
    print("  Backfill finished.")
    print("=" * 62)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Backfill snapshot Parquet files for a date range"
    )
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    parser.add_argument("--tenant", default=None, help="Process only this tenant ID")
    args = parser.parse_args()
    main(config_path=args.config, tenant_filter=args.tenant)
