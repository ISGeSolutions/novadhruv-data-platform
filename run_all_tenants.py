"""
run_all_tenants.py
==================

Main entry point: runs the fact pipeline and snapshot pipeline for every
tenant defined in tenants.yaml.

On per-tenant failure: logs the error, skips to the next tenant, and records
the failure in the run summary. Does not abort the full run.

Writes {DATA_ROOT}/run_summary_{date}.json after all tenants are processed.

Usage:
    python run_all_tenants.py
    python run_all_tenants.py --config /path/to/config.yaml
"""

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import yaml

from src.connectors.db_connector import extract_all_sps, get_dhruvlog_connection
from src.models.tenant import TenantConfig
from src.pipeline.fact_pipeline import FactPipeline
from src.pipeline.snapshot_pipeline import SnapshotPipeline
from src.tracking.tracking_store import get_last_run_date, record_successful_run
from src.utils.encryption import EncryptionUtility
from src.utils.logging_config import TenantLoggerAdapter, get_logger, setup_logging
from src.validation.validator import validate_all


def load_config(config_path: str) -> dict:
    with open(config_path) as f:
        return yaml.safe_load(f)


def load_tenants(tenants_path: str) -> list[TenantConfig]:
    with open(tenants_path) as f:
        raw = yaml.safe_load(f)
    return [
        TenantConfig(
            tenant_id=t["tenant_id"],
            db_type=t["db_type"],
            encrypted_connection_string=t["encrypted_connection_string"],
            encryption_key_path=t["encryption_key_path"],
        )
        for t in raw["tenants"]
    ]


def run_tenant(
    tenant: TenantConfig,
    config: dict,
    snapshot_date: date,
    base_logger,
) -> dict:
    """
    Run the full pipeline for a single tenant.

    Returns a result dict with status and counts.
    """
    adapter = TenantLoggerAdapter(base_logger, tenant_id=tenant.tenant_id, stage="init")

    conn_str = EncryptionUtility.decrypt_value(
        tenant.encrypted_connection_string,
        tenant.encryption_key_path,
    )
    data_root = config["data_root"]

    # --- Fact pipeline ---
    adapter = adapter.with_context(stage="fact")
    with get_dhruvlog_connection(conn_str) as dhruvlog_conn:
        last_run_date = get_last_run_date(dhruvlog_conn, tenant.tenant_id, "YOYFactPipeline")

    adapter.info(f"Last run date: {last_run_date.isoformat()}")

    sp1, sp2, sp3 = extract_all_sps(conn_str, last_run_date, tenant.db_type, recon_mode=False)
    valid_sp1, valid_sp2, valid_sp3 = validate_all(sp1, sp2, sp3, adapter)

    fact_counts = {}
    if valid_sp1:
        with get_dhruvlog_connection(conn_str) as dhruvlog_conn:
            pipeline = FactPipeline(
                tenant_id=tenant.tenant_id,
                data_root=data_root,
                adapter=adapter,
            )
            fact_counts = pipeline.run(valid_sp1, valid_sp2, valid_sp3, dhruvlog_conn)
            run_ts = datetime.now(tz=timezone.utc)
            record_successful_run(dhruvlog_conn, tenant.tenant_id, "YOYFactPipeline", run_ts)
    else:
        adapter.info("No valid SP rows — skipping fact pipeline")

    # --- Snapshot pipeline ---
    adapter = adapter.with_context(stage="snapshot")
    snap_pipeline = SnapshotPipeline(
        tenant_id=tenant.tenant_id,
        data_root=data_root,
        snapshot_date=snapshot_date,
        retention_days=config.get("snapshot_retention_days", 30),
        retention_years=config.get("snapshot_retention_years", 5),
        adapter=adapter,
    )
    snap_counts = snap_pipeline.run()

    with get_dhruvlog_connection(conn_str) as dhruvlog_conn:
        record_successful_run(
            dhruvlog_conn, tenant.tenant_id, "YOYSnapshotPipeline",
            datetime.now(tz=timezone.utc)
        )

    return {
        "status": "success",
        "fact_counts": fact_counts,
        "snapshot_counts": snap_counts,
    }


def main(config_path: str = "config.yaml") -> None:
    config = load_config(config_path)

    setup_logging(
        log_level=config.get("log_level"),
        log_file=config.get("log_file"),
    )
    base_logger = get_logger(__name__)

    tenants_path = config.get("tenants_file", "tenants.yaml")
    tenants = load_tenants(tenants_path)

    # Resolve snapshot date
    if config.get("snapshot_date"):
        snapshot_date = date.fromisoformat(config["snapshot_date"])
    else:
        snapshot_date = date.today()

    data_root = config["data_root"]
    run_date_str = date.today().isoformat()

    results = {}
    for tenant in tenants:
        try:
            base_logger.info(f"Starting pipeline for tenant: {tenant.tenant_id}")
            result = run_tenant(tenant, config, snapshot_date, base_logger)
            results[tenant.tenant_id] = {
                **result,
                "completed_at": datetime.now(tz=timezone.utc).isoformat(),
            }
            base_logger.info(f"Completed tenant: {tenant.tenant_id}")
        except Exception as e:
            base_logger.error(
                f"Pipeline failed for tenant {tenant.tenant_id}: {e}",
                exc_info=True,
            )
            results[tenant.tenant_id] = {
                "status": "failed",
                "error": str(e),
                "failed_at": datetime.now(tz=timezone.utc).isoformat(),
            }

    # Write run summary
    summary_path = Path(data_root) / f"run_summary_{run_date_str}.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary = {
        "run_date": run_date_str,
        "snapshot_date": snapshot_date.isoformat(),
        "tenants": results,
    }
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)

    failed = [t for t, r in results.items() if r.get("status") != "success"]
    base_logger.info(
        f"Run complete. "
        f"Success: {len(tenants) - len(failed)}/{len(tenants)}. "
        f"Summary: {summary_path}"
    )

    if failed:
        base_logger.error(f"Failed tenants: {failed}")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run YOY fact + snapshot pipelines for all tenants")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    args = parser.parse_args()
    main(config_path=args.config)
