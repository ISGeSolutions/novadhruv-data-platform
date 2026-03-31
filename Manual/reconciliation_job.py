"""
reconciliation_job.py  [Manual]
================================

Standalone weekly reconciliation job using @ReconModeInd = 1.

Calls all 3 SPs with ReconModeInd=1 and its own @LastRunDate from
YOYReconPipeline, then processes the results through the same hash-based
Cases A–E logic as the daily fact pipeline.

This is a periodic safety net — it is NOT the primary mechanism for data
correctness (the daily EnquiryParquetFile tracking handles that). Run weekly
or trigger manually.

This script is intentionally kept simple and readable. Do not add
orchestration complexity here.

Usage:
    python Manual/reconciliation_job.py
    python Manual/reconciliation_job.py --config /path/to/config.yaml
    python Manual/reconciliation_job.py --tenant acme
"""

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml

# Ensure project root is on the path when running directly
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.connectors.db_connector import extract_all_sps, get_dhruvlog_connection
from src.models.tenant import TenantConfig
from src.pipeline.fact_pipeline import FactPipeline
from src.tracking.tracking_store import get_last_run_date, record_successful_run
from src.utils.encryption import EncryptionUtility
from src.utils.logging_config import TenantLoggerAdapter, get_logger, setup_logging
from src.validation.validator import validate_all


def load_config(config_path: str) -> dict:
    with open(config_path) as f:
        return yaml.safe_load(f)


def load_tenants(config: dict) -> list[TenantConfig]:
    tenants_path = config.get("tenants_file", "tenants.yaml")
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


def reconcile_tenant(
    tenant: TenantConfig,
    data_root: str,
    base_logger,
) -> None:
    """Run reconciliation for a single tenant."""
    adapter = TenantLoggerAdapter(base_logger, tenant_id=tenant.tenant_id, stage="recon")

    conn_str = EncryptionUtility.decrypt_value(
        tenant.encrypted_connection_string,
        tenant.encryption_key_path,
    )

    # Get last recon run date (separate from fact pipeline run date)
    with get_dhruvlog_connection(conn_str) as dhruvlog_conn:
        last_recon_date = get_last_run_date(
            dhruvlog_conn, tenant.tenant_id, "YOYReconPipeline"
        )

    adapter.info(
        f"Starting reconciliation with LastRunDate={last_recon_date.isoformat()} "
        f"(ReconModeInd=1)"
    )

    # Call SPs with ReconModeInd=1
    sp1, sp2, sp3 = extract_all_sps(conn_str, last_recon_date, tenant.db_type, recon_mode=True)
    valid_sp1, valid_sp2, valid_sp3 = validate_all(sp1, sp2, sp3, adapter)

    if not valid_sp1:
        adapter.info("No records returned by reconciliation SPs — nothing to process")
        return

    adapter.info(f"Reconciliation extract: {len(valid_sp1)} enquiries to process")

    # Process using the same FactPipeline logic (Cases A–E)
    with get_dhruvlog_connection(conn_str) as dhruvlog_conn:
        pipeline = FactPipeline(
            tenant_id=tenant.tenant_id,
            data_root=data_root,
            adapter=adapter,
        )
        counts = pipeline.run(valid_sp1, valid_sp2, valid_sp3, dhruvlog_conn)

        adapter.info(f"Reconciliation complete: {counts}")

        # Record successful recon run
        record_successful_run(
            dhruvlog_conn,
            tenant.tenant_id,
            "YOYReconPipeline",
            datetime.now(tz=timezone.utc),
        )


def main(config_path: str, tenant_filter: str | None = None) -> None:
    config = load_config(config_path)
    setup_logging(log_level=config.get("log_level"), log_file=config.get("log_file"))
    base_logger = get_logger(__name__)

    tenants = load_tenants(config)
    data_root = config["data_root"]

    if tenant_filter:
        tenants = [t for t in tenants if t.tenant_id == tenant_filter]
        if not tenants:
            base_logger.error(f"Tenant '{tenant_filter}' not found")
            sys.exit(1)

    failed: list[str] = []
    for tenant in tenants:
        try:
            reconcile_tenant(tenant, data_root, base_logger)
        except Exception as e:
            base_logger.error(
                f"Reconciliation failed for tenant {tenant.tenant_id}: {e}",
                exc_info=True,
            )
            failed.append(tenant.tenant_id)

    if failed:
        base_logger.error(f"Reconciliation failed for tenants: {failed}")
        sys.exit(1)

    base_logger.info("All tenants reconciled successfully")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Weekly reconciliation job (ReconModeInd=1)")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    parser.add_argument("--tenant", default=None, help="Run for a single tenant only")
    args = parser.parse_args()
    main(config_path=args.config, tenant_filter=args.tenant)
