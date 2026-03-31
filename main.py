"""
main.py
=======

Example entry point demonstrating how to run the pipeline for a single tenant
programmatically (useful for testing and debugging individual tenants).

For production use, run run_all_tenants.py instead.

Usage:
    python main.py --tenant acme --config config.yaml
"""

import argparse
from datetime import date, datetime, timezone

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


def load_tenant(tenants_path: str, tenant_id: str) -> TenantConfig:
    with open(tenants_path) as f:
        raw = yaml.safe_load(f)
    for t in raw["tenants"]:
        if t["tenant_id"] == tenant_id:
            return TenantConfig(
                tenant_id=t["tenant_id"],
                db_type=t["db_type"],
                encrypted_connection_string=t["encrypted_connection_string"],
                encryption_key_path=t["encryption_key_path"],
            )
    raise ValueError(f"Tenant '{tenant_id}' not found in {tenants_path}")


def main(tenant_id: str, config_path: str) -> None:
    config = load_config(config_path)
    setup_logging(log_level=config.get("log_level"), log_file=config.get("log_file"))

    base_logger = get_logger(__name__)
    adapter = TenantLoggerAdapter(base_logger, tenant_id=tenant_id, stage="init")

    tenants_path = config.get("tenants_file", "tenants.yaml")
    tenant = load_tenant(tenants_path, tenant_id)

    conn_str = EncryptionUtility.decrypt_value(
        tenant.encrypted_connection_string,
        tenant.encryption_key_path,
    )
    data_root = config["data_root"]
    snapshot_date = (
        date.fromisoformat(config["snapshot_date"])
        if config.get("snapshot_date")
        else date.today()
    )

    # --- Fact pipeline ---
    adapter = adapter.with_context(stage="fact")
    with get_dhruvlog_connection(conn_str) as dhruvlog_conn:
        last_run_date = get_last_run_date(dhruvlog_conn, tenant_id, "YOYFactPipeline")

    adapter.info(f"Extracting from SPs with LastRunDate={last_run_date.isoformat()}")
    sp1, sp2, sp3 = extract_all_sps(conn_str, last_run_date, tenant.db_type, recon_mode=False)
    valid_sp1, valid_sp2, valid_sp3 = validate_all(sp1, sp2, sp3, adapter)

    if valid_sp1:
        with get_dhruvlog_connection(conn_str) as dhruvlog_conn:
            fact_pipeline = FactPipeline(
                tenant_id=tenant_id,
                data_root=data_root,
                adapter=adapter,
            )
            counts = fact_pipeline.run(valid_sp1, valid_sp2, valid_sp3, dhruvlog_conn)
            adapter.info(f"Fact pipeline counts: {counts}")
            record_successful_run(
                dhruvlog_conn, tenant_id, "YOYFactPipeline",
                datetime.now(tz=timezone.utc)
            )

    # --- Snapshot pipeline ---
    adapter = adapter.with_context(stage="snapshot")
    snap = SnapshotPipeline(
        tenant_id=tenant_id,
        data_root=data_root,
        snapshot_date=snapshot_date,
        retention_days=config.get("snapshot_retention_days", 30),
        adapter=adapter,
    )
    snap_counts = snap.run()
    adapter.info(f"Snapshot counts: {snap_counts}")

    with get_dhruvlog_connection(conn_str) as dhruvlog_conn:
        record_successful_run(
            dhruvlog_conn, tenant_id, "YOYSnapshotPipeline",
            datetime.now(tz=timezone.utc)
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run pipeline for a single tenant")
    parser.add_argument("--tenant", required=True, help="Tenant ID")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    args = parser.parse_args()
    main(tenant_id=args.tenant, config_path=args.config)
