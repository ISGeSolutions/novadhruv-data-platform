"""
Tenant Model
============

Data classes representing tenant configuration loaded from tenants.yaml.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class TenantConfig:
    """
    Immutable configuration for a single tenant.

    Attributes:
        tenant_id: Unique tenant identifier (used in Parquet paths).
        db_type: Database engine the tenant's MIS database runs on.
            Must be one of: "mssql", "postgres", "mariadb".
        encrypted_connection_string: Fernet-encrypted ODBC connection string.
        encryption_key_path: Path to the Fernet key file used to decrypt the
            connection string.
    """

    tenant_id: str
    db_type: str
    encrypted_connection_string: str
    encryption_key_path: str
