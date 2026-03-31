"""
mssql_connector.py — compatibility re-export
=============================================

This module is superseded by db_connector.py, which supports MSSQL, Postgres,
and MariaDB. All symbols are re-exported here so that any remaining imports of
mssql_connector continue to work without change.

New code should import from src.connectors.db_connector directly.
"""

from src.connectors.db_connector import (  # noqa: F401
    SP1_COLUMNS,
    SP2_COLUMNS,
    SP3_COLUMNS,
    execute_sp1,
    execute_sp2,
    execute_sp3,
    extract_all_sps,
    get_dhruvlog_connection,
    get_mis_connection,
)
