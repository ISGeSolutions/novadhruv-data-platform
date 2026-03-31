"""
DB Connector  [AI Generated]
=============================

Handles connections to the tenant MIS database and the shared dhruvlog database.
Supports MSSQL, Postgres, and MariaDB tenants via pyodbc (ODBC).

SP qualified names and call syntax differ by database type:
  mssql    — EXEC <db>.dbo.<sp> @LastRunDate=?, @ReconModeInd=?
  postgres — CALL <db>.<sp>(?, ?)   (schema-name = dbname per Postgres convention)
  mariadb  — CALL <db>.<sp>(?, ?)

The db_type for each tenant is declared in tenants.yaml and flows through
TenantConfig into extract_all_sps.

SP column order is defined here and used by the hash computation in
fact_pipeline.py — do not reorder without updating the hash logic.
"""

import re
from datetime import datetime
from typing import Optional

import pyodbc

from src.utils.logging_config import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Column definitions (order is authoritative for hash computation)
# ---------------------------------------------------------------------------

SP1_COLUMNS = [
    "EnquiryNo", "ClientCode", "TourType", "TourGenericCode",
    "DepartureDate", "ReturnDate", "EnquiryDate", "ConvertedToQuoteDate",
    "BookingDate", "BookingValue", "NoOfPax",
]

SP2_COLUMNS = ["EnquiryNo", "CountryCode", "CountryName", "BookingValue", "NoOfPax"]

SP3_COLUMNS = ["EnquiryNo", "TagName", "BookingValue", "NoOfPax"]

# Base SP names (without any database/schema qualifier)
_SP1_NAME = "mis_YOYAnalyticsEnquiries"
_SP2_NAME = "mis_YOYAnalyticsEnquiriesEnqCountries"
_SP3_NAME = "mis_YOYAnalyticsEnquiriesEnqTags"


# ---------------------------------------------------------------------------
# Connection string helpers
# ---------------------------------------------------------------------------

def _extract_dbname(connection_string: str) -> str:
    """
    Extract the database name from an ODBC connection string.

    Handles both DATABASE= (MSSQL, MariaDB ODBC) and dbname= (Postgres ODBC).
    """
    pattern = re.compile(r"(?:DATABASE|dbname)\s*=\s*([^;]+)", re.IGNORECASE)
    m = pattern.search(connection_string)
    if m:
        return m.group(1).strip()
    raise ValueError(
        "Cannot determine database name from connection string — "
        "expected DATABASE= or dbname= key"
    )


def _replace_database(connection_string: str, new_database: str) -> str:
    """Replace the DATABASE= component in an ODBC connection string."""
    pattern = re.compile(r"((?:DATABASE|dbname)\s*=\s*)[^;]+", re.IGNORECASE)
    if pattern.search(connection_string):
        return pattern.sub(rf"\g<1>{new_database}", connection_string)
    # Key not present — append it
    return connection_string.rstrip(";") + f";DATABASE={new_database}"


# ---------------------------------------------------------------------------
# SP call SQL builder
# ---------------------------------------------------------------------------

def _build_sp_call_sql(db_type: str, dbname: str, sp_name: str) -> str:
    """
    Build the full SQL statement to call a stored procedure.

    Args:
        db_type: "mssql", "postgres", or "mariadb".
        dbname:  Database name (extracted from the connection string).
        sp_name: Base SP name without any qualifier.

    Returns:
        SQL string ready for cursor.execute(sql, last_run_date, recon_mode_int).
    """
    if db_type == "mssql":
        qualified = f"{dbname}.dbo.{sp_name}"
        return f"EXEC {qualified} @LastRunDate=?, @ReconModeInd=?"
    elif db_type in ("postgres", "mariadb"):
        qualified = f"{dbname}.{sp_name}"
        return f"CALL {qualified}(?, ?)"
    else:
        raise ValueError(
            f"Unsupported db_type: {db_type!r}. "
            "Must be one of: mssql, postgres, mariadb"
        )


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

def get_mis_connection(connection_string: str) -> pyodbc.Connection:
    """
    Open a connection to the tenant's MIS database.

    Args:
        connection_string: Decrypted ODBC connection string.

    Returns:
        pyodbc.Connection
    """
    return pyodbc.connect(connection_string, autocommit=False)


def get_dhruvlog_connection(connection_string: str) -> pyodbc.Connection:
    """
    Open a connection to the dhruvlog database on the same server.

    The DATABASE component of the connection string is replaced with 'dhruvlog'.

    Args:
        connection_string: Decrypted ODBC connection string (pointing to MIS).

    Returns:
        pyodbc.Connection
    """
    dhruvlog_cs = _replace_database(connection_string, "dhruvlog")
    return pyodbc.connect(dhruvlog_cs, autocommit=False)


# ---------------------------------------------------------------------------
# SP execution helpers
# ---------------------------------------------------------------------------

def _rows_to_dicts(cursor: pyodbc.Cursor, columns: list[str]) -> list[dict]:
    """Convert cursor rows to a list of dicts using the provided column names."""
    rows = cursor.fetchall()
    return [dict(zip(columns, row)) for row in rows]


def execute_sp1(
    conn: pyodbc.Connection,
    db_type: str,
    dbname: str,
    last_run_date: datetime,
    recon_mode: bool = False,
) -> list[dict]:
    """
    Execute SP1: <db>.dbo.mis_YOYAnalyticsEnquiries (MSSQL)
              or <db>.mis_YOYAnalyticsEnquiries      (Postgres / MariaDB)

    Args:
        conn: MIS database connection.
        db_type: "mssql", "postgres", or "mariadb".
        dbname: Database name (used to build the qualified SP name).
        last_run_date: @LastRunDate parameter.
        recon_mode: @ReconModeInd parameter (True = reconciliation mode).

    Returns:
        List of dicts with SP1_COLUMNS keys.
    """
    sql = _build_sp_call_sql(db_type, dbname, _SP1_NAME)
    cursor = conn.cursor()
    cursor.execute(sql, last_run_date, 1 if recon_mode else 0)
    rows = _rows_to_dicts(cursor, SP1_COLUMNS)
    logger.debug(f"SP1 returned {len(rows)} rows")
    return rows


def execute_sp2(
    conn: pyodbc.Connection,
    db_type: str,
    dbname: str,
    last_run_date: datetime,
    recon_mode: bool = False,
) -> list[dict]:
    """
    Execute SP2: <db>.dbo.mis_YOYAnalyticsEnquiriesEnqCountries (MSSQL)
              or <db>.mis_YOYAnalyticsEnquiriesEnqCountries      (Postgres / MariaDB)

    Args:
        conn: MIS database connection.
        db_type: "mssql", "postgres", or "mariadb".
        dbname: Database name.
        last_run_date: @LastRunDate parameter.
        recon_mode: @ReconModeInd parameter.

    Returns:
        List of dicts with SP2_COLUMNS keys.
    """
    sql = _build_sp_call_sql(db_type, dbname, _SP2_NAME)
    cursor = conn.cursor()
    cursor.execute(sql, last_run_date, 1 if recon_mode else 0)
    rows = _rows_to_dicts(cursor, SP2_COLUMNS)
    logger.debug(f"SP2 returned {len(rows)} rows")
    return rows


def execute_sp3(
    conn: pyodbc.Connection,
    db_type: str,
    dbname: str,
    last_run_date: datetime,
    recon_mode: bool = False,
) -> list[dict]:
    """
    Execute SP3: <db>.dbo.mis_YOYAnalyticsEnquiriesEnqTags (MSSQL)
              or <db>.mis_YOYAnalyticsEnquiriesEnqTags      (Postgres / MariaDB)

    Args:
        conn: MIS database connection.
        db_type: "mssql", "postgres", or "mariadb".
        dbname: Database name.
        last_run_date: @LastRunDate parameter.
        recon_mode: @ReconModeInd parameter.

    Returns:
        List of dicts with SP3_COLUMNS keys.
    """
    sql = _build_sp_call_sql(db_type, dbname, _SP3_NAME)
    cursor = conn.cursor()
    cursor.execute(sql, last_run_date, 1 if recon_mode else 0)
    rows = _rows_to_dicts(cursor, SP3_COLUMNS)
    logger.debug(f"SP3 returned {len(rows)} rows")
    return rows


# ---------------------------------------------------------------------------
# Combined extract
# ---------------------------------------------------------------------------

def extract_all_sps(
    connection_string: str,
    last_run_date: datetime,
    db_type: str,
    recon_mode: bool = False,
) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Execute all three SPs and return their results.

    Opens a single MIS connection, runs all three SPs, then closes it.
    The SP qualified name and call syntax are determined by db_type.

    Args:
        connection_string: Decrypted ODBC connection string.
        last_run_date: @LastRunDate for all three SPs.
        db_type: "mssql", "postgres", or "mariadb".
        recon_mode: @ReconModeInd flag.

    Returns:
        Tuple of (sp1_rows, sp2_rows, sp3_rows).
    """
    dbname = _extract_dbname(connection_string)
    with get_mis_connection(connection_string) as conn:
        sp1 = execute_sp1(conn, db_type, dbname, last_run_date, recon_mode)
        sp2 = execute_sp2(conn, db_type, dbname, last_run_date, recon_mode)
        sp3 = execute_sp3(conn, db_type, dbname, last_run_date, recon_mode)

    logger.info(
        "SP extract complete",
        extra={"sp1_rows": len(sp1), "sp2_rows": len(sp2), "sp3_rows": len(sp3)},
    )
    return sp1, sp2, sp3
