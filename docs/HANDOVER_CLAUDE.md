# Claude Session Handover — novadhruv-data-platform

**Paste this file at the start of a new Claude chat session to restore full project context.**

---

## What this project is

A production-grade, multi-tenant Python data pipeline for year-on-year (YOY) booking analytics. It reads from each client's booking database via stored procedures, writes incremental Parquet fact tables, and builds daily snapshot tables. The snapshots will be queried by a DuckDB + LLM natural language interface (Phase 2, not yet built).

**Project root:** `/Users/rajeevjha/Library/Mobile Documents/com~apple~CloudDocs/dhruv-v2024/python/novadhruv-data-platform/`

---

## Current build status

- **Phase 1: Complete.** All pipeline code is written and on disk. Awaiting connection to a real database.
- **Phase 2: Not started.** DuckDB query layer + Claude API natural language interface. This is the next major piece of work.

---

## Data flow (one sentence per step)

1. `db_connector.py` calls three stored procedures on the tenant's MIS database via pyodbc.
2. `validator.py` filters out invalid/null records before any processing.
3. `fact_pipeline.py` computes a SHA-256 hash per EnquiryNo and runs Cases A–E (new / unchanged / changed-same-partition / changed-partition-moved / hard-delete).
4. `parquet_writer.py` atomically writes Polars DataFrames to partitioned Parquet files (temp file → `os.replace`).
5. `tracking_store.py` writes/reads `dhruvlog.dbo.EnquiryParquetFile` and `dhruvlog.dbo.LastRunTracking` to track state between runs.
6. `snapshot_pipeline.py` reads all fact partitions, applies the forward-bookings filter (BookingDate ≤ today ≤ DepartureDate), computes `RelativeDepartureMonth`, and writes two aggregated snapshot Parquet files per day.

---

## Source system — three stored procedures

The pipeline is read-only against the source DB. All three SPs take `@LastRunDate` and `@ReconModeInd`:

| SP (base name) | Returns |
|----------------|---------|
| `mis_YOYAnalyticsEnquiries` | One row per EnquiryNo (header) |
| `mis_YOYAnalyticsEnquiriesEnqCountries` | One row per EnquiryNo × CountryCode |
| `mis_YOYAnalyticsEnquiriesEnqTags` | One row per EnquiryNo × TagName |

**SP naming rules by db_type (critical):**
- `mssql` → `EXEC {dbname}.dbo.{sp_name} @LastRunDate=?, @ReconModeInd=?`
- `postgres` → `CALL {dbname}.{sp_name}(?, ?)` (schema = dbname)
- `mariadb` → `CALL {dbname}.{sp_name}(?, ?)`

`dbname` is extracted at runtime from the `DATABASE=` / `dbname=` key in the ODBC connection string.

---

## Key design decisions (with rationale)

### Hash-based change detection
The SHA-256 hash covers **all SP columns** (including `ConvertedToQuoteDate` which is excluded from fact tables). This means any field change upstream — even one not stored in Parquet — is detected. Hash is computed as: `SP1_pipe_delimited ||| SP2_rows_sorted_by_CountryCode ||| SP3_rows_sorted_by_TagName`. NULL → literal "NULL". SP2 sorted by CountryCode, SP3 by TagName for determinism.

### Atomic Parquet writes
Every partition write goes through `_atomic_write()` in `parquet_writer.py`: write to `_tmp_{uuid}.parquet` in the same directory, then `os.replace()`. Readers never see a partial file.

### Partition-level granularity
Fact tables are partitioned by `BookingMonth=YYYY-MM`. A changed EnquiryNo rewrites the entire partition it lives in. This is simple but means large partitions are expensive to update.

### dhruvlog is always MSSQL
The tracking database (`dhruvlog`) is a shared MSSQL database on the same server as the tenant MIS. The Python code derives its connection string by replacing `DATABASE=` with `dhruvlog`. The `db_type` field on a tenant config only controls how the MIS stored procedures are called — dhruvlog is always pyodbc/MSSQL.

### Snapshot tables are pre-aggregated and self-contained
Each snapshot row contains all the dimensional context needed to answer a YOY query. This was intentional — Phase 2 DuckDB/LLM queries should need no joins.

---

## The TagName non-additivity rule — most important business constraint

`fact_bookingtags` and Snapshot B hold **full booking value per tag, not distributed**. A booking tagged [Family, Riding] appears in BOTH rows at 100% value each.

**Consequence:** Never `SUM(BookingValue)` across multiple TagNames in Snapshot B. Always filter to exactly one TagName, or `GROUP BY TagName` without summing a grand total. For total values (no tag breakdown), use Snapshot A.

---

## Parquet directory layout

```
{data_root}/
  tenant_id={id}/
    fact_bookingcountries/         BookingMonth=YYYY-MM/data.parquet
    fact_bookingtags/              BookingMonth=YYYY-MM/data.parquet
    fact_bookingcountries_snapshot/            SnapshotDate=YYYY-MM-DD/data.parquet
    fact_bookingcountries_snapshot_country_tag/ SnapshotDate=YYYY-MM-DD/data.parquet
```

---

## Tracking database schema (MSSQL — dhruvlog)

```sql
-- Append-only run log; last row per (TenantId, ProcessName) is used on next run
dhruvlog.dbo.LastRunTracking
  SeqNo IDENTITY, TenantId, ProcessName, LastRunDateTime, UpdatedDateTime

-- One row per TenantId × EnquiryNo; unique index on (TenantId, EnquiryNo)
dhruvlog.dbo.EnquiryParquetFile
  SeqNo IDENTITY, TenantId, EnquiryNo, ParquetFile (relative path), DataHash (SHA-256), CreatedOn, UpdatedOn
```

DDL: `sql/create_dhruvlog_tables.sql`

---

## Snapshot columns

### Snapshot A — `fact_bookingcountries_snapshot`
`SnapshotDate, BookingFY, DepartureFY, RelativeDepartureMonth, TourType, TourGenericCode, CountryCode, CountryName, BookingValue (SUM), NoOfPax (SUM)`

### Snapshot B — `fact_bookingcountries_snapshot_country_tag`
Same as A plus `TagName`.

**RelativeDepartureMonth:** 1 = departure in same calendar month as SnapshotDate, 2 = next month, etc. Range 1–24 only.

**Financial year:** Aug–Jul. `BookingFY` and `DepartureFY` are strings like `"2025-2026"`.

### Snapshot retention policy

Two config parameters control retention:

| Parameter | Default | Meaning |
|-----------|---------|---------|
| `snapshot_retention_days` | 30 | Width of the keep window around each anchor date |
| `snapshot_retention_years` | 5 | Number of prior years to retain |

After each run, the pipeline keeps a 30-day window ending on the current snapshot date, plus the equivalent 30-day window for each of the prior 5 years. Snapshots outside all windows are deleted.

**Example — run on 24/04/2026:** keeps 25/03→24/04 for 2026, 2025, 2024, 2023, 2022, and 2021. Dates between windows (e.g. 25/04/2025 → 24/03/2026) are deleted.

This ensures same-date-prior-year snapshots survive long enough for YOY comparison. A simple rolling 30-day window would delete prior-year history within a month.

---

## File map — where things live

| File | What it does |
|------|--------------|
| `src/models/tenant.py` | `TenantConfig` dataclass: `tenant_id, db_type, encrypted_connection_string, encryption_key_path` |
| `src/connectors/db_connector.py` | SP calls for MSSQL/Postgres/MariaDB; extracts dbname; builds call SQL |
| `src/connectors/mssql_connector.py` | Re-export shim only — superseded by db_connector |
| `src/validation/validator.py` | Validates SP rows before hash computation |
| `src/tracking/tracking_store.py` | All dhruvlog DB reads/writes |
| `src/pipeline/fact_pipeline.py` | Cases A–E incremental logic |
| `src/pipeline/snapshot_pipeline.py` | Snapshot A and B builder |
| `src/io/parquet_writer.py` | Atomic writes; upsert and delete helpers |
| `src/io/parquet_reader.py` | Reads all BookingMonth partitions for snapshot build |
| `src/utils/encryption.py` | Fernet encrypt/decrypt with `lru_cache` |
| `src/utils/logging_config.py` | JSON structured logging; `TenantLoggerAdapter` |
| `run_all_tenants.py` | Production entry point |
| `main.py` | Single-tenant debug runner |
| `Manual/reconciliation_job.py` | Weekly recon job (ReconModeInd=1) |
| `config.yaml` | Runtime config (data_root, log settings, retention) |
| `tenants.yaml` | Tenant definitions (encrypted connection strings + db_type) |
| `sql/create_dhruvlog_tables.sql` | DDL — run once to set up dhruvlog |
| `docs/ARCHITECTURE.md` | Full architecture document for humans |
| `docs/YOY_QUERY_PATTERNS.md` | DuckDB YOY query patterns — overall and by country (top N + Others) |

---

## `tenants.yaml` structure

```yaml
tenants:
  - tenant_id: acme
    db_type: mssql          # mssql | postgres | mariadb
    encrypted_connection_string: "gAAAAAB..."
    encryption_key_path: keys/acme.key
```

Keys are stored in `keys/` directory. Never committed to source control.

---

## Running the pipeline

```bash
# All tenants — production
python run_all_tenants.py

# Single tenant — debugging
python main.py --tenant acme

# Weekly reconciliation
python Manual/reconciliation_job.py
python Manual/reconciliation_job.py --tenant acme
```

---

## Phase 2 — what needs to be built next

1. **DuckDB query layer:** Register snapshot Parquet files as DuckDB views using hive partitioning. Example:
   ```python
   duckdb.sql("CREATE VIEW snap_a AS SELECT * FROM read_parquet('{data_root}/tenant_id=*/fact_bookingcountries_snapshot/**/*.parquet', hive_partitioning=true)")
   ```

2. **LLM SQL translation:** Send a natural language question + schema context to the Claude API. Receive a SQL query over the DuckDB views.

3. **Query interface:** CLI or API that takes free-text input, runs the generated SQL against DuckDB, returns formatted results.

**Phase 2 constraints to communicate to Claude:**
- Query snapshot tables only — no joins between them needed
- Snapshot B requires a single TagName filter or GroupBy without cross-tag summing (TagName non-additivity)
- YOY comparison pattern: query the same RelativeDepartureMonth range at two SnapshotDates (today vs same date last year)
- Use CountryCode for WHERE/dedup; use CountryName for display in answers

---

## Technology stack

- **pyodbc** — DB connectivity (MSSQL, Postgres via psqlODBC, MariaDB via MariaDB ODBC Connector)
- **Polars** — all DataFrame transformations
- **PyArrow** — Parquet writes
- **cryptography (Fernet)** — connection string encryption
- **PyYAML** — config loading
- **python-json-logger** — structured JSON logging
- **DuckDB** — Phase 2 query layer (not yet implemented)
- **Anthropic SDK** — Phase 2 LLM interface (not yet implemented)

---

## Things to watch out for

1. **SP column order is hardcoded** in `db_connector.py` (`SP1_COLUMNS`, `SP2_COLUMNS`, `SP3_COLUMNS`). If the source SPs change column order or add columns, update these lists and re-hash all data (or accept a one-time full reprocessing run by deleting EnquiryParquetFile rows).

2. **Large partitions are expensive.** Changing one enquiry in a busy BookingMonth rewrites the whole partition. Consider splitting hot partitions if performance becomes an issue.

3. **Snapshot is a full rebuild.** Every snapshot run reads all fact partitions. As history grows (years of data), this gets slower. Could be optimised by only re-snapshotting if fact data changed, but currently not done.

4. **Reconciliation LastRunDate is independent.** `YOYReconPipeline` tracks its own LastRunDate separately from `YOYFactPipeline`. If recon is run infrequently, it will do a large volume of work to catch up.

5. **dhruvlog DDL is MSSQL-only.** If the tracking infrastructure needs to move to Postgres/MariaDB, the DDL and `UpdatedOn = GETUTCDATE()` calls in `tracking_store.py` need updating.
