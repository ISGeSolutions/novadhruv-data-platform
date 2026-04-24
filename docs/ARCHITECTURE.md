# novadhruv-data-platform — Architecture Document

**Version:** 2026-03-31
**Status:** Phase 1 complete. Phase 2 (DuckDB + LLM query interface) not yet built.

---

## 1. Purpose

This system is a **multi-tenant, incremental data pipeline** for year-on-year (YOY) booking analytics. It reads from each tenant's booking database via stored procedures, transforms the data into Parquet files on disk, and builds daily snapshot tables that capture the forward-looking bookings position for any given date.

The end goal is to answer questions like:

> *"Compared to the same date last year, how are forward bookings looking for departures in the next 12 months?"*

---

## 2. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  Tenant Source Database  (MSSQL / Postgres / MariaDB)               │
│                                                                     │
│  mis_YOYAnalyticsEnquiries          (SP1 — enquiry header)          │
│  mis_YOYAnalyticsEnquiriesEnqCountries  (SP2 — country breakdown)   │
│  mis_YOYAnalyticsEnquiriesEnqTags   (SP3 — tag breakdown)           │
└────────────────────┬────────────────────────────────────────────────┘
                     │  pyodbc (ODBC)
                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│  db_connector.py                                                    │
│  • Builds qualified SP call SQL based on db_type                    │
│  • Extracts dbname from connection string                           │
│  • Returns raw rows as list[dict]                                   │
└────────────────────┬────────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│  validator.py                                                       │
│  • SP1: EnquiryNo > 0, BookingDate not null, BookingValue >= 0      │
│  • SP2: EnquiryNo in valid SP1 set, no duplicate (No, Country)     │
│  • SP3: EnquiryNo in valid SP1 set, no duplicate (No, Tag)         │
└────────────────────┬────────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│  fact_pipeline.py  — Hash-based incremental load (Cases A–E)        │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  dhruvlog DB (MSSQL)  — EnquiryParquetFile tracking table    │  │
│  │  Stores: TenantId, EnquiryNo, ParquetFile path, SHA-256 hash │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                     │
│  Per EnquiryNo:                                                     │
│    A — New → write Parquet + INSERT tracking                        │
│    B — Hash unchanged → skip                                        │
│    C — Hash changed, same partition → rewrite partition + UPDATE    │
│    D — Hash changed, partition moved → delete old, write new + UPD  │
│    E — Zero-value + in tracking → delete Parquet + DELETE tracking  │
└────────────────────┬────────────────────────────────────────────────┘
                     │  Polars + PyArrow (atomic writes)
                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Parquet storage  (local filesystem or mounted volume)              │
│                                                                     │
│  {data_root}/                                                       │
│    tenant_id={id}/                                                  │
│      fact_bookingcountries/                                         │
│        BookingMonth=YYYY-MM/data.parquet                            │
│      fact_bookingtags/                                              │
│        BookingMonth=YYYY-MM/data.parquet                            │
└────────────────────┬────────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│  snapshot_pipeline.py  — Daily snapshot builder + auto-backfill     │
│                                                                     │
│  Snapshot A: fact_bookingcountries_snapshot                         │
│    Filter: BookingDate ≤ SnapshotDate AND DepartureDate ≥ SnapshotDate│
│    Horizon: RelativeDepartureMonth 1–24                             │
│    Aggregated by country                                            │
│                                                                     │
│  Snapshot B: fact_bookingcountries_snapshot_country_tag             │
│    = Snapshot A INNER JOIN fact_bookingtags on EnquiryNo            │
│    Adds TagName dimension                                           │
│                                                                     │
│  Auto-backfill: after writing today's snapshot, checks prior-year   │
│    equivalent dates (same MM-DD, years 1–N) and generates any that  │
│    are absent — fact data read once and reused across all dates.     │
│                                                                     │
│  Written to:                                                        │
│    {data_root}/tenant_id={id}/fact_bookingcountries_snapshot/       │
│      SnapshotDate=YYYY-MM-DD/data.parquet                           │
│    (old partitions pruned per multi-year retention windows)         │
└────────────────────┬────────────────────────────────────────────────┘
                     │
                     ▼
             [Phase 2 — NOT YET BUILT]
┌─────────────────────────────────────────────────────────────────────┐
│  DuckDB query layer + LLM natural language interface                │
│  Reads snapshot Parquet files via hive partitioning                 │
│  Claude API translates natural language → SQL                       │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 3. Source System — Stored Procedures

Each tenant exposes **three stored procedures** in their MIS database. The Python pipeline only reads — it never writes to the source database.

| SP | Purpose | Key columns |
|----|---------|-------------|
| SP1 `mis_YOYAnalyticsEnquiries` | One row per EnquiryNo. Enquiry lifecycle data. | EnquiryNo, BookingDate, DepartureDate, ReturnDate, TourType, TourGenericCode, BookingValue, NoOfPax, ConvertedToQuoteDate, ClientCode |
| SP2 `mis_YOYAnalyticsEnquiriesEnqCountries` | One row per EnquiryNo × CountryCode. Country-distributed values. | EnquiryNo, CountryCode, CountryName, BookingValue, NoOfPax |
| SP3 `mis_YOYAnalyticsEnquiriesEnqTags` | One row per EnquiryNo × TagName. Full enquiry-level values per tag. | EnquiryNo, TagName, BookingValue, NoOfPax |

All three SPs take two parameters: `@LastRunDate datetime2` and `@ReconModeInd bit`.

- **`@LastRunDate`:** Only records modified since this date are returned. The pipeline reads the last successful run datetime from the dhruvlog tracking table to set this. On first run, it passes `1900-01-01` to get the full dataset.
- **`@ReconModeInd`:** `0` = incremental (daily). `1` = reconciliation (weekly full scan).

**SP naming by database type:**

| db_type | Call syntax |
|---------|------------|
| `mssql` | `EXEC {dbname}.dbo.{sp_name} @LastRunDate=?, @ReconModeInd=?` |
| `postgres` | `CALL {dbname}.{sp_name}(?, ?)` — schema-name equals dbname |
| `mariadb` | `CALL {dbname}.{sp_name}(?, ?)` |

The `dbname` is extracted dynamically from the `DATABASE=` (or `dbname=`) key in each tenant's ODBC connection string.

---

## 4. Multi-Tenant Design

Each tenant is an isolated unit. Isolation is enforced at every layer:

- **Config:** Each tenant has its own encrypted connection string and Fernet key file in `tenants.yaml`.
- **Parquet paths:** All files are stored under `tenant_id={id}/` as the root partition.
- **Tracking DB:** Every dhruvlog query includes `TenantId` in the `WHERE` clause.
- **Execution:** Tenants run sequentially. A failure in one tenant does not abort others.
- **DB type:** Each tenant independently declares `db_type: mssql | postgres | mariadb`.

---

## 5. Fact Tables

The two fact tables are the canonical source of truth for booking data. They are never queried directly by end-users — the snapshot tables are built from them.

### `fact_bookingcountries`

**Grain:** EnquiryNo × CountryCode
**Partition key:** BookingMonth (YYYY-MM, derived from BookingDate)

| Column | Type | Source |
|--------|------|--------|
| EnquiryNo | int | SP1 |
| CountryCode | string | SP2 |
| CountryName | string | SP2 |
| BookingValue | float | SP2 (country-distributed value) |
| NoOfPax | int | SP2 (country-distributed) |
| BookingDate | date | SP1 |
| DepartureDate | date | SP1 |
| TourType | string | SP1 |
| TourGenericCode | string | SP1 |
| BookingMonth | string YYYY-MM | derived from BookingDate |
| BookingFY | string e.g. "2025-2026" | derived (Aug–Jul financial year) |
| DepartureMonth | string YYYY-MM | derived from DepartureDate |
| DepartureFY | string | derived |

### `fact_bookingtags`

**Grain:** EnquiryNo × TagName
**Partition key:** BookingMonth (same as `fact_bookingcountries` for the same EnquiryNo)

| Column | Type | Source |
|--------|------|--------|
| EnquiryNo | int | SP1 |
| TagName | string | SP3 |
| BookingValue | float | SP3 (full enquiry-level value — NOT distributed) |
| NoOfPax | int | SP3 |
| BookingDate | date | SP1 |
| DepartureDate | date | SP1 |
| TourType | string | SP1 |
| TourGenericCode | string | SP1 |
| BookingMonth | string YYYY-MM | derived |
| BookingFY | string | derived |
| DepartureMonth | string YYYY-MM | derived |
| DepartureFY | string | derived |

**Important:** `fact_bookingtags.BookingValue` holds the **full enquiry value per tag** — it is NOT country-distributed. A booking tagged with [Family, Riding] appears twice, each row holding 100% of the booking value. This is correct for the source data model but must be handled carefully in queries (see Section 9).

---

## 6. Hash-Based Incremental Load (Cases A–E)

The pipeline computes a SHA-256 hash over all SP columns for each EnquiryNo and compares it to the stored hash in `dhruvlog.dbo.EnquiryParquetFile`. This detects changes without needing a database change-tracking mechanism.

**Hash construction:**

```
hash = SHA-256(
    SP1_columns_pipe_delimited
    |||
    SP2_rows_sorted_by_CountryCode_pipe_delimited
    |||
    SP3_rows_sorted_by_TagName_pipe_delimited
)
```

- NULL values are represented as the literal string `"NULL"`.
- SP2 rows are sorted by CountryCode, SP3 by TagName, for determinism.
- The hash covers **all SP columns** including `ConvertedToQuoteDate`, which is excluded from the fact tables. Any upstream change to any field triggers a re-write.

**The five cases:**

| Case | Condition | Action |
|------|-----------|--------|
| A — New | EnquiryNo not in tracking table | Write to both fact tables + INSERT tracking row |
| B — Unchanged | Hash matches stored hash | Skip — no writes |
| C — Changed, same month | Hash differs, BookingMonth unchanged | Rewrite partition for both tables + UPDATE hash |
| D — Changed, month moved | Hash differs, BookingMonth changed | Delete from old partition, write to new + UPDATE path + hash |
| E — Hard delete | BookingValue=0 AND NoOfPax=0, found in tracking | Delete rows from both fact tables + DELETE tracking row |

**Zero-value records not in tracking** (BookingValue=0, NoOfPax=0, not in dhruvlog) are silently skipped — they represent enquiries that were never converted.

---

## 7. Parquet Storage Layout

```
{data_root}/
  tenant_id=acme/
    fact_bookingcountries/
      BookingMonth=2025-09/data.parquet
      BookingMonth=2025-10/data.parquet
      ...
    fact_bookingtags/
      BookingMonth=2025-09/data.parquet
      ...
    fact_bookingcountries_snapshot/
      SnapshotDate=2026-03-25/data.parquet
      SnapshotDate=2026-03-26/data.parquet
      ...
    fact_bookingcountries_snapshot_country_tag/
      SnapshotDate=2026-03-25/data.parquet
      ...
  tenant_id=globetrotter/
    ...
```

All writes use an **atomic temp-file-then-rename** pattern (`os.replace`). Readers never see a partial file. Compression is Snappy.

---

## 8. Snapshot Tables

Snapshots are built daily. Each run is a **full rebuild** for the given SnapshotDate — they are idempotent. Old partitions are pruned after each run using a **multi-year retention policy** (see below).

**Auto-backfill of prior-year dates:** After writing today's snapshot, the pipeline automatically checks whether a snapshot exists for the same calendar date in each of the prior `snapshot_retention_years` years (e.g. running on 24/04/2026 also checks 24/04/2025 through 24/04/2021). Any missing dates are generated in the same run, reusing the already-loaded fact DataFrames. This eliminates the need to manually backfill prior-year Parquet files when deploying the pipeline for the first time or when a date was previously missed. Once generated, a prior-year partition is not regenerated on subsequent runs (existence check on disk).

### Snapshot A — `fact_bookingcountries_snapshot`

The country-level forward-bookings view as of a specific date.

**Filter applied at build time:**
- `BookingDate ≤ SnapshotDate` — booking was made on or before the snapshot
- `DepartureDate ≥ SnapshotDate` — departure is still in the future
- `RelativeDepartureMonth` between 1 and 24

**RelativeDepartureMonth** is computed as:
```
RelativeDepartureMonth = (DepartureMonth - SnapshotDate month) + 1
Month 1 = the calendar month containing SnapshotDate
Month 2 = the following month, etc.
```

**Aggregated by:** SnapshotDate, BookingFY, DepartureFY, RelativeDepartureMonth, TourType, TourGenericCode, CountryCode, CountryName
**Measures:** SUM(BookingValue), SUM(NoOfPax)

### Snapshot B — `fact_bookingcountries_snapshot_country_tag`

Same as Snapshot A plus a TagName dimension. Built by INNER JOINing `fact_bookingcountries` with `fact_bookingtags` on EnquiryNo.

**Measures come from `fact_bookingcountries`** (country-distributed values), NOT from `fact_bookingtags`. Bookings with no tags are excluded from Snapshot B.

**Aggregated by:** everything in Snapshot A + TagName

### Retention Policy

The retention policy is designed for **year-on-year comparison**, not a simple rolling window.

Two configuration parameters control it:

| Parameter | Default | Meaning |
|-----------|---------|---------|
| `snapshot_retention_days` | 30 | Width of the keep window around each anchor date |
| `snapshot_retention_years` | 5 | Number of prior years to retain (in addition to the current year) |

After each run, the pipeline keeps a 30-day window anchored on the current snapshot date, and an equivalent 30-day window anchored on the same calendar date for each of the prior 5 years. Everything outside these windows is deleted.

**Example — run on 24/04/2026, retention_days=30, retention_years=5:**

| Year | Keep window |
|------|-------------|
| 2026 (current) | 25/03/2026 → 24/04/2026 |
| 2025 | 25/03/2025 → 24/04/2025 |
| 2024 | 25/03/2024 → 24/04/2024 |
| 2023 | 25/03/2023 → 24/04/2023 |
| 2022 | 25/03/2022 → 24/04/2022 |
| 2021 | 25/03/2021 → 24/04/2021 |

Snapshots between windows (e.g. 25/04/2025 → 24/03/2026) are deleted, as are snapshots older than the oldest window. This ensures the same-date-prior-year snapshot is always available for YOY comparison at any point within the 30-day current window.

---

## 9. Critical Business Rule — TagName Non-Additivity

**This is the most important query-time constraint.** It applies to Snapshot B and to `fact_bookingtags` directly.

BookingValue in Snapshot B is **not additive across TagNames**. A booking with tags [Family, Riding] appears in **both** the Family row and the Riding row, each carrying the full booking value. Summing across multiple tags double-counts.

| Query pattern | Valid? |
|---------------|--------|
| `SUM(BookingValue) WHERE TagName = 'Family'` | Yes |
| `SUM(BookingValue) GROUP BY CountryCode WHERE TagName = 'Family'` | Yes |
| `SUM(BookingValue) GROUP BY TagName` (to get a total) | **No — double-counts** |
| `SUM(BookingValue)` with no tag filter on Snapshot B | **No — double-counts** |

For total values without tag breakdown, always use **Snapshot A**.

---

## 10. Tracking Database (dhruvlog)

The `dhruvlog` database lives on the **same server as the tenant's MIS database**. The pipeline derives its connection string by replacing the `DATABASE=` component of the tenant's MSSQL connection string.

> Note: dhruvlog is always MSSQL regardless of the tenant's db_type. The tenant's `db_type` affects only the SP calls to the MIS database.

**Two tables:**

### `dhruvlog.dbo.LastRunTracking`

Tracks the last successful pipeline run per tenant per process. Append-only; the most recent row is used on the next run.

| Column | Description |
|--------|-------------|
| SeqNo | Identity PK |
| TenantId | Tenant identifier |
| ProcessName | `YOYFactPipeline`, `YOYSnapshotPipeline`, or `YOYReconPipeline` |
| LastRunDateTime | UTC timestamp of successful run |

### `dhruvlog.dbo.EnquiryParquetFile`

One row per TenantId × EnquiryNo. Stores the partition path and hash for change detection.

| Column | Description |
|--------|-------------|
| TenantId | Tenant identifier |
| EnquiryNo | Booking reference number |
| ParquetFile | Relative path, e.g. `fact_bookingcountries/BookingMonth=2025-11` |
| DataHash | SHA-256 hex string (64 chars) |
| CreatedOn / UpdatedOn | Timestamps |

The DDL to create both tables is in `sql/create_dhruvlog_tables.sql`.

---

## 11. Security — Connection String Encryption

Each tenant's ODBC connection string is encrypted using **Fernet symmetric encryption** (`cryptography` library).

- Each tenant has its own key file (e.g. `keys/acme.key`).
- Keys are never committed to source control.
- The encrypted value is what lives in `tenants.yaml`.
- `EncryptionUtility` caches Fernet instances with `@lru_cache` — the key file is read from disk only once per process.

**To add a new tenant:**
```python
from src.utils.encryption import EncryptionUtility
key = EncryptionUtility.generate_key()
EncryptionUtility.save_key(key, "keys/newclient.key")
encrypted = EncryptionUtility.encrypt_value(
    "DRIVER={...};SERVER=...;DATABASE=mis;UID=...;PWD=...",
    "keys/newclient.key"
)
print(encrypted)  # paste this into tenants.yaml
```

---

## 12. Configuration Files

### `config.yaml` (runtime config — not tenant-specific)

```yaml
data_root: /data/novadhruv        # Root for all Parquet output
tenants_file: tenants.yaml        # Path to tenant definitions
log_level: INFO                   # DEBUG | INFO | WARNING | ERROR
log_format: json                  # json | text
# log_file: /var/log/...          # Optional; stdout only if omitted
# snapshot_date: "2026-03-21"     # Override today's date (backfill)
snapshot_retention_days: 30       # Width of keep window around each anchor date
snapshot_retention_years: 5       # Prior years to retain for YOY comparison
```

### `tenants.yaml` (per-tenant config — secrets encrypted)

```yaml
tenants:
  - tenant_id: acme
    db_type: mssql               # mssql | postgres | mariadb
    encrypted_connection_string: "gAAAAAB..."
    encryption_key_path: keys/acme.key
```

---

## 13. Running the Pipeline

### Daily production run (all tenants)
```bash
python run_all_tenants.py
python run_all_tenants.py --config /path/to/config.yaml
```

Writes a `{data_root}/run_summary_{date}.json` after completion. Exits with code 1 if any tenant fails.

### Single tenant (debugging)
```bash
python main.py --tenant acme
python main.py --tenant acme --config config.yaml
```

### Weekly reconciliation (safety net)
```bash
python Manual/reconciliation_job.py
python Manual/reconciliation_job.py --tenant acme   # single tenant
```

Uses `@ReconModeInd=1` and its own `YOYReconPipeline` LastRunDate, independent of the daily pipeline. Processes through the same Cases A–E logic.

---

## 14. Logging

All log output is structured JSON (configurable to plain text). Every log record from a pipeline run includes `tenant_id` and `stage` fields via `TenantLoggerAdapter`.

Key log events:
- SP row counts after extraction
- Validation pass/fail counts
- Case A/B/C/D/E counts after fact pipeline
- Snapshot A/B row counts
- Per-tenant errors with full stack trace

---

## 15. Project File Structure

```
novadhruv-data-platform/
├── config.yaml                    # Runtime config (edit for each environment)
├── tenants.yaml                   # Tenant list (encrypted connection strings)
├── run_all_tenants.py             # Production entry point
├── main.py                        # Single-tenant debug runner
├── keys/                          # Fernet key files (NOT in source control)
│   ├── acme.key
│   └── globetrotter.key
├── sql/
│   └── create_dhruvlog_tables.sql # Run once to set up dhruvlog DB
├── Manual/
│   └── reconciliation_job.py      # Weekly recon job (run manually or via scheduler)
├── src/
│   ├── models/
│   │   └── tenant.py              # TenantConfig dataclass
│   ├── connectors/
│   │   ├── db_connector.py        # Multi-DB SP calls (MSSQL / Postgres / MariaDB)
│   │   └── mssql_connector.py     # Backward-compat re-export shim only
│   ├── validation/
│   │   └── validator.py           # SP row validation before hash/write
│   ├── tracking/
│   │   └── tracking_store.py      # dhruvlog DB read/write operations
│   ├── pipeline/
│   │   ├── fact_pipeline.py       # Incremental Cases A–E logic
│   │   └── snapshot_pipeline.py   # Daily snapshot builder
│   ├── io/
│   │   ├── parquet_writer.py      # Atomic partition writes + snapshot writes
│   │   └── parquet_reader.py      # Read all fact partitions for snapshot build
│   └── utils/
│       ├── encryption.py          # Fernet encrypt/decrypt with lru_cache
│       └── logging_config.py      # JSON logging, TenantLoggerAdapter
└── docs/
    ├── ARCHITECTURE.md            # This document
    ├── HANDOVER_CLAUDE.md         # Claude AI session handover file
    ├── YOY_QUERY_PATTERNS.md      # DuckDB YOY query patterns (overall + by country)
    └── Generate Multi-Tenant Python Data Pipeline AI Prompt.md  # Original spec
```

---

## 16. Technology Stack

| Concern | Library |
|---------|---------|
| DB connectivity | pyodbc (ODBC driver for MSSQL, Postgres, MariaDB) |
| DataFrame operations | Polars |
| Parquet I/O | PyArrow |
| Encryption | cryptography (Fernet) |
| Config loading | PyYAML |
| Logging | python-json-logger |
| Phase 2 query layer | DuckDB (not yet built) |
| Phase 2 LLM interface | Claude API via Anthropic SDK (not yet built) |

---

## 17. Phase 2 — Not Yet Built

Phase 2 adds a natural language query interface on top of the snapshot Parquet files:

1. **DuckDB query layer** — registers snapshot Parquet files as DuckDB views with hive partitioning.
2. **LLM SQL translation** — Claude API receives a natural language question and returns a SQL query over the DuckDB views.
3. **Query interface** — a service (CLI or API) that takes a free-text question and returns formatted results.

Key design constraint: Phase 2 queries **snapshot tables only** (Snapshot A and B). No joins between snapshot tables are needed — each row is self-contained. The TagName non-additivity rule (Section 9) must be enforced in all generated SQL.

The snapshot tables were intentionally designed with flat, self-describing column names to make LLM SQL generation straightforward.

---

## 18. Known Constraints and Gotchas

1. **Fact pipeline is write-heavy:** Each changed EnquiryNo rewrites an entire BookingMonth partition. Partitions with thousands of enquiries will be read, modified, and rewritten in full. This is intentional (simplicity over performance) but may need revisiting at scale.

2. **dhruvlog is always MSSQL:** The tracking database DDL (`create_dhruvlog_tables.sql`) is MSSQL-only. If the infrastructure shifts away from MSSQL entirely, this DDL needs porting.

3. **Snapshot rebuild cost:** Each snapshot run reads all fact partitions for the tenant once, then reuses those DataFrames for today's snapshot and any auto-backfill dates. As history grows, the single read pass becomes more expensive. Partition pruning (`snapshot_retention_days`) only applies to snapshots, not to fact tables.

4. **Reconciliation is a safety net only:** It uses a separate LastRunDate (`YOYReconPipeline`). If it falls significantly behind the daily pipeline, it will re-process a large volume. It should be run weekly at most.

5. **No schema evolution:** The SP column lists (`SP1_COLUMNS`, `SP2_COLUMNS`, `SP3_COLUMNS`) are hardcoded in `db_connector.py`. If the source SPs add or rename columns, these lists and the hash logic must be updated together.

6. **keys/ directory must not be committed:** The Fernet key files are the only secret material. They must be distributed to the production server out-of-band (not via git).
