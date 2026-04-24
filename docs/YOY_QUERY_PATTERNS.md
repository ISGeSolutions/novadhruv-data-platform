# Year-on-Year Query Patterns

**Version:** 2026-04-24
**Applies to:** Phase 2 — DuckDB query layer (not yet built)

This document describes the two standard YOY comparison query patterns that the
DuckDB + LLM interface must support. Both patterns read from the snapshot Parquet
files via hive partitioning and pivot RelativeDepartureMonth into columns.

---

## Background and terminology

| Term | Meaning |
|------|---------|
| SnapshotDate | The date the snapshot was built. Represents "the forward-bookings position as of this date." |
| RelativeDepartureMonth (RDM) | 1 = departure in the same calendar month as SnapshotDate, 2 = next month, etc. Range 1–24. |
| Year 1 | The prior-year snapshot date (e.g. 2025-04-24) |
| Year 2 | The current snapshot date (e.g. 2026-04-24) |
| Variance £ | Year 2 BookingValue − Year 1 BookingValue (per RDM) |
| Variance % | Variance £ ÷ Year 1 BookingValue × 100 (per RDM) |

**Critical rule:** YOY comparison must always align on RelativeDepartureMonth, not
on absolute DepartureMonth. RDM 6 in 2026 means "6 months out from April 2026"
(September 2026 departures). RDM 6 in 2025 means "6 months out from April 2025"
(September 2025 departures). These are different physical months — that is correct
and expected.

---

## Pattern 1 — Overall YOY (no country breakdown)

### Use case

Total forward-bookings position across all countries, all tour types. Answers:
*"Overall, how are bookings looking compared to the same point last year?"*

### Source table

**Snapshot A** (`fact_bookingcountries_snapshot`) only. Do not use Snapshot B for
overall totals — see TagName non-additivity rule in ARCHITECTURE.md Section 9.

### Query steps

**Step 1 — Filter**
Read Snapshot A for the two SnapshotDates. DuckDB's hive partitioning means only
the two relevant partition folders are opened.

**Step 2 — Aggregate**
SUM(BookingValue) and SUM(NoOfPax), grouped by SnapshotDate × RelativeDepartureMonth.
Apply any optional dimension filters (TourType, TourGenericCode) here before summing.

**Step 3 — Self-join**
Join 2026 rows to 2025 rows on RelativeDepartureMonth to produce one row per RDM
with columns Value_2026 and Value_2025. Use a full outer join so that RDMs present
in one year but not the other are not silently dropped.

**Step 4 — Compute variance**
- Variance £ = Value_2026 − Value_2025
- Variance % = (Value_2026 − Value_2025) ÷ Value_2025 × 100
Guard against divide-by-zero where Value_2025 is zero or null.

**Step 5 — Union into 4 rows**
Stack four rows in this fixed sequence:

| Row | Source |
|-----|--------|
| `2026-04-24` | Value_2026 per RDM |
| `2025-04-24` | Value_2025 per RDM |
| `Variance £` | Variance £ per RDM |
| `Variance %` | Variance % per RDM |

**Step 6 — Pivot**
Rotate RDM 1–24 from rows into columns. Row identifier is the row-type label.

### Result shape

| Row label   | RDM 1 | RDM 2 | RDM 3 | … | RDM 24 |
|-------------|-------|-------|-------|---|--------|
| 2026-04-24  | £X    | £X    | £X    | … | £X     |
| 2025-04-24  | £X    | £X    | £X    | … | £X     |
| Variance £  | £X    | £X    | £X    | … | £X     |
| Variance %  | X%    | X%    | X%    | … | X%     |

### Tag-filtered variant

To restrict to a single tag (e.g. Family bookings only):
- Switch source to **Snapshot B** (`fact_bookingcountries_snapshot_country_tag`)
- Add `WHERE TagName = 'Family'` at Step 1, before any aggregation
- All subsequent steps are identical
- Never add TagName to the GROUP BY in this pattern — that would produce one result
  per tag, breaking non-additivity

---

## Pattern 2 — YOY by country, top N + Others

### Use case

Forward-bookings position broken down by destination country, with the highest-value
countries named individually and the remainder collapsed into an "Others" bucket.
Answers: *"By country, how are bookings looking compared to the same point last year?"*

### Source table

**Snapshot A** for unfiltered country totals. **Snapshot B** if a TagName filter is
applied (same rule as Pattern 1).

### Design decision — ranking countries

Countries are ranked by their **combined BookingValue across both snapshot dates**
(Year 1 + Year 2 summed together). This ensures the same N countries are named in
both years, making the YOY comparison consistent. Ranking by Year 2 alone risks a
country that was large in Year 1 but has declined falling into "Others", which
would distort the YOY picture.

N is configurable. The default recommended value is **10**.

### Query steps

**Step 1 — Filter**
Read Snapshot A for the two SnapshotDates.

**Step 2 — Aggregate base data**
SUM(BookingValue) grouped by SnapshotDate × CountryCode × CountryName × RelativeDepartureMonth.
Apply any optional dimension filters (TourType, TourGenericCode, TagName) here.

**Step 3 — Rank countries**
Sum BookingValue across both SnapshotDates and all RDMs per CountryCode. Rank by
this combined total descending. Top N countries are named; all others are labelled
"Others".

**Step 4 — Fold Others**
Replace CountryCode and CountryName with 'OTH' / 'Others' for all countries ranked
N+1 and below. Re-aggregate Step 2 results, summing BookingValue across all "Others"
countries per SnapshotDate × RelativeDepartureMonth.

Result: N+1 named groups (top N countries + Others), each with one value per
SnapshotDate × RDM.

**Step 5 — Self-join per country group**
For each country group, join 2026 rows to 2025 rows on CountryCode × RelativeDepartureMonth.
Use a full outer join within each group. Treat missing values as zero.

**Step 6 — Compute variance per country group**
Same as Pattern 1 Step 4, computed independently within each country group.

**Step 7 — Union into 4 rows per country group**
For each country group, stack four rows in this fixed sequence:

| Row | Source |
|-----|--------|
| `2026-04-24 [CountryName]` | Value_2026 per RDM |
| `2025-04-24 [CountryName]` | Value_2025 per RDM |
| `Variance £` | Variance £ per RDM |
| `Variance %` | Variance % per RDM |

The "Others" group uses the same four-row structure with `2026-04-24 Others` etc.

**Step 8 — Pivot**
Rotate RDM 1–24 into columns. Row identifier is CountryCode + row-type label.

**Step 9 — Sort**
Order by combined-total rank (top country first, Others always last), then within
each country group by fixed row-type order: Year 2 → Year 1 → Variance £ → Variance %.

### Result shape

| Row label              | RDM 1 | RDM 2 | … | RDM 24 |
|------------------------|-------|-------|---|--------|
| 2026-04-24 India       | £X    | £X    | … | £X     |
| 2025-04-24 India       | £X    | £X    | … | £X     |
| Variance £             | £X    | £X    | … | £X     |
| Variance %             | X%    | X%    | … | X%     |
| 2026-04-24 Kenya       | £X    | £X    | … | £X     |
| 2025-04-24 Kenya       | £X    | £X    | … | £X     |
| Variance £             | £X    | £X    | … | £X     |
| Variance %             | X%    | X%    | … | X%     |
| …                      |       |       |   |        |
| 2026-04-24 Others      | £X    | £X    | … | £X     |
| 2025-04-24 Others      | £X    | £X    | … | £X     |
| Variance £             | £X    | £X    | … | £X     |
| Variance %             | X%    | X%    | … | X%     |

### Tag-filtered variant

Same as Pattern 1's tag-filtered variant: switch to Snapshot B, add a single
TagName filter at Step 1, do not group by TagName.

---

## Shared business rules (both patterns)

| Rule | Detail |
|------|--------|
| Always use RelativeDepartureMonth for alignment | Never compare on absolute DepartureMonth across years |
| Overall totals → Snapshot A | No tag involvement |
| Single-tag filter → Snapshot B | Add `WHERE TagName = ?` before any aggregation |
| Never GROUP BY TagName without a WHERE TagName filter | Double-counts across tags |
| Join on CountryCode, display CountryName | CountryCode is stable; CountryName can change in source |
| Full outer join across years | Prevents silent loss of RDMs present in one year only |
| Others always ranked last | Regardless of combined total |
| Divide-by-zero on Variance % | Treat as null/N/A where Year 1 value is zero |

---

## Configuration parameters

| Parameter | Where set | Default | Notes |
|-----------|-----------|---------|-------|
| Top-N countries | Query parameter | 10 | Countries ranked by combined BookingValue across both years |
| SnapshotDates | Derived at query time | today + (today − 1 year) | Same calendar date one year apart |
| RDM range | Fixed | 1–24 | Matches snapshot build horizon |
