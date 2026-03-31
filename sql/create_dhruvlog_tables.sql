-- DDL for dhruvlog database tables
-- Run this once against the dhruvlog database on the tenant's MSSQL server.

USE dhruvlog;
GO

-- ============================================================
-- Table: LastRunTracking
-- Tracks the last successful pipeline run per tenant/process.
-- ============================================================
CREATE TABLE dbo.LastRunTracking (
    SeqNo           int             NOT NULL IDENTITY(1,1),
    TenantId        nvarchar(50)    NOT NULL,
    ProcessName     nvarchar(25)    NOT NULL,
    LastRunDateTime datetime2       NOT NULL,
    UpdatedDateTime datetime2       NOT NULL DEFAULT (getutcdate()),
    CONSTRAINT PK_LastRunTracking PRIMARY KEY (SeqNo)
);
GO

CREATE INDEX IX_LastRunTracking_TenantId_ProcessName
    ON dbo.LastRunTracking (TenantId, ProcessName, LastRunDateTime DESC);
GO


-- ============================================================
-- Table: EnquiryParquetFile
-- Tracks each enquiry's Parquet partition path and data hash.
-- Enables real-time hash-based change detection on every run.
-- ============================================================
CREATE TABLE dbo.EnquiryParquetFile (
    SeqNo       int             NOT NULL IDENTITY(1,1),
    TenantId    nvarchar(50)    NOT NULL,
    EnquiryNo   int             NOT NULL,
    ParquetFile nvarchar(500)   NOT NULL,   -- relative path, e.g. fact_bookingcountries/BookingMonth=2025-11
    DataHash    nvarchar(64)    NOT NULL,   -- SHA-256 hex string
    CreatedOn   datetime2       NOT NULL DEFAULT (getutcdate()),
    UpdatedOn   datetime2       NOT NULL DEFAULT (getutcdate()),
    CONSTRAINT PK_EnquiryParquetFile PRIMARY KEY (SeqNo)
);
GO

-- Unique index enforces one row per TenantId + EnquiryNo (business rule).
CREATE UNIQUE INDEX IX_EnquiryParquetFile_TenantId_EnquiryNo
    ON dbo.EnquiryParquetFile (TenantId, EnquiryNo);
GO
