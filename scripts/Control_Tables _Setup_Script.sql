USE DataWarehouse;
GO

/* =============================================================
   1. Migration Mapping Table
   Purpose:
       Dokumentado kung saan galing ang data (source), anong type 
       (SQL View, Power Query, Excel), at saan siya papasok (target schema/table).
============================================================= */
IF OBJECT_ID('dbo.MigrationMapping', 'U') IS NOT NULL
    DROP TABLE dbo.MigrationMapping;
GO

CREATE TABLE dbo.MigrationMapping (
    MappingID INT IDENTITY(1,1) PRIMARY KEY,
    SourceName NVARCHAR(200),          -- e.g., SALES_VIEW
    SourceType NVARCHAR(50),           -- SQL View / Power Query / Excel
    TargetSchema NVARCHAR(50),         -- bronze/silver/gold
    TargetTable NVARCHAR(200),         -- e.g., Sales_Staging
    TransformationNotes NVARCHAR(MAX), -- business rules/cleansing notes
    CreatedAt DATETIME DEFAULT GETDATE()
);
GO


/* =============================================================
   2. ETL_RunLog
   Purpose:
       I-log lahat ng ETL executions (success/fail) per package/job.
============================================================= */
IF OBJECT_ID('dbo.ETL_RunLog', 'U') IS NOT NULL
    DROP TABLE dbo.ETL_RunLog;
GO

CREATE TABLE dbo.ETL_RunLog (
    RunID BIGINT IDENTITY(1,1) PRIMARY KEY,
    PackageName NVARCHAR(200),     -- SSIS package name or SQL Job
    StartTime DATETIME,
    EndTime DATETIME,
    Status NVARCHAR(20),           -- Success / Failed
    RowCount INT,                  -- rows processed
    ErrorMessage NVARCHAR(MAX)     -- if failed
);
GO


/* =============================================================
   3. ETL_Checkpoints
   Purpose:
       Track last incremental load timestamp or ID per table.
============================================================= */
IF OBJECT_ID('dbo.ETL_Checkpoints', 'U') IS NOT NULL
    DROP TABLE dbo.ETL_Checkpoints;
GO

CREATE TABLE dbo.ETL_Checkpoints (
    CheckpointID INT IDENTITY(1,1) PRIMARY KEY,
    SourceTable NVARCHAR(200),     -- e.g., OINV
    LastLoadDate DATETIME,         -- last successful extract date
    LastKeyValue BIGINT NULL,      -- optional (DocEntry, DocNum)
    UpdatedAt DATETIME DEFAULT GETDATE()
);
GO


/* =============================================================
   4. Error Logging Table
   Purpose:
       Kung may data validation/constraint errors, dito ilalagay.
============================================================= */
IF OBJECT_ID('dbo.ETL_ErrorLog', 'U') IS NOT NULL
    DROP TABLE dbo.ETL_ErrorLog;
GO

CREATE TABLE dbo.ETL_ErrorLog (
    ErrorID BIGINT IDENTITY(1,1) PRIMARY KEY,
    SourceTable NVARCHAR(200),
    TargetTable NVARCHAR(200),
    ErrorTime DATETIME DEFAULT GETDATE(),
    ErrorMessage NVARCHAR(MAX),
    FailedRecord NVARCHAR(MAX)     -- store JSON/XML of bad row if possible
);
GO
