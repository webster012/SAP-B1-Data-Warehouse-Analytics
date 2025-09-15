USE DataWarehouse;
GO

/***********************************************
  STAGING + MERGE PROCS for ALL BRONZE TABLES
  Tables covered:
   - OINV, INV1
   - ORIN, RIN1
   - OCRD
   - OITM
   - OITB
   - NNM1
   - OWTR, WTR1
   - OPCH, PCH1
   - SAT, SCT
   - ORDN
************************************************/

/* -----------------------------------------------------------
   1) OINV (stg_OINV + sp_Merge_OINV)
   ----------------------------------------------------------- */
IF OBJECT_ID('bronze.stg_OINV','U') IS NOT NULL DROP TABLE bronze.stg_OINV;
CREATE TABLE bronze.stg_OINV (
    DocEntry INT,
    DocNum NVARCHAR(50),
    DocDate DATE,
    CardCode NVARCHAR(50),
    CardName NVARCHAR(200),
    DocTotal DECIMAL(18,2),
    DocType NVARCHAR(20),
    CANCELED CHAR(1),
    Series INT,
    UpdateDate DATETIME
);
GO

IF OBJECT_ID('dbo.sp_Merge_OINV','P') IS NOT NULL DROP PROCEDURE dbo.sp_Merge_OINV;
GO
CREATE PROCEDURE dbo.sp_Merge_OINV
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @StartTime DATETIME = GETDATE(), @Row_Count INT = 0, @ErrMsg NVARCHAR(MAX);

    BEGIN TRY
        MERGE bronze.OINV AS tgt
        USING bronze.stg_OINV AS src
            ON tgt.DocEntry = src.DocEntry
        WHEN MATCHED AND ISNULL(tgt.UpdateDate,'19000101') < ISNULL(src.UpdateDate,'19000101') THEN
            UPDATE SET
                DocNum = src.DocNum, DocDate = src.DocDate, CardCode = src.CardCode, CardName = src.CardName,
                DocTotal = src.DocTotal, DocType = src.DocType, CANCELED = src.CANCELED, Series = src.Series, UpdateDate = src.UpdateDate
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (DocEntry, DocNum, DocDate, CardCode, CardName, DocTotal, DocType, CANCELED, Series, UpdateDate)
            VALUES (src.DocEntry, src.DocNum, src.DocDate, src.CardCode, src.CardName, src.DocTotal, src.DocType, src.CANCELED, src.Series, src.UpdateDate);

        SET @Row_Count = @Row_Count;

        -- update checkpoint using max UpdateDate from permanent table
        MERGE dbo.ETL_Checkpoints AS chk
        USING (SELECT 'OINV' AS SourceTable, MAX(UpdateDate) AS LastLoadDate FROM bronze.OINV) AS srcchk
            ON chk.SourceTable = srcchk.SourceTable
        WHEN MATCHED THEN UPDATE SET LastLoadDate = srcchk.LastLoadDate, UpdatedAt = GETDATE()
        WHEN NOT MATCHED THEN INSERT (SourceTable, LastLoadDate, UpdatedAt) VALUES (srcchk.SourceTable, srcchk.LastLoadDate, GETDATE());

        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, Row_Count)
        VALUES ('sp_Merge_OINV', @StartTime, GETDATE(), 'Success', @Row_Count);
    END TRY
    BEGIN CATCH
        SET @ErrMsg = ERROR_MESSAGE();
        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, Row_Count, ErrorMessage)
        VALUES ('sp_Merge_OINV', @StartTime, GETDATE(), 'Failed', 0, @ErrMsg);
        INSERT INTO dbo.ETL_ErrorLog (SourceTable, TargetTable, ErrorMessage)
        VALUES ('stg_OINV', 'bronze.OINV', @ErrMsg);
        THROW;
    END CATCH
END;
GO

/* -----------------------------------------------------------
   2) INV1 (stg_INV1 + sp_Merge_INV1)
   ----------------------------------------------------------- */
IF OBJECT_ID('bronze.stg_INV1','U') IS NOT NULL DROP TABLE bronze.stg_INV1;
CREATE TABLE bronze.stg_INV1 (
    DocEntry INT,
    LineNum INT,
    ItemCode NVARCHAR(50),
    Dscription NVARCHAR(200),
    LineTotal DECIMAL(18,2),
    GrssProfit DECIMAL(18,2),
    StockPrice DECIMAL(18,6),
    Price DECIMAL(18,6),
    Quantity DECIMAL(18,6),
    WhsCode NVARCHAR(50)
);
GO

IF OBJECT_ID('dbo.sp_Merge_INV1','P') IS NOT NULL DROP PROCEDURE dbo.sp_Merge_INV1;
GO
CREATE PROCEDURE dbo.sp_Merge_INV1
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @StartTime DATETIME = GETDATE(), @Row_Count INT = 0, @ErrMsg NVARCHAR(MAX);

    BEGIN TRY
        MERGE bronze.INV1 AS tgt
        USING bronze.stg_INV1 AS src
            ON tgt.DocEntry = src.DocEntry AND tgt.LineNum = src.LineNum
        WHEN MATCHED THEN
            UPDATE SET ItemCode = src.ItemCode, Dscription = src.Dscription, LineTotal = src.LineTotal,
                       GrssProfit = src.GrssProfit, StockPrice = src.StockPrice, Price = src.Price,
                       Quantity = src.Quantity, WhsCode = src.WhsCode
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (DocEntry, LineNum, ItemCode, Dscription, LineTotal, GrssProfit, StockPrice, Price, Quantity, WhsCode)
            VALUES (src.DocEntry, src.LineNum, src.ItemCode, src.Dscription, src.LineTotal, src.GrssProfit, src.StockPrice, src.Price, src.Quantity, src.WhsCode);

        SET @Row_Count = @Row_Count;

        MERGE dbo.ETL_Checkpoints AS chk
        USING (SELECT 'INV1' AS SourceTable, GETDATE() AS LastLoadDate) AS srcchk
            ON chk.SourceTable = srcchk.SourceTable
        WHEN MATCHED THEN UPDATE SET LastLoadDate = srcchk.LastLoadDate, UpdatedAt = GETDATE()
        WHEN NOT MATCHED THEN INSERT (SourceTable, LastLoadDate, UpdatedAt) VALUES (srcchk.SourceTable, srcchk.LastLoadDate, GETDATE());

        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, Row_Count)
        VALUES ('sp_Merge_INV1', @StartTime, GETDATE(), 'Success', @Row_Count);
    END TRY
    BEGIN CATCH
        SET @ErrMsg = ERROR_MESSAGE();
        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, Row_Count, ErrorMessage)
        VALUES ('sp_Merge_INV1', @StartTime, GETDATE(), 'Failed', 0, @ErrMsg);
        INSERT INTO dbo.ETL_ErrorLog (SourceTable, TargetTable, ErrorMessage)
        VALUES ('stg_INV1', 'bronze.INV1', @ErrMsg);
        THROW;
    END CATCH
END;
GO

/* -----------------------------------------------------------
   3) ORIN (Credit Notes Header)
   ----------------------------------------------------------- */
IF OBJECT_ID('bronze.stg_ORIN','U') IS NOT NULL DROP TABLE bronze.stg_ORIN;
CREATE TABLE bronze.stg_ORIN (
    DocEntry INT,
    DocNum NVARCHAR(50),
    DocDate DATE,
    CardCode NVARCHAR(50),
    CardName NVARCHAR(200),
    DocTotal DECIMAL(18,2),
    CANCELED CHAR(1),
    UpdateDate DATETIME
);
GO

IF OBJECT_ID('dbo.sp_Merge_ORIN','P') IS NOT NULL DROP PROCEDURE dbo.sp_Merge_ORIN;
GO
CREATE PROCEDURE dbo.sp_Merge_ORIN
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @StartTime DATETIME = GETDATE(), @Row_Count INT = 0, @ErrMsg NVARCHAR(MAX);
    BEGIN TRY
        MERGE bronze.ORIN tgt
        USING bronze.stg_ORIN src
            ON tgt.DocEntry = src.DocEntry
        WHEN MATCHED AND ISNULL(tgt.UpdateDate,'19000101') < ISNULL(src.UpdateDate,'19000101') THEN
            UPDATE SET DocNum = src.DocNum, DocDate = src.DocDate, CardCode = src.CardCode, CardName = src.CardName,
                       DocTotal = src.DocTotal, CANCELED = src.CANCELED, UpdateDate = src.UpdateDate
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (DocEntry, DocNum, DocDate, CardCode, CardName, DocTotal, CANCELED, UpdateDate)
            VALUES (src.DocEntry, src.DocNum, src.DocDate, src.CardCode, src.CardName, src.DocTotal, src.CANCELED, src.UpdateDate);

        SET @Row_Count = @Row_Count;

        MERGE dbo.ETL_Checkpoints chk
        USING (SELECT 'ORIN' AS SourceTable, MAX(UpdateDate) AS LastLoadDate FROM bronze.ORIN) srcchk
            ON chk.SourceTable = srcchk.SourceTable
        WHEN MATCHED THEN UPDATE SET LastLoadDate = srcchk.LastLoadDate, UpdatedAt = GETDATE()
        WHEN NOT MATCHED THEN INSERT (SourceTable, LastLoadDate, UpdatedAt) VALUES (srcchk.SourceTable, srcchk.LastLoadDate, GETDATE());

        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, Row_Count)
        VALUES ('sp_Merge_ORIN', @StartTime, GETDATE(), 'Success', @Row_Count);
    END TRY
    BEGIN CATCH
        SET @ErrMsg = ERROR_MESSAGE();
        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, Row_Count, ErrorMessage)
        VALUES ('sp_Merge_ORIN', @StartTime, GETDATE(), 'Failed', 0, @ErrMsg);
        INSERT INTO dbo.ETL_ErrorLog (SourceTable, TargetTable, ErrorMessage)
        VALUES ('stg_ORIN', 'bronze.ORIN', @ErrMsg);
        THROW;
    END CATCH
END;
GO

/* -----------------------------------------------------------
   4) RIN1 (Credit Notes Lines)
   ----------------------------------------------------------- */
IF OBJECT_ID('bronze.stg_RIN1','U') IS NOT NULL DROP TABLE bronze.stg_RIN1;
CREATE TABLE bronze.stg_RIN1 (
    DocEntry INT,
    LineNum INT,
    ItemCode NVARCHAR(50),
    Dscription NVARCHAR(200),
    LineTotal DECIMAL(18,2),
    Quantity DECIMAL(18,6),
    StockPrice DECIMAL(18,6),
    WhsCode NVARCHAR(50)
);
GO

IF OBJECT_ID('dbo.sp_Merge_RIN1','P') IS NOT NULL DROP PROCEDURE dbo.sp_Merge_RIN1;
GO
CREATE PROCEDURE dbo.sp_Merge_RIN1
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @StartTime DATETIME = GETDATE(), @Row_Count INT = 0, @ErrMsg NVARCHAR(MAX);

    BEGIN TRY
        MERGE bronze.RIN1 tgt
        USING bronze.stg_RIN1 src
            ON tgt.DocEntry = src.DocEntry AND tgt.LineNum = src.LineNum
        WHEN MATCHED THEN
            UPDATE SET ItemCode = src.ItemCode, Dscription = src.Dscription, LineTotal = src.LineTotal,
                       Quantity = src.Quantity, StockPrice = src.StockPrice, WhsCode = src.WhsCode
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (DocEntry, LineNum, ItemCode, Dscription, LineTotal, Quantity, StockPrice, WhsCode)
            VALUES (src.DocEntry, src.LineNum, src.ItemCode, src.Dscription, src.LineTotal, src.Quantity, src.StockPrice, src.WhsCode);

        SET @Row_Count = @Row_Count;

        MERGE dbo.ETL_Checkpoints chk
        USING (SELECT 'RIN1' AS SourceTable, GETDATE() AS LastLoadDate) srcchk
            ON chk.SourceTable = srcchk.SourceTable
        WHEN MATCHED THEN UPDATE SET LastLoadDate = srcchk.LastLoadDate, UpdatedAt = GETDATE()
        WHEN NOT MATCHED THEN INSERT (SourceTable, LastLoadDate, UpdatedAt) VALUES (srcchk.SourceTable, srcchk.LastLoadDate, GETDATE());

        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, Row_Count)
        VALUES ('sp_Merge_RIN1', @StartTime, GETDATE(), 'Success', @Row_Count);
    END TRY
    BEGIN CATCH
        SET @ErrMsg = ERROR_MESSAGE();
        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, Row_Count, ErrorMessage)
        VALUES ('sp_Merge_RIN1', @StartTime, GETDATE(), 'Failed', 0, @ErrMsg);
        INSERT INTO dbo.ETL_ErrorLog (SourceTable, TargetTable, ErrorMessage)
        VALUES ('stg_RIN1', 'bronze.RIN1', @ErrMsg);
        THROW;
    END CATCH
END;
GO

/* -----------------------------------------------------------
   5) OCRD (Customers)
   ----------------------------------------------------------- */
IF OBJECT_ID('bronze.stg_OCRD','U') IS NOT NULL DROP TABLE bronze.stg_OCRD;
CREATE TABLE bronze.stg_OCRD (
    CardCode NVARCHAR(50),
    CardName NVARCHAR(200),
    Balance DECIMAL(18,2),
    CardType NVARCHAR(20),
    U_AGENTS NVARCHAR(50),
    U_SALES_CLASSIFICATION NVARCHAR(50)
);
GO

IF OBJECT_ID('dbo.sp_Merge_OCRD','P') IS NOT NULL DROP PROCEDURE dbo.sp_Merge_OCRD;
GO
CREATE PROCEDURE dbo.sp_Merge_OCRD
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @StartTime DATETIME = GETDATE(), @Row_Count INT = 0, @ErrMsg NVARCHAR(MAX);

    BEGIN TRY
        MERGE bronze.OCRD tgt
        USING bronze.stg_OCRD src
            ON tgt.CardCode = src.CardCode
        WHEN MATCHED THEN
            UPDATE SET CardName = src.CardName, Balance = src.Balance, CardType = src.CardType,
                       U_AGENTS = src.U_AGENTS, U_SALES_CLASSIFICATION = src.U_SALES_CLASSIFICATION
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (CardCode, CardName, Balance, CardType, U_AGENTS, U_SALES_CLASSIFICATION)
            VALUES (src.CardCode, src.CardName, src.Balance, src.CardType, src.U_AGENTS, src.U_SALES_CLASSIFICATION);

        SET @Row_Count = @Row_Count;

        MERGE dbo.ETL_Checkpoints chk
        USING (SELECT 'OCRD' AS SourceTable, GETDATE() AS LastLoadDate) srcchk
            ON chk.SourceTable = srcchk.SourceTable
        WHEN MATCHED THEN UPDATE SET LastLoadDate = srcchk.LastLoadDate, UpdatedAt = GETDATE()
        WHEN NOT MATCHED THEN INSERT (SourceTable, LastLoadDate, UpdatedAt) VALUES (srcchk.SourceTable, srcchk.LastLoadDate, GETDATE());

        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, Row_Count)
        VALUES ('sp_Merge_OCRD', @StartTime, GETDATE(), 'Success', @Row_Count);
    END TRY
    BEGIN CATCH
        SET @ErrMsg = ERROR_MESSAGE();
        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, Row_Count, ErrorMessage)
        VALUES ('sp_Merge_OCRD', @StartTime, GETDATE(), 'Failed', 0, @ErrMsg);
        INSERT INTO dbo.ETL_ErrorLog (SourceTable, TargetTable, ErrorMessage)
        VALUES ('stg_OCRD', 'bronze.OCRD', @ErrMsg);
        THROW;
    END CATCH
END;
GO

/* -----------------------------------------------------------
   6) OITM (Items)
   ----------------------------------------------------------- */
IF OBJECT_ID('bronze.stg_OITM','U') IS NOT NULL DROP TABLE bronze.stg_OITM;
CREATE TABLE bronze.stg_OITM (
    ItemCode NVARCHAR(50),
    ItemName NVARCHAR(200),
    ItmsGrpCod INT
);
GO

IF OBJECT_ID('dbo.sp_Merge_OITM','P') IS NOT NULL DROP PROCEDURE dbo.sp_Merge_OITM;
GO
CREATE PROCEDURE dbo.sp_Merge_OITM
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @StartTime DATETIME = GETDATE(), @Row_Count INT = 0, @ErrMsg NVARCHAR(MAX);

    BEGIN TRY
        MERGE bronze.OITM tgt
        USING bronze.stg_OITM src
            ON tgt.ItemCode = src.ItemCode
        WHEN MATCHED THEN
            UPDATE SET ItemName = src.ItemName, ItmsGrpCod = src.ItmsGrpCod
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (ItemCode, ItemName, ItmsGrpCod)
            VALUES (src.ItemCode, src.ItemName, src.ItmsGrpCod);

        SET @Row_Count = @Row_Count;

        MERGE dbo.ETL_Checkpoints chk
        USING (SELECT 'OITM' AS SourceTable, GETDATE() AS LastLoadDate) srcchk
            ON chk.SourceTable = srcchk.SourceTable
        WHEN MATCHED THEN UPDATE SET LastLoadDate = srcchk.LastLoadDate, UpdatedAt = GETDATE()
        WHEN NOT MATCHED THEN INSERT (SourceTable, LastLoadDate, UpdatedAt) VALUES (srcchk.SourceTable, srcchk.LastLoadDate, GETDATE());

        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, Row_Count)
        VALUES ('sp_Merge_OITM', @StartTime, GETDATE(), 'Success', @Row_Count);
    END TRY
    BEGIN CATCH
        SET @ErrMsg = ERROR_MESSAGE();
        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, Row_Count, ErrorMessage)
        VALUES ('sp_Merge_OITM', @StartTime, GETDATE(), 'Failed', 0, @ErrMsg);
        INSERT INTO dbo.ETL_ErrorLog (SourceTable, TargetTable, ErrorMessage)
        VALUES ('stg_OITM', 'bronze.OITM', @ErrMsg);
        THROW;
    END CATCH
END;
GO

/* -----------------------------------------------------------
   7) OITB (Item Groups)
   ----------------------------------------------------------- */
IF OBJECT_ID('bronze.stg_OITB','U') IS NOT NULL DROP TABLE bronze.stg_OITB;
CREATE TABLE bronze.stg_OITB (
    ItmsGrpCod INT,
    ItmsGrpNam NVARCHAR(100)
);
GO

IF OBJECT_ID('dbo.sp_Merge_OITB','P') IS NOT NULL DROP PROCEDURE dbo.sp_Merge_OITB;
GO
CREATE PROCEDURE dbo.sp_Merge_OITB
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @StartTime DATETIME = GETDATE(), @Row_Count INT = 0, @ErrMsg NVARCHAR(MAX);

    BEGIN TRY
        MERGE bronze.OITB tgt
        USING bronze.stg_OITB src
            ON tgt.ItmsGrpCod = src.ItmsGrpCod
        WHEN MATCHED THEN
            UPDATE SET ItmsGrpNam = src.ItmsGrpNam
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (ItmsGrpCod, ItmsGrpNam) VALUES (src.ItmsGrpCod, src.ItmsGrpNam);

        SET @Row_Count = @Row_Count;

        MERGE dbo.ETL_Checkpoints chk
        USING (SELECT 'OITB' AS SourceTable, GETDATE() AS LastLoadDate) srcchk
            ON chk.SourceTable = srcchk.SourceTable
        WHEN MATCHED THEN UPDATE SET LastLoadDate = srcchk.LastLoadDate, UpdatedAt = GETDATE()
        WHEN NOT MATCHED THEN INSERT (SourceTable, LastLoadDate, UpdatedAt) VALUES (srcchk.SourceTable, srcchk.LastLoadDate, GETDATE());

        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, Row_Count)
        VALUES ('sp_Merge_OITB', @StartTime, GETDATE(), 'Success', @Row_Count);
    END TRY
    BEGIN CATCH
        SET @ErrMsg = ERROR_MESSAGE();
        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, Row_Count, ErrorMessage)
        VALUES ('sp_Merge_OITB', @StartTime, GETDATE(), 'Failed', 0, @ErrMsg);
        INSERT INTO dbo.ETL_ErrorLog (SourceTable, TargetTable, ErrorMessage)
        VALUES ('stg_OITB', 'bronze.OITB', @ErrMsg);
        THROW;
    END CATCH
END;
GO

/* -----------------------------------------------------------
   8) NNM1 (Series)
   ----------------------------------------------------------- */
IF OBJECT_ID('bronze.stg_NNM1','U') IS NOT NULL DROP TABLE bronze.stg_NNM1;
CREATE TABLE bronze.stg_NNM1 (
    Series INT,
    SeriesName NVARCHAR(100)
);
GO

IF OBJECT_ID('dbo.sp_Merge_NNM1','P') IS NOT NULL DROP PROCEDURE dbo.sp_Merge_NNM1;
GO
CREATE PROCEDURE dbo.sp_Merge_NNM1
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @StartTime DATETIME = GETDATE(), @Row_Count INT = 0, @ErrMsg NVARCHAR(MAX);

    BEGIN TRY
        MERGE bronze.NNM1 tgt
        USING bronze.stg_NNM1 src
            ON tgt.Series = src.Series
        WHEN MATCHED THEN
            UPDATE SET SeriesName = src.SeriesName
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (Series, SeriesName) VALUES (src.Series, src.SeriesName);

        SET @Row_Count = @Row_Count;

        MERGE dbo.ETL_Checkpoints chk
        USING (SELECT 'NNM1' AS SourceTable, GETDATE() AS LastLoadDate) srcchk
            ON chk.SourceTable = srcchk.SourceTable
        WHEN MATCHED THEN UPDATE SET LastLoadDate = srcchk.LastLoadDate, UpdatedAt = GETDATE()
        WHEN NOT MATCHED THEN INSERT (SourceTable, LastLoadDate, UpdatedAt) VALUES (srcchk.SourceTable, srcchk.LastLoadDate, GETDATE());

        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, Row_Count)
        VALUES ('sp_Merge_NNM1', @StartTime, GETDATE(), 'Success', @Row_Count);
    END TRY
    BEGIN CATCH
        SET @ErrMsg = ERROR_MESSAGE();
        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, Row_Count, ErrorMessage)
        VALUES ('sp_Merge_NNM1', @StartTime, GETDATE(), 'Failed', 0, @ErrMsg);
        INSERT INTO dbo.ETL_ErrorLog (SourceTable, TargetTable, ErrorMessage)
        VALUES ('stg_NNM1', 'bronze.NNM1', @ErrMsg);
        THROW;
    END CATCH
END;
GO

/* -----------------------------------------------------------
   9) OWTR (Transfers header)
   ----------------------------------------------------------- */
IF OBJECT_ID('bronze.stg_OWTR','U') IS NOT NULL DROP TABLE bronze.stg_OWTR;
CREATE TABLE bronze.stg_OWTR (
    DocEntry INT,
    DocNum NVARCHAR(50),
    DocDate DATE,
    Filler NVARCHAR(100),
    UpdateDate DATETIME
);
GO

IF OBJECT_ID('dbo.sp_Merge_OWTR','P') IS NOT NULL DROP PROCEDURE dbo.sp_Merge_OWTR;
GO
CREATE PROCEDURE dbo.sp_Merge_OWTR
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @StartTime DATETIME = GETDATE(), @Row_Count INT = 0, @ErrMsg NVARCHAR(MAX);

    BEGIN TRY
        MERGE bronze.OWTR tgt
        USING bronze.stg_OWTR src
            ON tgt.DocEntry = src.DocEntry
        WHEN MATCHED AND ISNULL(tgt.UpdateDate,'19000101') < ISNULL(src.UpdateDate,'19000101') THEN
            UPDATE SET DocNum = src.DocNum, DocDate = src.DocDate, Filler = src.Filler, UpdateDate = src.UpdateDate
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (DocEntry, DocNum, DocDate, Filler, UpdateDate)
            VALUES (src.DocEntry, src.DocNum, src.DocDate, src.Filler, src.UpdateDate);

        SET @Row_Count = @Row_Count;

        MERGE dbo.ETL_Checkpoints chk
        USING (SELECT 'OWTR' AS SourceTable, MAX(UpdateDate) AS LastLoadDate FROM bronze.OWTR) srcchk
            ON chk.SourceTable = srcchk.SourceTable
        WHEN MATCHED THEN UPDATE SET LastLoadDate = srcchk.LastLoadDate, UpdatedAt = GETDATE()
        WHEN NOT MATCHED THEN INSERT (SourceTable, LastLoadDate, UpdatedAt) VALUES (srcchk.SourceTable, srcchk.LastLoadDate, GETDATE());

        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, Row_Count)
        VALUES ('sp_Merge_OWTR', @StartTime, GETDATE(), 'Success', @Row_Count);
    END TRY
    BEGIN CATCH
        SET @ErrMsg = ERROR_MESSAGE();
        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, Row_Count, ErrorMessage)
        VALUES ('sp_Merge_OWTR', @StartTime, GETDATE(), 'Failed', 0, @ErrMsg);
        INSERT INTO dbo.ETL_ErrorLog (SourceTable, TargetTable, ErrorMessage)
        VALUES ('stg_OWTR', 'bronze.OWTR', @ErrMsg);
        THROW;
    END CATCH
END;
GO

/* -----------------------------------------------------------
 10) WTR1 (Transfer lines)
   ----------------------------------------------------------- */
IF OBJECT_ID('bronze.stg_WTR1','U') IS NOT NULL DROP TABLE bronze.stg_WTR1;
CREATE TABLE bronze.stg_WTR1 (
    DocEntry INT,
    LineNum INT,
    ItemCode NVARCHAR(50),
    Dscription NVARCHAR(200),
    FromWhsCod NVARCHAR(50),
    WhsCode NVARCHAR(50),
    Quantity DECIMAL(18,6),
    StockPrice DECIMAL(18,6)
);
GO

IF OBJECT_ID('dbo.sp_Merge_WTR1','P') IS NOT NULL DROP PROCEDURE dbo.sp_Merge_WTR1;
GO
CREATE PROCEDURE dbo.sp_Merge_WTR1
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @StartTime DATETIME = GETDATE(), @Row_Count INT = 0, @ErrMsg NVARCHAR(MAX);

    BEGIN TRY
        MERGE bronze.WTR1 tgt
        USING bronze.stg_WTR1 src
            ON tgt.DocEntry = src.DocEntry AND tgt.LineNum = src.LineNum
        WHEN MATCHED THEN
            UPDATE SET ItemCode = src.ItemCode, Dscription = src.Dscription, FromWhsCod = src.FromWhsCod,
                       WhsCode = src.WhsCode, Quantity = src.Quantity, StockPrice = src.StockPrice
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (DocEntry, LineNum, ItemCode, Dscription, FromWhsCod, WhsCode, Quantity, StockPrice)
            VALUES (src.DocEntry, src.LineNum, src.ItemCode, src.Dscription, src.FromWhsCod, src.WhsCode, src.Quantity, src.StockPrice);

        SET @Row_Count = @Row_Count;

        MERGE dbo.ETL_Checkpoints chk
        USING (SELECT 'WTR1' AS SourceTable, GETDATE() AS LastLoadDate) srcchk
            ON chk.SourceTable = srcchk.SourceTable
        WHEN MATCHED THEN UPDATE SET LastLoadDate = srcchk.LastLoadDate, UpdatedAt = GETDATE()
        WHEN NOT MATCHED THEN INSERT (SourceTable, LastLoadDate, UpdatedAt) VALUES (srcchk.SourceTable, srcchk.LastLoadDate, GETDATE());

        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, Row_Count)
        VALUES ('sp_Merge_WTR1', @StartTime, GETDATE(), 'Success', @Row_Count);
    END TRY
    BEGIN CATCH
        SET @ErrMsg = ERROR_MESSAGE();
        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, Row_Count, ErrorMessage)
        VALUES ('sp_Merge_WTR1', @StartTime, GETDATE(), 'Failed', 0, @ErrMsg);
        INSERT INTO dbo.ETL_ErrorLog (SourceTable, TargetTable, ErrorMessage)
        VALUES ('stg_WTR1', 'bronze.WTR1', @ErrMsg);
        THROW;
    END CATCH
END;
GO

/* -----------------------------------------------------------
 11) OPCH (AP Invoice header)
   ----------------------------------------------------------- */
IF OBJECT_ID('bronze.stg_OPCH','U') IS NOT NULL DROP TABLE bronze.stg_OPCH;
CREATE TABLE bronze.stg_OPCH (
    DocEntry INT,
    DocNum NVARCHAR(50),
    DocDate DATE,
    CardCode NVARCHAR(50),
    CardName NVARCHAR(200),
    DocTotal DECIMAL(18,2),
    CANCELED CHAR(1),
    UpdateDate DATETIME
);
GO

IF OBJECT_ID('dbo.sp_Merge_OPCH','P') IS NOT NULL DROP PROCEDURE dbo.sp_Merge_OPCH;
GO
CREATE PROCEDURE dbo.sp_Merge_OPCH
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @StartTime DATETIME = GETDATE(), @Row_Count INT = 0, @ErrMsg NVARCHAR(MAX);

    BEGIN TRY
        MERGE bronze.OPCH tgt
        USING bronze.stg_OPCH src
            ON tgt.DocEntry = src.DocEntry
        WHEN MATCHED AND ISNULL(tgt.UpdateDate,'19000101') < ISNULL(src.UpdateDate,'19000101') THEN
            UPDATE SET DocNum = src.DocNum, DocDate = src.DocDate, CardCode = src.CardCode, CardName = src.CardName,
                       DocTotal = src.DocTotal, CANCELED = src.CANCELED, UpdateDate = src.UpdateDate
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (DocEntry, DocNum, DocDate, CardCode, CardName, DocTotal, CANCELED, UpdateDate)
            VALUES (src.DocEntry, src.DocNum, src.DocDate, src.CardCode, src.CardName, src.DocTotal, src.CANCELED, src.UpdateDate);

        SET @Row_Count = @Row_Count;

        MERGE dbo.ETL_Checkpoints chk
        USING (SELECT 'OPCH' AS SourceTable, MAX(UpdateDate) AS LastLoadDate FROM bronze.OPCH) srcchk
            ON chk.SourceTable = srcchk.SourceTable
        WHEN MATCHED THEN UPDATE SET LastLoadDate = srcchk.LastLoadDate, UpdatedAt = GETDATE()
        WHEN NOT MATCHED THEN INSERT (SourceTable, LastLoadDate, UpdatedAt) VALUES (srcchk.SourceTable, srcchk.LastLoadDate, GETDATE());

        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, Row_Count)
        VALUES ('sp_Merge_OPCH', @StartTime, GETDATE(), 'Success', @Row_Count);
    END TRY
    BEGIN CATCH
        SET @ErrMsg = ERROR_MESSAGE();
        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, Row_Count, ErrorMessage)
        VALUES ('sp_Merge_OPCH', @StartTime, GETDATE(), 'Failed', 0, @ErrMsg);
        INSERT INTO dbo.ETL_ErrorLog (SourceTable, TargetTable, ErrorMessage)
        VALUES ('stg_OPCH', 'bronze.OPCH', @ErrMsg);
        THROW;
    END CATCH
END;
GO

/* -----------------------------------------------------------
 12) PCH1 (AP Invoice lines)
   ----------------------------------------------------------- */
IF OBJECT_ID('bronze.stg_PCH1','U') IS NOT NULL DROP TABLE bronze.stg_PCH1;
CREATE TABLE bronze.stg_PCH1 (
    DocEntry INT,
    LineNum INT,
    ItemCode NVARCHAR(50),
    Dscription NVARCHAR(200),
    LineTotal DECIMAL(18,2),
    Quantity DECIMAL(18,6),
    StockPrice DECIMAL(18,6),
    WhsCode NVARCHAR(50)
);
GO

IF OBJECT_ID('dbo.sp_Merge_PCH1','P') IS NOT NULL DROP PROCEDURE dbo.sp_Merge_PCH1;
GO
CREATE PROCEDURE dbo.sp_Merge_PCH1
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @StartTime DATETIME = GETDATE(), @Row_Count INT = 0, @ErrMsg NVARCHAR(MAX);

    BEGIN TRY
        MERGE bronze.PCH1 tgt
        USING bronze.stg_PCH1 src
            ON tgt.DocEntry = src.DocEntry AND tgt.LineNum = src.LineNum
        WHEN MATCHED THEN
            UPDATE SET ItemCode = src.ItemCode, Dscription = src.Dscription, LineTotal = src.LineTotal,
                       Quantity = src.Quantity, StockPrice = src.StockPrice, WhsCode = src.WhsCode
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (DocEntry, LineNum, ItemCode, Dscription, LineTotal, Quantity, StockPrice, WhsCode)
            VALUES (src.DocEntry, src.LineNum, src.ItemCode, src.Dscription, src.LineTotal, src.Quantity, src.StockPrice, src.WhsCode);

        SET @Row_Count = @Row_Count;

        MERGE dbo.ETL_Checkpoints chk
        USING (SELECT 'PCH1' AS SourceTable, GETDATE() AS LastLoadDate) srcchk
            ON chk.SourceTable = srcchk.SourceTable
        WHEN MATCHED THEN UPDATE SET LastLoadDate = srcchk.LastLoadDate, UpdatedAt = GETDATE()
        WHEN NOT MATCHED THEN INSERT (SourceTable, LastLoadDate, UpdatedAt) VALUES (srcchk.SourceTable, srcchk.LastLoadDate, GETDATE());

        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, Row_Count)
        VALUES ('sp_Merge_PCH1', @StartTime, GETDATE(), 'Success', @Row_Count);
    END TRY
    BEGIN CATCH
        SET @ErrMsg = ERROR_MESSAGE();
        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, Row_Count, ErrorMessage)
        VALUES ('sp_Merge_PCH1', @StartTime, GETDATE(), 'Failed', 0, @ErrMsg);
        INSERT INTO dbo.ETL_ErrorLog (SourceTable, TargetTable, ErrorMessage)
        VALUES ('stg_PCH1', 'bronze.PCH1', @ErrMsg);
        THROW;
    END CATCH
END;
GO

/* -----------------------------------------------------------
 13) SAT (user table @SAT)
   ----------------------------------------------------------- */
IF OBJECT_ID('bronze.stg_SAT','U') IS NOT NULL DROP TABLE bronze.stg_SAT;
CREATE TABLE bronze.stg_SAT (
    Code NVARCHAR(50),
    Name NVARCHAR(200)
);
GO

IF OBJECT_ID('dbo.sp_Merge_SAT','P') IS NOT NULL DROP PROCEDURE dbo.sp_Merge_SAT;
GO
CREATE PROCEDURE dbo.sp_Merge_SAT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @StartTime DATETIME = GETDATE(), @Row_Count INT = 0, @ErrMsg NVARCHAR(MAX);

    BEGIN TRY
        MERGE bronze.SAT tgt
        USING bronze.stg_SAT src
            ON tgt.Code = src.Code
        WHEN MATCHED THEN
            UPDATE SET Name = src.Name
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (Code, Name) VALUES (src.Code, src.Name);

        SET @Row_Count = @Row_Count;

        MERGE dbo.ETL_Checkpoints chk
        USING (SELECT 'SAT' AS SourceTable, GETDATE() AS LastLoadDate) srcchk
            ON chk.SourceTable = srcchk.SourceTable
        WHEN MATCHED THEN UPDATE SET LastLoadDate = srcchk.LastLoadDate, UpdatedAt = GETDATE()
        WHEN NOT MATCHED THEN INSERT (SourceTable, LastLoadDate, UpdatedAt) VALUES (srcchk.SourceTable, srcchk.LastLoadDate, GETDATE());

        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, Row_Count)
        VALUES ('sp_Merge_SAT', @StartTime, GETDATE(), 'Success', @Row_Count);
    END TRY
    BEGIN CATCH
        SET @ErrMsg = ERROR_MESSAGE();
        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, Row_Count, ErrorMessage)
        VALUES ('sp_Merge_SAT', @StartTime, GETDATE(), 'Failed', 0, @ErrMsg);
        INSERT INTO dbo.ETL_ErrorLog (SourceTable, TargetTable, ErrorMessage)
        VALUES ('stg_SAT', 'bronze.SAT', @ErrMsg);
        THROW;
    END CATCH
END;
GO

/* -----------------------------------------------------------
 14) SCT (user table @SCT)
   ----------------------------------------------------------- */
IF OBJECT_ID('bronze.stg_SCT','U') IS NOT NULL DROP TABLE bronze.stg_SCT;
CREATE TABLE bronze.stg_SCT (
    Code NVARCHAR(50),
    Name NVARCHAR(200)
);
GO

IF OBJECT_ID('dbo.sp_Merge_SCT','P') IS NOT NULL DROP PROCEDURE dbo.sp_Merge_SCT;
GO
CREATE PROCEDURE dbo.sp_Merge_SCT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @StartTime DATETIME = GETDATE(), @Row_Count INT = 0, @ErrMsg NVARCHAR(MAX);

    BEGIN TRY
        MERGE bronze.SCT tgt
        USING bronze.stg_SCT src
            ON tgt.Code = src.Code
        WHEN MATCHED THEN
            UPDATE SET Name = src.Name
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (Code, Name) VALUES (src.Code, src.Name);

        SET @Row_Count = @Row_Count;

        MERGE dbo.ETL_Checkpoints chk
        USING (SELECT 'SCT' AS SourceTable, GETDATE() AS LastLoadDate) srcchk
            ON chk.SourceTable = srcchk.SourceTable
        WHEN MATCHED THEN UPDATE SET LastLoadDate = srcchk.LastLoadDate, UpdatedAt = GETDATE()
        WHEN NOT MATCHED THEN INSERT (SourceTable, LastLoadDate, UpdatedAt) VALUES (srcchk.SourceTable, srcchk.LastLoadDate, GETDATE());

        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, Row_Count)
        VALUES ('sp_Merge_SCT', @StartTime, GETDATE(), 'Success', @Row_Count);
    END TRY
    BEGIN CATCH
        SET @ErrMsg = ERROR_MESSAGE();
        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, Row_Count, ErrorMessage)
        VALUES ('sp_Merge_SCT', @StartTime, GETDATE(), 'Failed', 0, @ErrMsg);
        INSERT INTO dbo.ETL_ErrorLog (SourceTable, TargetTable, ErrorMessage)
        VALUES ('stg_SCT', 'bronze.SCT', @ErrMsg);
        THROW;
    END CATCH
END;
GO

/* -----------------------------------------------------------
 15) ORDN (Sales Returns header) - optional/future-proof
   ----------------------------------------------------------- */
IF OBJECT_ID('bronze.stg_ORDN','U') IS NOT NULL DROP TABLE bronze.stg_ORDN;
CREATE TABLE bronze.stg_ORDN (
    DocEntry INT,
    DocNum NVARCHAR(50),
    DocDate DATE,
    CardCode NVARCHAR(50),
    CardName NVARCHAR(200),
    DocTotal DECIMAL(18,2),
    CANCELED CHAR(1),
    UpdateDate DATETIME
);
GO

IF OBJECT_ID('dbo.sp_Merge_ORDN','P') IS NOT NULL DROP PROCEDURE dbo.sp_Merge_ORDN;
GO
CREATE PROCEDURE dbo.sp_Merge_ORDN
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @StartTime DATETIME = GETDATE(), @Row_Count INT = 0, @ErrMsg NVARCHAR(MAX);

    BEGIN TRY
        MERGE bronze.ORDN tgt
        USING bronze.stg_ORDN src
            ON tgt.DocEntry = src.DocEntry
        WHEN MATCHED AND ISNULL(tgt.UpdateDate,'19000101') < ISNULL(src.UpdateDate,'19000101') THEN
            UPDATE SET DocNum = src.DocNum, DocDate = src.DocDate, CardCode = src.CardCode, CardName = src.CardName,
                       DocTotal = src.DocTotal, CANCELED = src.CANCELED, UpdateDate = src.UpdateDate
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (DocEntry, DocNum, DocDate, CardCode, CardName, DocTotal, CANCELED, UpdateDate)
            VALUES (src.DocEntry, src.DocNum, src.DocDate, src.CardCode, src.CardName, src.DocTotal, src.CANCELED, src.UpdateDate);

        SET @Row_Count = @Row_Count;

        MERGE dbo.ETL_Checkpoints chk
        USING (SELECT 'ORDN' AS SourceTable, MAX(UpdateDate) AS LastLoadDate FROM bronze.ORDN) srcchk
            ON chk.SourceTable = srcchk.SourceTable
        WHEN MATCHED THEN UPDATE SET LastLoadDate = srcchk.LastLoadDate, UpdatedAt = GETDATE()
        WHEN NOT MATCHED THEN INSERT (SourceTable, LastLoadDate, UpdatedAt) VALUES (srcchk.SourceTable, srcchk.LastLoadDate, GETDATE());

        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, Row_Count)
        VALUES ('sp_Merge_ORDN', @StartTime, GETDATE(), 'Success', @Row_Count);
    END TRY
    BEGIN CATCH
        SET @ErrMsg = ERROR_MESSAGE();
        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, Row_Count, ErrorMessage)
        VALUES ('sp_Merge_ORDN', @StartTime, GETDATE(), 'Failed', 0, @ErrMsg);
        INSERT INTO dbo.ETL_ErrorLog (SourceTable, TargetTable, ErrorMessage)
        VALUES ('stg_ORDN', 'bronze.ORDN', @ErrMsg);
        THROW;
    END CATCH
END;
GO

/* -----------------------------------------------------------
 16) -- AP Credit Notes (Header)
   ----------------------------------------------------------- */

IF OBJECT_ID('bronze.ORPC','U') IS NOT NULL DROP TABLE bronze.ORPC;
CREATE TABLE bronze.ORPC (
    DocEntry INT PRIMARY KEY,
    DocNum NVARCHAR(50),
    DocDate DATE,
    CardCode NVARCHAR(50),
    CardName NVARCHAR(200),
    DocTotal DECIMAL(18,2),
    CANCELED CHAR(1),
    UpdateDate DATETIME
);
GO

/* ================================
   MERGE PROCS
================================= */

-- ORPC
IF OBJECT_ID('dbo.sp_Merge_ORPC','P') IS NOT NULL DROP PROCEDURE dbo.sp_Merge_ORPC;
GO
CREATE PROCEDURE dbo.sp_Merge_ORPC
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @StartTime DATETIME = GETDATE(), @Row_Count INT = 0, @Err NVARCHAR(MAX);

    BEGIN TRY
        MERGE bronze.ORPC tgt
        USING bronze.stg_ORPC src
            ON tgt.DocEntry = src.DocEntry
        WHEN MATCHED AND tgt.UpdateDate < src.UpdateDate THEN
            UPDATE SET DocNum=src.DocNum, DocDate=src.DocDate, CardCode=src.CardCode, CardName=src.CardName,
                       DocTotal=src.DocTotal, CANCELED=src.CANCELED, UpdateDate=src.UpdateDate
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (DocEntry, DocNum, DocDate, CardCode, CardName, DocTotal, CANCELED, UpdateDate)
            VALUES (src.DocEntry, src.DocNum, src.DocDate, src.CardCode, src.CardName, src.DocTotal, src.CANCELED, src.UpdateDate);

        SET @Row_Count = @Row_Count;

        MERGE dbo.ETL_Checkpoints chk
        USING (SELECT 'ORPC' AS SourceTable, MAX(UpdateDate) AS LastLoadDate FROM bronze.ORPC) s
            ON chk.SourceTable = s.SourceTable
        WHEN MATCHED THEN UPDATE SET LastLoadDate=s.LastLoadDate, UpdatedAt=GETDATE()
        WHEN NOT MATCHED THEN INSERT (SourceTable, LastLoadDate, UpdatedAt) VALUES (s.SourceTable, s.LastLoadDate, GETDATE());

        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, Row_Count)
        VALUES ('sp_Merge_ORPC', @StartTime, GETDATE(), 'Success', @Row_Count);
    END TRY
    BEGIN CATCH
        SET @Err = ERROR_MESSAGE();
        INSERT INTO dbo.ETL_ErrorLog (SourceTable, TargetTable, ErrorMessage)
        VALUES ('stg_ORPC', 'bronze.ORPC', @Err);
        THROW;
    END CATCH
END;
GO


/* -----------------------------------------------------------
 17) -- AP Credit Notes (Lines)
   ----------------------------------------------------------- */
IF OBJECT_ID('bronze.stg_RPC1','U') IS NOT NULL DROP TABLE bronze.stg_RPC1;
CREATE TABLE bronze.stg_RPC1 (
    DocEntry INT,
    LineNum INT,
    ItemCode NVARCHAR(50),
    Dscription NVARCHAR(200),
    LineTotal DECIMAL(18,2),
    Quantity DECIMAL(18,6),
    StockPrice DECIMAL(18,6),
    WhsCode NVARCHAR(50)
);
GO

/* ================================
   MERGE PROCS
================================= */
-- RPC1
IF OBJECT_ID('dbo.sp_Merge_RPC1','P') IS NOT NULL DROP PROCEDURE dbo.sp_Merge_RPC1;
GO
CREATE PROCEDURE dbo.sp_Merge_RPC1
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @StartTime DATETIME = GETDATE(), @Row_Count INT = 0, @Err NVARCHAR(MAX);

    BEGIN TRY
        MERGE bronze.RPC1 tgt
        USING bronze.stg_RPC1 src
            ON tgt.DocEntry = src.DocEntry AND tgt.LineNum = src.LineNum
        WHEN MATCHED THEN
            UPDATE SET ItemCode=src.ItemCode, Dscription=src.Dscription, LineTotal=src.LineTotal,
                       Quantity=src.Quantity, StockPrice=src.StockPrice, WhsCode=src.WhsCode
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (DocEntry, LineNum, ItemCode, Dscription, LineTotal, Quantity, StockPrice, WhsCode)
            VALUES (src.DocEntry, src.LineNum, src.ItemCode, src.Dscription, src.LineTotal, src.Quantity, src.StockPrice, src.WhsCode);

        SET @Row_Count = @Row_Count;

        MERGE dbo.ETL_Checkpoints chk
        USING (SELECT 'RPC1' AS SourceTable, GETDATE() AS LastLoadDate) s
            ON chk.SourceTable = s.SourceTable
        WHEN MATCHED THEN UPDATE SET LastLoadDate=s.LastLoadDate, UpdatedAt=GETDATE()
        WHEN NOT MATCHED THEN INSERT (SourceTable, LastLoadDate, UpdatedAt) VALUES (s.SourceTable, s.LastLoadDate, GETDATE());

        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, Row_Count)
        VALUES ('sp_Merge_RPC1', @StartTime, GETDATE(), 'Success', @Row_Count);
    END TRY
    BEGIN CATCH
        SET @Err = ERROR_MESSAGE();
        INSERT INTO dbo.ETL_ErrorLog (SourceTable, TargetTable, ErrorMessage)
        VALUES ('stg_RPC1', 'bronze.RPC1', @Err);
        THROW;
    END CATCH
END;
GO


/* -----------------------------------------------------------
 18) -- Goods Receipt PO (Header)
   ----------------------------------------------------------- */
-- Goods Receipt PO (Header)
IF OBJECT_ID('bronze.stg_OPDN','U') IS NOT NULL DROP TABLE bronze.stg_OPDN;
CREATE TABLE bronze.stg_OPDN (
    DocEntry INT,
    DocNum NVARCHAR(50),
    DocDate DATE,
    CardCode NVARCHAR(50),
    CardName NVARCHAR(200),
    DocTotal DECIMAL(18,2),
    CANCELED CHAR(1),
    UpdateDate DATETIME
);
GO

/* ================================
   MERGE PROCS
================================= */
-- OPDN
IF OBJECT_ID('dbo.sp_Merge_OPDN','P') IS NOT NULL DROP PROCEDURE dbo.sp_Merge_OPDN;
GO
CREATE PROCEDURE dbo.sp_Merge_OPDN
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @StartTime DATETIME = GETDATE(), @Row_Count INT = 0, @Err NVARCHAR(MAX);

    BEGIN TRY
        MERGE bronze.OPDN tgt
        USING bronze.stg_OPDN src
            ON tgt.DocEntry = src.DocEntry
        WHEN MATCHED AND tgt.UpdateDate < src.UpdateDate THEN
            UPDATE SET DocNum=src.DocNum, DocDate=src.DocDate, CardCode=src.CardCode, CardName=src.CardName,
                       DocTotal=src.DocTotal, CANCELED=src.CANCELED, UpdateDate=src.UpdateDate
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (DocEntry, DocNum, DocDate, CardCode, CardName, DocTotal, CANCELED, UpdateDate)
            VALUES (src.DocEntry, src.DocNum, src.DocDate, src.CardCode, src.CardName, src.DocTotal, src.CANCELED, src.UpdateDate);

        SET @Row_Count = @Row_Count;

        MERGE dbo.ETL_Checkpoints chk
        USING (SELECT 'OPDN' AS SourceTable, MAX(UpdateDate) AS LastLoadDate FROM bronze.OPDN) s
            ON chk.SourceTable = s.SourceTable
        WHEN MATCHED THEN UPDATE SET LastLoadDate=s.LastLoadDate, UpdatedAt=GETDATE()
        WHEN NOT MATCHED THEN INSERT (SourceTable, LastLoadDate, UpdatedAt) VALUES (s.SourceTable, s.LastLoadDate, GETDATE());

        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, Row_Count)
        VALUES ('sp_Merge_OPDN', @StartTime, GETDATE(), 'Success', @Row_Count);
    END TRY
    BEGIN CATCH
        SET @Err = ERROR_MESSAGE();
        INSERT INTO dbo.ETL_ErrorLog (SourceTable, TargetTable, ErrorMessage)
        VALUES ('stg_OPDN', 'bronze.OPDN', @Err);
        THROW;
    END CATCH
END;
GO

/* -----------------------------------------------------------
 19) -- Goods Receipt PO (Lines)
   ----------------------------------------------------------- */
-- Goods Receipt PO (Lines)
IF OBJECT_ID('bronze.stg_PDN1','U') IS NOT NULL DROP TABLE bronze.stg_PDN1;
CREATE TABLE bronze.stg_PDN1 (
    DocEntry INT,
    LineNum INT,
    ItemCode NVARCHAR(50),
    Dscription NVARCHAR(200),
    LineTotal DECIMAL(18,2),
    Quantity DECIMAL(18,6),
    StockPrice DECIMAL(18,6),
    WhsCode NVARCHAR(50)
);
GO

/* ================================
   MERGE PROCS
================================= */
-- PDN1
IF OBJECT_ID('dbo.sp_Merge_PDN1','P') IS NOT NULL DROP PROCEDURE dbo.sp_Merge_PDN1;
GO
CREATE PROCEDURE dbo.sp_Merge_PDN1
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @StartTime DATETIME = GETDATE(), @Row_Count INT = 0, @Err NVARCHAR(MAX);

    BEGIN TRY
        MERGE bronze.PDN1 tgt
        USING bronze.stg_PDN1 src
            ON tgt.DocEntry = src.DocEntry AND tgt.LineNum = src.LineNum
        WHEN MATCHED THEN
            UPDATE SET ItemCode=src.ItemCode, Dscription=src.Dscription, LineTotal=src.LineTotal,
                       Quantity=src.Quantity, StockPrice=src.StockPrice, WhsCode=src.WhsCode
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (DocEntry, LineNum, ItemCode, Dscription, LineTotal, Quantity, StockPrice, WhsCode)
            VALUES (src.DocEntry, src.LineNum, src.ItemCode, src.Dscription, src.LineTotal, src.Quantity, src.StockPrice, src.WhsCode);

        SET @Row_Count = @Row_Count;

        MERGE dbo.ETL_Checkpoints chk
        USING (SELECT 'PDN1' AS SourceTable, GETDATE() AS LastLoadDate) s
            ON chk.SourceTable = s.SourceTable
        WHEN MATCHED THEN UPDATE SET LastLoadDate=s.LastLoadDate, UpdatedAt=GETDATE()
        WHEN NOT MATCHED THEN INSERT (SourceTable, LastLoadDate, UpdatedAt) VALUES (s.SourceTable, s.LastLoadDate, GETDATE());

        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, Row_Count)
        VALUES ('sp_Merge_PDN1', @StartTime, GETDATE(), 'Success', @Row_Count);
    END TRY
    BEGIN CATCH
        SET @Err = ERROR_MESSAGE();
        INSERT INTO dbo.ETL_ErrorLog (SourceTable, TargetTable, ErrorMessage)
        VALUES ('stg_PDN1', 'bronze.PDN1', @Err);
        THROW;
    END CATCH
END;
GO



-- End of master script
