/*Title: Add Silver InventoryTransferView procedure

Comment: Converted InventoryTransferView into Silver table + proc*/

USE DataWarehouse;
GO

/* --------------------------------------------
   1) Silver table (structure aligned with InventoryTransferView)
   -------------------------------------------- */
IF OBJECT_ID('silver.InventoryTransferView','U') IS NOT NULL
    DROP TABLE silver.InventoryTransferView;
GO

CREATE TABLE silver.InventoryTransferView (
    DocumentNumber     NVARCHAR(50),
    ItemGroupName      NVARCHAR(50),
    ItemCode           NVARCHAR(50),
    ItemDescription    NVARCHAR(200),
    ToWarehouse        NVARCHAR(50),
    FromWarehouse      NVARCHAR(50),
    PostingDate        DATE,
    Quantity           DECIMAL(18,6),
    ItemCost           DECIMAL(18,6),
    LoadDate           DATETIME DEFAULT GETDATE()
);
GO

/* ------------------------------------------------
   2) Stored proc to load Silver.InventoryTransferView
   ------------------------------------------------ */
IF OBJECT_ID('dbo.sp_Load_Silver_InventoryTransferView','P') IS NOT NULL
    DROP PROCEDURE dbo.sp_Load_Silver_InventoryTransferView;
GO

CREATE PROCEDURE dbo.sp_Load_Silver_InventoryTransferView
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @Err NVARCHAR(MAX);

    BEGIN TRY
        TRUNCATE TABLE silver.InventoryTransferView;

        INSERT INTO silver.InventoryTransferView (
            DocumentNumber, ItemGroupName, ItemCode, ItemDescription,
            ToWarehouse, FromWarehouse, PostingDate, Quantity, ItemCost
        )
        SELECT DISTINCT
            h.DocNum AS DocumentNumber,
            CASE 
                WHEN g.ItmsGrpNam = 'IA' THEN 'IA'
                WHEN g.ItmsGrpNam = 'OMNI' THEN 'OMNI'
                ELSE 'Others'
            END AS ItemGroupName,
            l.ItemCode,
            l.Dscription AS ItemDescription,
            l.WhsCode AS ToWarehouse,
            l.FromWhsCod AS FromWarehouse,
            h.DocDate AS PostingDate,
            l.Quantity,
            l.StockPrice AS ItemCost
        FROM bronze.OWTR h
        INNER JOIN bronze.WTR1 l ON h.DocEntry = l.DocEntry
        LEFT JOIN bronze.OITM i ON l.ItemCode = i.ItemCode
        LEFT JOIN bronze.OITB g ON i.ItmsGrpCod = g.ItmsGrpCod;

        /* Log success */
        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, Row_Count)
        VALUES ('sp_Load_Silver_InventoryTransferView', @StartTime, GETDATE(), 'Success', (SELECT COUNT(*) FROM silver.InventoryTransferView));
    END TRY
    BEGIN CATCH
        SET @Err = ERROR_MESSAGE();
        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, Row_Count, ErrorMessage)
        VALUES ('sp_Load_Silver_InventoryTransferView', @StartTime, GETDATE(), 'Failed', 0, @Err);
        THROW;
    END CATCH
END;
GO
