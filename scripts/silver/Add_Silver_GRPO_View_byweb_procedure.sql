/*Title: Add Silver GRPO_View_byweb procedure

Comment: Converted GRPO_VIEW_byweb into Silver table + union-all proc*/

USE DataWarehouse;
GO

/* --------------------------------------------
   1) Silver table (structure aligned with GRPO_VIEW_byweb)
   -------------------------------------------- */
IF OBJECT_ID('silver.GRPO_View_byweb','U') IS NOT NULL
    DROP TABLE silver.GRPO_View_byweb;
GO

CREATE TABLE silver.GRPO_View_byweb (
    DocumentNumber         NVARCHAR(50),
    ItemGroupName          NVARCHAR(50),
    ItemCode               NVARCHAR(50),
    ItemDescription        NVARCHAR(200),
    WarehouseCode          NVARCHAR(50),
    PostingDate            DATE,
    Quantity               DECIMAL(18,6),
    Discount               DECIMAL(18,6),
    ItemCost               DECIMAL(18,6),
    TotalLC                DECIMAL(18,2),
    Series                 INT,
    SeriesName             NVARCHAR(200),
    TotalBeforeDiscount    DECIMAL(18,2),
    TotalNet               DECIMAL(18,2),
    LoadDate               DATETIME DEFAULT GETDATE()
);
GO

/* ------------------------------------------------
   2) Stored proc with UNION ALL (clone of GRPO_VIEW_byweb)
   ------------------------------------------------ */
IF OBJECT_ID('dbo.sp_Load_Silver_GRPO_View_byweb','P') IS NOT NULL
    DROP PROCEDURE dbo.sp_Load_Silver_GRPO_View_byweb;
GO

CREATE PROCEDURE dbo.sp_Load_Silver_GRPO_View_byweb
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @Err NVARCHAR(MAX);

    BEGIN TRY
        TRUNCATE TABLE silver.GRPO_View_byweb;

        INSERT INTO silver.GRPO_View_byweb (
            DocumentNumber, ItemGroupName, ItemCode, ItemDescription, WarehouseCode,
            PostingDate, Quantity, Discount, ItemCost, TotalLC, Series, SeriesName,
            TotalBeforeDiscount, TotalNet
        )
        /* ---------------------------
           AP Invoice
           --------------------------- */
        SELECT 
            h.DocNum,
            CASE 
                WHEN g.ItmsGrpNam = 'IA' THEN 'IA'
                WHEN g.ItmsGrpNam = 'OMNI' THEN 'OMNI'
                ELSE 'Others'
            END AS ItemGroupName,
            l.ItemCode,
            l.Dscription AS ItemDescription,
            l.WhsCode AS WarehouseCode,
            h.DocDate AS PostingDate,
            l.Quantity,
            l.DiscPrcnt,
            l.PriceBefDi AS ItemCost,
            l.LineTotal,
            h.Series,
            n.SeriesName,
            ROUND(l.PriceBefDi * l.Quantity, 2) AS TotalBeforeDiscount,
            SUM(ROUND(ROUND(l.PriceBefDi * (1 - (l.DiscPrcnt / 100)), 2) * l.Quantity, 2)) AS TotalNet
        FROM bronze.OPCH h
        INNER JOIN bronze.PCH1 l ON h.DocEntry = l.DocEntry
        LEFT JOIN bronze.OITM i ON l.ItemCode = i.ItemCode
        LEFT JOIN bronze.OITB g ON i.ItmsGrpCod = g.ItmsGrpCod
        LEFT JOIN bronze.NNM1 n ON h.Series = n.Series
        WHERE h.CANCELED = 'N'
          AND n.SeriesName <> 'HO'
          AND l.ItemCode <> 'NULL'
        GROUP BY h.DocNum, g.ItmsGrpNam, l.ItemCode, l.Dscription, l.WhsCode, h.DocDate, 
                 l.Quantity, l.DiscPrcnt, l.PriceBefDi, l.LineTotal, h.Series, n.SeriesName

        UNION ALL

        /* ---------------------------
           AP Credit Memo
           --------------------------- */
        SELECT 
            h.DocNum,
            CASE 
                WHEN g.ItmsGrpNam = 'IA' THEN 'IA'
                WHEN g.ItmsGrpNam = 'OMNI' THEN 'OMNI'
                ELSE 'Others'
            END AS ItemGroupName,
            l.ItemCode,
            l.Dscription,
            l.WhsCode,
            h.DocDate,
            -l.Quantity,
            -l.DiscPrcnt,
            -l.PriceBefDi,
            -l.LineTotal,
            h.Series,
            n.SeriesName,
            -ROUND(l.PriceBefDi * l.Quantity, 2),
            -SUM(ROUND(ROUND(l.PriceBefDi * (1 - (l.DiscPrcnt / 100)), 2) * l.Quantity, 2))
        FROM bronze.ORPC h
        INNER JOIN bronze.RPC1 l ON h.DocEntry = l.DocEntry
        LEFT JOIN bronze.OITM i ON l.ItemCode = i.ItemCode
        LEFT JOIN bronze.OITB g ON i.ItmsGrpCod = g.ItmsGrpCod
        LEFT JOIN bronze.NNM1 n ON h.Series = n.Series
        WHERE h.CANCELED = 'N'
          AND n.SeriesName <> 'HO'
          AND l.ItemCode <> 'NULL'
        GROUP BY h.DocNum, g.ItmsGrpNam, l.ItemCode, l.Dscription, l.WhsCode, h.DocDate, 
                 l.Quantity, l.DiscPrcnt, l.PriceBefDi, l.LineTotal, h.Series, n.SeriesName

        UNION ALL

        /* ---------------------------
           GRPO
           --------------------------- */
        SELECT 
            h.DocNum,
            CASE 
                WHEN g.ItmsGrpNam = 'IA' THEN 'IA'
                WHEN g.ItmsGrpNam = 'OMNI' THEN 'OMNI'
                ELSE 'Others'
            END AS ItemGroupName,
            l.ItemCode,
            l.Dscription,
            l.WhsCode,
            h.DocDate,
            l.Quantity,
            l.DiscPrcnt,
            l.PriceBefDi,
            l.LineTotal,
            h.Series,
            n.SeriesName,
            ROUND(l.PriceBefDi * l.Quantity, 2),
            SUM(ROUND(ROUND(l.PriceBefDi * (1 - (l.DiscPrcnt / 100)), 2) * l.Quantity, 2))
        FROM bronze.OPDN h
        INNER JOIN bronze.PDN1 l ON h.DocEntry = l.DocEntry
        LEFT JOIN bronze.OITM i ON l.ItemCode = i.ItemCode
        LEFT JOIN bronze.OITB g ON i.ItmsGrpCod = g.ItmsGrpCod
        LEFT JOIN bronze.NNM1 n ON h.Series = n.Series
        WHERE h.CANCELED = 'N'
          AND h.DocStatus = 'O'
          AND n.SeriesName <> 'HO'
          AND l.ItemCode <> 'NULL'
        GROUP BY h.DocNum, g.ItmsGrpNam, l.ItemCode, l.Dscription, l.WhsCode, h.DocDate, 
                 l.Quantity, l.DiscPrcnt, l.PriceBefDi, l.LineTotal, h.Series, n.SeriesName;

        /* Log success */
        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, RowCount)
        VALUES ('sp_Load_Silver_GRPO_View_byweb', @StartTime, GETDATE(), 'Success', (SELECT COUNT(*) FROM silver.GRPO_View_byweb));
    END TRY
    BEGIN CATCH
        SET @Err = ERROR_MESSAGE();
        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, RowCount, ErrorMessage)
        VALUES ('sp_Load_Silver_GRPO_View_byweb', @StartTime, GETDATE(), 'Failed', 0, @Err);
        THROW;
    END CATCH
END;
GO
