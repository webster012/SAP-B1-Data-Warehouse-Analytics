/*Title: Add Silver Sales_View procedure

Comment: Refactored to exact UNION ALL logic of original SALES_VIEW*/

USE DataWarehouse;
GO

/* --------------------------------------------
   1) Silver table (structure aligned with SALES_VIEW)
   -------------------------------------------- */
IF OBJECT_ID('silver.Sales_View','U') IS NOT NULL
    DROP TABLE silver.Sales_View;
GO

CREATE TABLE silver.Sales_View (
    TRType                     NVARCHAR(20),
    DocNum                     NVARCHAR(50),
    DocNo                      NVARCHAR(60),   -- formatted Doc No
    ItemGroupName              NVARCHAR(50),
    ActualItmsGrpNam           NVARCHAR(100),
    ItmsGrpCod                 NVARCHAR(50),
    DocDate                    DATE,
    CardCode                   NVARCHAR(50),
    DocType                    NVARCHAR(50),
    CardName                   NVARCHAR(200),
    CustomerBalance            DECIMAL(18,2),
    ItemCode                   NVARCHAR(50),
    ItemDesc                   NVARCHAR(200),
    SalesTotal                 DECIMAL(18,2),
    LineTotal                  DECIMAL(18,2),
    NetSalesAmt                DECIMAL(18,2),
    GrssProfit                 DECIMAL(18,2),
    GP_Percent                 DECIMAL(10,6),
    ItemCost                   DECIMAL(18,6),
    SalePrice                  DECIMAL(18,6),
    Qty                        DECIMAL(18,6),
    SalesAtCost                DECIMAL(18,6),
    AgentCode                  NVARCHAR(50),
    Agent                      NVARCHAR(200),
    SalesClassificationCode    NVARCHAR(50),
    SalesClassification        NVARCHAR(200),
    WhsCode                    NVARCHAR(50),
    Series                     INT,
    SeriesName                 NVARCHAR(200),
    CardType                   NVARCHAR(50),
    LoadDate                   DATETIME DEFAULT GETDATE()
);
GO

/* ------------------------------------------------
   2) Stored proc with UNION ALL (clone of SALES_VIEW)
   ------------------------------------------------ */
IF OBJECT_ID('dbo.sp_Load_Silver_Sales_View','P') IS NOT NULL
    DROP PROCEDURE dbo.sp_Load_Silver_Sales_View;
GO

CREATE PROCEDURE dbo.sp_Load_Silver_Sales_View
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @Err NVARCHAR(MAX);

    BEGIN TRY
        TRUNCATE TABLE silver.Sales_View;

        INSERT INTO silver.Sales_View (
            TRType, DocNum, DocNo, ItemGroupName, ActualItmsGrpNam, ItmsGrpCod,
            DocDate, CardCode, DocType, CardName, CustomerBalance,
            ItemCode, ItemDesc, SalesTotal, LineTotal, NetSalesAmt, GrssProfit,
            GP_Percent, ItemCost, SalePrice, Qty, SalesAtCost,
            AgentCode, Agent, SalesClassificationCode, SalesClassification,
            WhsCode, Series, SeriesName, CardType
        )
        /* ---------------------------
           Invoices
           --------------------------- */
        SELECT DISTINCT
            'Invoice' AS TRType,
            h.DocNum,
            CONCAT('IN ', h.DocNum) AS DocNo,

            CASE 
                WHEN g.ItmsGrpNam = 'IA' THEN 'IA'
                WHEN g.ItmsGrpNam = 'OMNI' THEN 'OMNI'
                ELSE 'Others'
            END AS ItemGroupName,
            COALESCE(g.ItmsGrpNam, '<BLANK>') AS ActualItmsGrpNam,
            COALESCE(CAST(g.ItmsGrpCod AS NVARCHAR(50)), '0') AS ItmsGrpCod,

            h.DocDate,
            h.CardCode,
            h.DocType,
            h.CardName,
            c.Balance AS CustomerBalance,

            COALESCE(l.ItemCode, '<BLANK>') AS ItemCode,
            COALESCE(l.Dscription, '<BLANK>') AS ItemDesc,

            h.DocTotal AS SalesTotal,
            l.LineTotal,
            SUM(l.LineTotal) AS NetSalesAmt,
            l.GrssProfit,
            CASE WHEN SUM(l.LineTotal) = 0 THEN 0
                 ELSE l.GrssProfit / NULLIF(SUM(l.LineTotal),0) * 100 END AS GP_Percent,

            l.StockPrice AS ItemCost,
            l.Price AS SalePrice,
            l.Quantity AS Qty,
            l.StockPrice * l.Quantity AS SalesAtCost,

            COALESCE(sat.Code, '<BLANK>') AS AgentCode,
            COALESCE(sat.Name, 'Branch') AS Agent,
            COALESCE(sct.Code, '<BLANK>') AS SalesClassificationCode,
            COALESCE(sct.Name, 'Branch') AS SalesClassification,

            l.WhsCode,
            n.Series,
            n.SeriesName,
            c.CardType
        FROM bronze.OINV h
        INNER JOIN bronze.OCRD c ON h.CardCode = c.CardCode
        LEFT JOIN bronze.INV1 l ON h.DocEntry = l.DocEntry
        LEFT JOIN bronze.OITM i ON l.ItemCode = i.ItemCode
        LEFT JOIN bronze.OITB g ON i.ItmsGrpCod = g.ItmsGrpCod
        LEFT JOIN bronze.SAT sat ON c.U_AGENTS = sat.Code
        LEFT JOIN bronze.SCT sct ON c.U_SALES_CLASSIFICATION = sct.Code
        LEFT JOIN bronze.NNM1 n ON h.Series = n.Series
        WHERE h.CANCELED = 'N'
          AND h.DocNum NOT IN (80002985, 80002986, 80002987)
        GROUP BY 
            h.DocNum, g.ItmsGrpCod, g.ItmsGrpNam, h.DocDate, h.CardCode, h.CardName, 
            h.DocTotal, c.Balance, l.ItemCode, l.Dscription, l.LineTotal, 
            l.GrssProfit, l.StockPrice, l.Price, l.Quantity, sat.Code, sat.Name, 
            sct.Code, sct.Name, n.Series, n.SeriesName, c.CardType, l.WhsCode, h.DocType

        UNION ALL

        /* ---------------------------
           Credits
           --------------------------- */
        SELECT DISTINCT
            'Credit' AS TRType,
            h.DocNum,
            CONCAT('CN ', h.DocNum) AS DocNo,

            CASE 
                WHEN g.ItmsGrpNam = 'IA' THEN 'IA'
                WHEN g.ItmsGrpNam = 'OMNI' THEN 'OMNI'
                ELSE 'Others'
            END AS ItemGroupName,
            COALESCE(g.ItmsGrpNam, '<BLANK>') AS ActualItmsGrpNam,
            COALESCE(CAST(g.ItmsGrpCod AS NVARCHAR(50)), '0') AS ItmsGrpCod,

            h.DocDate,
            h.CardCode,
            h.DocType,
            h.CardName,
            -c.Balance AS CustomerBalance,

            COALESCE(l.ItemCode, '<BLANK>') AS ItemCode,
            COALESCE(l.Dscription, '<BLANK>') AS ItemDesc,

            h.DocTotal AS SalesTotal,
            -l.LineTotal AS LineTotal,
            -SUM(l.LineTotal) AS NetSalesAmt,
            -l.GrssProfit AS GrssProfit,
            CASE WHEN SUM(l.LineTotal) = 0 THEN 0
                 ELSE (-l.GrssProfit) / NULLIF(SUM(l.LineTotal),0) * 100 END AS GP_Percent,

            -l.StockPrice AS ItemCost,
            -l.Price AS SalePrice,
            -l.Quantity AS Qty,
            -(l.StockPrice * l.Quantity) AS SalesAtCost,

            COALESCE(sat.Code, '<BLANK>') AS AgentCode,
            COALESCE(sat.Name, 'Branch') AS Agent,
            COALESCE(sct.Code, '<BLANK>') AS SalesClassificationCode,
            COALESCE(sct.Name, 'Branch') AS SalesClassification,

            l.WhsCode,
            n.Series,
            n.SeriesName,
            c.CardType
        FROM bronze.ORIN h
        INNER JOIN bronze.OCRD c ON h.CardCode = c.CardCode
        LEFT JOIN bronze.RIN1 l ON h.DocEntry = l.DocEntry
        LEFT JOIN bronze.OITM i ON l.ItemCode = i.ItemCode
        LEFT JOIN bronze.OITB g ON i.ItmsGrpCod = g.ItmsGrpCod
        LEFT JOIN bronze.SAT sat ON c.U_AGENTS = sat.Code
        LEFT JOIN bronze.SCT sct ON c.U_SALES_CLASSIFICATION = sct.Code
        LEFT JOIN bronze.NNM1 n ON h.Series = n.Series
        WHERE h.CANCELED = 'N'
          AND h.DocNum NOT IN (80002985, 80002986, 80002987)
        GROUP BY 
            h.DocNum, g.ItmsGrpCod, g.ItmsGrpNam, h.DocDate, h.CardCode, h.CardName, 
            h.DocTotal, c.Balance, l.ItemCode, l.Dscription, l.LineTotal, 
            l.GrssProfit, l.StockPrice, l.Price, l.Quantity, sat.Code, sat.Name, 
            sct.Code, sct.Name, n.Series, n.SeriesName, c.CardType, l.WhsCode, h.DocType;

        /* Log success */
        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, RowCount)
        VALUES ('sp_Load_Silver_Sales_View', @StartTime, GETDATE(), 'Success', (SELECT COUNT(*) FROM silver.Sales_View));
    END TRY
    BEGIN CATCH
        SET @Err = ERROR_MESSAGE();
        INSERT INTO dbo.ETL_RunLog (PackageName, StartTime, EndTime, Status, RowCount, ErrorMessage)
        VALUES ('sp_Load_Silver_Sales_View', @StartTime, GETDATE(), 'Failed', 0, @Err);
        THROW;
    END CATCH
END;
GO
