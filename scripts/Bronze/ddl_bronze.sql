USE DataWarehouse;
GO

/* ================================
   SALES (OINV = Invoice Header)
================================= */
IF OBJECT_ID('bronze.OINV','U') IS NOT NULL DROP TABLE bronze.OINV;
CREATE TABLE bronze.OINV (
    DocEntry INT PRIMARY KEY,
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

/* ================================
   SALES LINES (INV1)
================================= */
IF OBJECT_ID('bronze.INV1','U') IS NOT NULL DROP TABLE bronze.INV1;
CREATE TABLE bronze.INV1 (
    DocEntry INT,
    LineNum INT,
    ItemCode NVARCHAR(50),
    Dscription NVARCHAR(200),
    LineTotal DECIMAL(18,2),
    GrssProfit DECIMAL(18,2),
    StockPrice DECIMAL(18,6),
    Price DECIMAL(18,6),
    Quantity DECIMAL(18,6),
    WhsCode NVARCHAR(50),
    CONSTRAINT PK_INV1 PRIMARY KEY (DocEntry, LineNum)
);
GO

/* ================================
   CREDIT NOTES (ORIN = Header)
================================= */
IF OBJECT_ID('bronze.ORIN','U') IS NOT NULL DROP TABLE bronze.ORIN;
CREATE TABLE bronze.ORIN (
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
   CREDIT NOTES LINES (RIN1)
================================= */
IF OBJECT_ID('bronze.RIN1','U') IS NOT NULL DROP TABLE bronze.RIN1;
CREATE TABLE bronze.RIN1 (
    DocEntry INT,
    LineNum INT,
    ItemCode NVARCHAR(50),
    Dscription NVARCHAR(200),
    LineTotal DECIMAL(18,2),
    Quantity DECIMAL(18,6),
    StockPrice DECIMAL(18,6),
    WhsCode NVARCHAR(50),
    CONSTRAINT PK_RIN1 PRIMARY KEY (DocEntry, LineNum)
);
GO

/* ================================
   CUSTOMERS (OCRD)
================================= */
IF OBJECT_ID('bronze.OCRD','U') IS NOT NULL DROP TABLE bronze.OCRD;
CREATE TABLE bronze.OCRD (
    CardCode NVARCHAR(50) PRIMARY KEY,
    CardName NVARCHAR(200),
    Balance DECIMAL(18,2),
    CardType NVARCHAR(20),
    U_AGENTS NVARCHAR(50),
    U_SALES_CLASSIFICATION NVARCHAR(50)
);
GO

/* ================================
   ITEMS (OITM)
================================= */
IF OBJECT_ID('bronze.OITM','U') IS NOT NULL DROP TABLE bronze.OITM;
CREATE TABLE bronze.OITM (
    ItemCode NVARCHAR(50) PRIMARY KEY,
    ItemName NVARCHAR(200),
    ItmsGrpCod INT
);
GO

/* ================================
   ITEM GROUPS (OITB)
================================= */
IF OBJECT_ID('bronze.OITB','U') IS NOT NULL DROP TABLE bronze.OITB;
CREATE TABLE bronze.OITB (
    ItmsGrpCod INT PRIMARY KEY,
    ItmsGrpNam NVARCHAR(100)
);
GO

/* ================================
   DOCUMENT SERIES (NNM1)
================================= */
IF OBJECT_ID('bronze.NNM1','U') IS NOT NULL DROP TABLE bronze.NNM1;
CREATE TABLE bronze.NNM1 (
    Series INT PRIMARY KEY,
    SeriesName NVARCHAR(100)
);
GO

/* ================================
   TRANSFERS (OWTR = Header)
================================= */
IF OBJECT_ID('bronze.OWTR','U') IS NOT NULL DROP TABLE bronze.OWTR;
CREATE TABLE bronze.OWTR (
    DocEntry INT PRIMARY KEY,
    DocNum NVARCHAR(50),
    DocDate DATE,
    Filler NVARCHAR(100),   -- From Warehouse
    UpdateDate DATETIME
);
GO

/* ================================
   TRANSFERS LINES (WTR1)
================================= */
IF OBJECT_ID('bronze.WTR1','U') IS NOT NULL DROP TABLE bronze.WTR1;
CREATE TABLE bronze.WTR1 (
    DocEntry INT,
    LineNum INT,
    ItemCode NVARCHAR(50),
    Dscription NVARCHAR(200),
    FromWhsCod NVARCHAR(50),
    WhsCode NVARCHAR(50),
    Quantity DECIMAL(18,6),
    StockPrice DECIMAL(18,6),
    CONSTRAINT PK_WTR1 PRIMARY KEY (DocEntry, LineNum)
);
GO

/* ================================
   AP INVOICES (OPCH = Header)
================================= */
IF OBJECT_ID('bronze.OPCH','U') IS NOT NULL DROP TABLE bronze.OPCH;
CREATE TABLE bronze.OPCH (
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
   AP INVOICES LINES (PCH1)
================================= */
IF OBJECT_ID('bronze.PCH1','U') IS NOT NULL DROP TABLE bronze.PCH1;
CREATE TABLE bronze.PCH1 (
    DocEntry INT,
    LineNum INT,
    ItemCode NVARCHAR(50),
    Dscription NVARCHAR(200),
    LineTotal DECIMAL(18,2),
    Quantity DECIMAL(18,6),
    StockPrice DECIMAL(18,6),
    WhsCode NVARCHAR(50),
    CONSTRAINT PK_PCH1 PRIMARY KEY (DocEntry, LineNum)
);
GO

/* ================================
   USER TABLE - Sales Agents ([@SAT])
================================= */
IF OBJECT_ID('bronze.SAT','U') IS NOT NULL DROP TABLE bronze.SAT;
CREATE TABLE bronze.SAT (
    Code NVARCHAR(50) PRIMARY KEY,
    Name NVARCHAR(200)
);
GO

/* ================================
   USER TABLE - Sales Classification ([@SCT])
================================= */
IF OBJECT_ID('bronze.SCT','U') IS NOT NULL DROP TABLE bronze.SCT;
CREATE TABLE bronze.SCT (
    Code NVARCHAR(50) PRIMARY KEY,
    Name NVARCHAR(200)
);
GO

/* ================================
   SALES RETURNS (ORDN = Return Delivery Header)
================================= */
IF OBJECT_ID('bronze.ORDN','U') IS NOT NULL DROP TABLE bronze.ORDN;
CREATE TABLE bronze.ORDN (
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
  AP Credit Notes (ORPC = AP Credit Notes Header)
================================= */
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
  AP Credit Notes (RPC1 = AP Credit Notes Lines)
================================= */
IF OBJECT_ID('bronze.RPC1','U') IS NOT NULL DROP TABLE bronze.RPC1;
CREATE TABLE bronze.RPC1 (
    DocEntry INT,
    LineNum INT,
    ItemCode NVARCHAR(50),
    Dscription NVARCHAR(200),
    LineTotal DECIMAL(18,2),
    Quantity DECIMAL(18,6),
    StockPrice DECIMAL(18,6),
    WhsCode NVARCHAR(50),
    CONSTRAINT PK_RPC1 PRIMARY KEY (DocEntry, LineNum)
);
GO


/* ================================
  Goods Receipt PO (OPDN = Header)
================================= */
IF OBJECT_ID('bronze.OPDN','U') IS NOT NULL DROP TABLE bronze.OPDN;
CREATE TABLE bronze.OPDN (
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
   Goods Receipt PO (PDN1 Lines) 
================================= */
-- Goods Receipt PO (Lines)
IF OBJECT_ID('bronze.PDN1','U') IS NOT NULL DROP TABLE bronze.PDN1;
CREATE TABLE bronze.PDN1 (
    DocEntry INT,
    LineNum INT,
    ItemCode NVARCHAR(50),
    Dscription NVARCHAR(200),
    LineTotal DECIMAL(18,2),
    Quantity DECIMAL(18,6),
    StockPrice DECIMAL(18,6),
    WhsCode NVARCHAR(50),
    CONSTRAINT PK_PDN1 PRIMARY KEY (DocEntry, LineNum)
);
GO
