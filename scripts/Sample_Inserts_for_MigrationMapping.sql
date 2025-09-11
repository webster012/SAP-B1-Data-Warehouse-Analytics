USE DataWarehouse;
GO

/* ============================
   Bronze Layer (Raw Extracts)
=============================== */
INSERT INTO dbo.MigrationMapping (SourceName, SourceType, TargetSchema, TargetTable, TransformationNotes)
VALUES
('OINV', 'SAP Table', 'bronze', 'OINV_Raw', 'Raw Sales Invoices (incremental by DocDate/UpdateDate)'),
('ORIN', 'SAP Table', 'bronze', 'ORIN_Raw', 'Raw Credit Notes'),
('OWTR', 'SAP Table', 'bronze', 'OWTR_Raw', 'Raw Inventory Transfers Header'),
('WTR1', 'SAP Table', 'bronze', 'WTR1_Raw', 'Raw Inventory Transfers Lines'),
('OPCH', 'SAP Table', 'bronze', 'OPCH_Raw', 'Raw AP Invoices'),
('ORPC', 'SAP Table', 'bronze', 'ORPC_Raw', 'Raw AP Credit Memos'),
('OPDN', 'SAP Table', 'bronze', 'OPDN_Raw', 'Raw Goods Receipt PO'),
('Profit Center TB.xlsx', 'Excel File', 'bronze', 'ProfitCenterTB_Raw', 'Excel upload for mapping Profit Center codes');


/* ============================
   Silver Layer (Business Logic)
=============================== */
INSERT INTO dbo.MigrationMapping (SourceName, SourceType, TargetSchema, TargetTable, TransformationNotes)
VALUES
('SALES_VIEW', 'SQL View', 'silver', 'Sales_Staging', 'Combine Invoices + Credit Notes, adjusted balances'),
('InventoryTransferView', 'SQL View', 'silver', 'InvTransfer_Staging', 'FromWhse/ToWhse, item cost calc'),
('GRPO_VIEW_byweb', 'SQL View', 'silver', 'Purchases_Staging', 'Combine AP Inv, AP CM, GRPO, Goods Return'),
('OWHS + OLCT', 'Power Query Join', 'silver', 'DimWarehouse_Staging', 'Warehouse + Location dimension clean'),
('Profit Center TB (Excel)', 'Power Query', 'silver', 'DimProfitCenter_Staging', 'Deduplicated, mapped to warehouses');


/* ============================
   Gold Layer (DW Facts & Dims)
=============================== */
INSERT INTO dbo.MigrationMapping (SourceName, SourceType, TargetSchema, TargetTable, TransformationNotes)
VALUES
('Sales_Staging', 'Silver Table', 'gold', 'FactSales', 'Fact table for Sales with measures'),
('Purchases_Staging', 'Silver Table', 'gold', 'FactPurchases', 'Fact table for AP Invoices + GRPO'),
('InvTransfer_Staging', 'Silver Table', 'gold', 'FactTransfers', 'Fact table for Warehouse Transfers'),
('Combined_Transactions2 (Power Query)', 'M Query', 'gold', 'FactInventoryFlow', 'Merged Sales + GRPO + Transfers, unified fact'),
('Inventory_Flow_Base (Power Query)', 'M Query', 'gold', 'FactInventoryFlow_Base', 'Monthly grouped flows: Sales @ Cost, Purchases, Transfers'),
('Flow_Agg (Power Query)', 'M Query', 'gold', 'FactFlowAgg', 'Aggregated by IA vs OMNI/Others'),
('InventoryOpeningBalance (View)', 'SQL View', 'gold', 'FactInventoryOB', 'Beginning balances per store/month'),
('fn_CalculateInventoryBalance (Power Query Function)', 'M Function', 'gold', 'FactInventoryBalance', 'Rolling balances (Beginning/Ending per month per store)'),
('Inv_Pur_Sal_Fact (Power Query)', 'M Query', 'gold', 'FactInvPurSal', 'Final inventory flow fact with purchases, transfers, sales, balances'),
('DimWarehouse_Staging', 'Silver Table', 'gold', 'DimWarehouse', 'Cleansed warehouse dimension'),
('DimProfitCenter_Staging', 'Silver Table', 'gold', 'DimProfitCenter', 'Profit Center dimension'),
('SAP Master Data (OCRD, OITM)', 'SAP Tables', 'gold', 'DimCustomer, DimItem', 'Master dimensions for customers and items'),
('Calendar (Power Query: MonthYearList)', 'M Query', 'gold', 'DimDate', 'Date dimension (MonthSort, MonthYear, etc.)');
