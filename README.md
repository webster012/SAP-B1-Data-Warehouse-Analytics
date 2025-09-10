# SAP B1 SQL Data Warehouse Project  

Welcome to the **SAP B1 SQL Data Warehouse Project** repository! 📊  

This project demonstrates the end-to-end process of building a **data warehouse solution for SAP Business One (SAP B1)**, designed to centralize and optimize business data for analytics and reporting.  

The solution covers:  
- **Data Extraction & Integration** from SAP B1 SQL Server  
- **Data Transformation & Modeling** using industry best practices  
- **Data Warehouse Design** for scalable analytics  
- **Power BI Dashboards & Reports** for actionable business insights  

Designed as a **portfolio project**, this repository highlights practical expertise in **data engineering, business intelligence, and SAP B1 systems integration**.  

---

## 🏗️ Data Architecture  

The data architecture for this project follows the **Medallion Architecture** with **Bronze**, **Silver**, and **Gold** layers:  

<img width="867" height="656" alt="HIGH LEVEL ARCHITECTURE FOR SAP B1 DWH ANALYTICS drawio" src="https://github.com/user-attachments/assets/e36b18b0-f265-4fc7-ac6d-c5bc317b1230" />

- **Bronze Layer**: Stores raw data extracted directly from **SAP Business One SQL Server tables**. Data is ingested via batch processing (full load: truncate & insert) without transformations, preserving the source system’s structure.  

- **Silver Layer**: Cleansed and standardized data. This layer applies **data cleansing, standardization, normalization, and enrichment**. Derived columns and integrations are also handled here, ensuring consistency and accuracy across entities.  

- **Gold Layer**: Contains **business-ready data** optimized for analytics. Data is transformed into **star schemas, flat tables, or aggregated views** with applied business logic, enabling efficient reporting, ad-hoc queries, and machine learning use cases.  

---

## 📖 Project Overview  

This project involves:  
1. **Data Architecture** – Designing a modern data warehouse using the **Medallion Architecture** (Bronze, Silver, Gold layers).  
2. **ETL Pipelines** – Extracting, transforming, and loading data from SAP B1 into the warehouse.  
3. **Data Modeling** – Developing fact and dimension tables optimized for analytical queries.  
4. **Analytics & Reporting** – Creating SQL-based reports and Power BI dashboards for actionable insights.  

🎯 This repository is an excellent resource to showcase my expertise in:  
- SQL Development  
- Data Architecture  
- Data Engineering  
- ETL Pipeline Development  
- Data Modeling  
- Data Analytics  

---

## 🚀 Project Requirements  

### 🏗️ Building the Data Warehouse (Data Engineering)  

#### 🎯 Objective  
Develop a modern data warehouse using SQL Server to consolidate **SAP Business One sales and customer data**, enabling advanced analytics, reporting, and strategic decision-making.  

#### 📌 Specifications  
- **Data Sources**:  
  - `OINV / ORIN` → Invoices and Credit Notes  
  - `OCRD` → Customer Master Data (CardCode, CardName, Balance, Classification)  
  - `INV1 / RIN1` → Sales Line Details (Item, Quantity, Prices, Costs, Gross Profit)  
  - `OITM / OITB` → Items and Item Groups (Product categorization)  
  - `[@SAT]` → Agent Master (Sales Agents)  
  - `[@SCT]` → Sales Classification  
  - `NNM1` → Document Series  

- **Data Quality**:  
  - Exclude canceled transactions  
  - Handle missing values with defaults (`<BLANK>`)  
  - Properly reflect negative balances for credit notes  

- **Integration**:  
  - Combine invoices and credit notes into a **unified sales view (`SALES_VIEW`)**  
  - Standardize calculations for sales, cost, and gross profit  

- **Scope**: Focus on **transaction-level data** (sales and returns) for financial and operational reporting.  

- **Documentation**: Maintain a clear **data model** with relationships between sales, customers, items, and agents for both technical and business users.  

---

### 📊 BI: Analytics & Reporting (Data Analytics)  

#### 🎯 Objective  
Develop **SQL-based analytics and Power BI dashboards** to deliver actionable insights into:  

- **Customer Behavior**  
  - Outstanding balances  
  - Credit utilization  
  - Classification by sales category  

- **Product & Item Group Performance**  
  - Sales & returns by Item Group (IA, OMNI, Others)  
  - Profitability analysis (Gross Profit %, Sales at Cost vs. Sales Total)  

- **Sales Trends & Performance**  
  - Monthly and yearly sales trends  
  - Sales Agent performance and contribution  
  - Branch / Warehouse sales distribution  

These insights empower business leaders with **key financial and operational metrics**, enabling **data-driven decisions** for growth and efficiency.  

---

## 🛡️ License  
This project is licensed under the [MIT License](LICENSE). You are free to use, modify, and share this project with proper attribution.  

---

## 🌟 About Me  
Hi there! I’m **Webster Jayson Sacluti**, an **SAP Specialist, Power BI Expert, and IT Professional** passionate about transforming business data into meaningful insights.  

With experience in **SAP Business One, SQL Server, and Power BI**, I help organizations streamline operations, build efficient data models, and create actionable dashboards. I’m also continuously exploring **AI-driven business solutions** to empower companies with smarter decision-making.  
