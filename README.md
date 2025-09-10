<img width="867" height="656" alt="HIGH LEVEL ARCHITECTURE FOR SAP B1 DWH ANALYTICS drawio" src="https://github.com/user-attachments/assets/ada0bb52-b86e-4008-a760-812813d0b43d" /># SAP B1 SQL Data Warehouse Project  

Welcome to the **SAP B1 SQL Data Warehouse Project** repository! 📊  

This project demonstrates the end-to-end process of building a **data warehouse solution for SAP Business One (SAP B1)**, designed to centralize and optimize business data for analytics and reporting.  

The solution covers:  
- **Data Extraction & Integration** from SAP B1 SQL Server  
- **Data Transformation & Modeling** using industry best practices  
- **Data Warehouse Design** for scalable analytics  
- **Power BI Dashboards & Reports** for actionable business insights  

Designed as a **portfolio project**, this repository highlights practical expertise in **data engineering, business intelligence, and SAP B1 systems integration**.  

---
###🏗️ Data Architecture
The data architecture for this project follows Medallion Architecture Bronze, Silver, and Gold layers:
![Uploading HIGH LEVEL ARCHITECTURE FOR SAP B1 DWH ANALYTICS.drawio.png…]()



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

- **Data Quality**: Ensure canceled transactions are excluded, missing values are handled with defaults (`<BLANK>`), and negative balances are properly reflected for credit notes.  

- **Integration**: Combine invoices and credit notes into a **unified sales view (`SALES_VIEW`)**, standardizing calculations for sales, cost, and gross profit.  

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
