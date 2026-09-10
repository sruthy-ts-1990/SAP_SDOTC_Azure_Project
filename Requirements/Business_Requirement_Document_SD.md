# BUSINESS REQUIREMENT DOCUMENT

## Sales & Distribution (SD) Analytics Project

*Order-to-Cash Reporting Platform — SAP-Sourced Data*

| Field | Detail |
|---|---|
| Document Type | Business Requirement Document (BRD) |
| Business Module | Sales & Distribution (SD) |
| Prepared For | Data Engineering Team (Training Cohort) |
| Prepared By | Business Analyst |
| Source System | SAP ECC (replicated), sourced via public dataset: Kaggle — "SAP DATASET \| BigQuery Dataset" (mustafakeser4/sap-dataset-bigquery-dataset), originally cloud-training-demos.SAP_REPLICATED_DATA |
| Version | 1.0 |
| Status | Approved for Development |
| Date | 13 July 2026 |

---

## 1. Project Background

This project simulates a real-world data engineering engagement for the Sales & Distribution (SD) module of a manufacturing/distribution company running SAP ECC. As part of a training program, students will act as Data Engineers receiving requirements from a Business Analyst (BA) and an architecture blueprint from a Data Architect, and will be responsible for building the ingestion, transformation, and data modelling pipeline end to end.

The source data is a publicly available extract of standard SAP tables, replicated to Google BigQuery and republished on Kaggle: https://www.kaggle.com/datasets/mustafakeser4/sap-dataset-bigquery-dataset. This dataset contains standard SAP Sales & Distribution (SD), Materials Management (MM) and cross-application (currency) tables in their native structure — i.e. header/item pairs, document flow tables, and master data tables — exactly as a real SAP landscape would expose them to a replication tool (e.g. SAP LT Replication Server / Qlik Replicate).

Because no SAP functional specialist is available on this project, this document explicitly names the source table(s) and fields required for every data point, so the Data Engineering team can self-serve without needing SAP domain expertise.

## 2. Business Objective

Build an analytics platform that reconstructs the SAP Order-to-Cash (OTC) process — Sales Order → Delivery → Billing — from replicated SAP source tables, converts all monetary values into a single Group Reporting Currency, and exposes the result as a governed Power BI semantic model so Data Analysts can self-serve sales performance reporting without writing SQL.

Key business questions the platform must answer:

- What is our Net Sales value, in a single reporting currency, by month, sales organization, distribution channel, division, customer and material?
- How efficiently are we converting Sales Orders into Deliveries and Invoices (cycle time, on-time delivery, backlog)?
- Which customers / materials / sales organizations are driving revenue, and how has this trended over time?
- Where are we losing value in the pipeline — orders not delivered, delivered but not billed, delayed billing?

## 3. Scope

### 3.1 In Scope

- Sales Order data (header + item)
- Delivery (outbound shipping) data (header + item)
- Billing / Invoice data (header + item)
- Document flow linking Sales Order → Delivery → Billing
- Customer master data (general + sales area)
- Material master data (general, description, sales area)
- Currency exchange rate conversion for all monetary reporting
- A Power BI semantic model consumed by Data Analysts

### 3.2 Out of Scope

- Finance/Controlling (FI/CO) postings beyond currency master data (TCURR/TCURF)
- Procurement / Materials Management inbound processes
- Real-time / streaming ingestion (batch only for this phase)
- Write-back to SAP or any transactional system

## 4. Stakeholders

| Role | Responsibility |
|---|---|
| Business Analyst (BA) | Owns this BRD; translates business need into data requirements; sole point of contact for source-data questions since no SAP specialist is on the project |
| Data Architect | Defines the Azure target architecture and technical design standards (see companion Data Architecture Document) |
| Data Engineer (Student) | Builds ADF pipelines, Databricks transformations, and the Azure SQL Database serving layer per this BRD and the architecture document |
| Data Analyst | Consumes the published Power BI semantic model to build reports and dashboards |
| Instructor / Reviewer | Validates deliverables against this BRD |

## 5. Source System Overview — SAP Tables Used

The table below is the master reference for every SAP table used in this project. Since no SAP specialist is available, treat this table as authoritative for field meanings. All tables are standard, unmodified SAP structures and are present as-is in the Kaggle dataset.

| Table | SAP Description | Grain / Key |
|---|---|---|
| VBAK | Sales Document – Header (Sales Order header) | 1 row per Sales Order (VBELN) |
| VBAP | Sales Document – Item (Sales Order line item) | 1 row per Order line (VBELN + POSNR) |
| LIKP | Delivery – Header | 1 row per Delivery (VBELN) |
| LIPS | Delivery – Item | 1 row per Delivery line (VBELN + POSNR) |
| VBRK | Billing Document – Header (Invoice header) | 1 row per Billing doc (VBELN) |
| VBRP | Billing Document – Item (Invoice line item) | 1 row per Billing line (VBELN + POSNR) |
| VBFA | Sales Document Flow – links preceding → subsequent documents | 1 row per document-to-document link |
| KNA1 | Customer Master – General Data (name, address, country) | 1 row per Customer (KUNNR) |
| KNVV | Customer Master – Sales Area Data (per Sales Org/Channel/Division) | 1 row per Customer + Sales Area |
| MARA | Material Master – General Data (material type, group, base UoM) | 1 row per Material (MATNR) |
| MAKT | Material Descriptions (language-dependent) | 1 row per Material + Language (MATNR + SPRAS) |
| MVKE | Material Master – Sales Area Data | 1 row per Material + Sales Area |
| TCURR | Exchange Rates (daily/period rates by currency pair & rate type) | 1 row per Rate type + From-currency + To-currency + Valid-from date |
| TCURF | Exchange Rate Ratios (conversion factors, e.g. per 100 units) | 1 row per Rate type + From-currency + To-currency + Valid-from date |

## 6. Business Process — Order-to-Cash (OTC)

SAP SD represents a sale as a chain of three linked documents. Each stage is stored in its own header/item table pair, and the documents are linked to each other through the document flow table VBFA:

**Sales Order (VBAK / VBAP) → Delivery (LIKP / LIPS) → Billing / Invoice (VBRK / VBRP)**

VBFA (Document Flow) stores each link as a row: preceding document (VBELV/POSNV) → subsequent document (VBELN/POSNN) with a document category (VBTYP_N). To reconstruct the full chain for a given Sales Order, the Data Engineer must traverse VBFA from the order, to its delivery(ies), to its billing document(s) — an order can fan out into multiple deliveries and multiple invoices, and this relationship must be preserved rather than assumed 1:1.

## 7. Detailed Business Requirements

Each requirement below states the business need, the exact source table(s) and fields to use, and how those tables must be combined. Priority: M = Must Have, S = Should Have.

### BR-01 — Sales Order Fact

**Business Need:** Report all Sales Orders with order value, quantity, customer, material, sales organization and order date.

**Source Tables:** VBAK (header) joined to VBAP (item) on VBELN

**Key Fields:** VBAK: VBELN, ERDAT (order date), AUART (order type), VKORG (sales org), VTWEG (distribution channel), SPART (division), KUNNR (sold-to party), WAERK (document currency) | VBAP: VBELN, POSNR (item), MATNR (material), KWMENG (order quantity), NETWR (net value, item level), MEINS (unit of measure)

**Transformation Note:** Join VBAK to VBAP on VBELN (one header to many items). NETWR at item level should be summed for order-level value; do not double count header-level NETWR if present.

**Priority:** M

### BR-02 — Delivery Fact

**Business Need:** Track deliveries against their originating sales orders to measure fulfilment and on-time performance.

**Source Tables:** LIKP (header) joined to LIPS (item) on VBELN; LIPS joined back to VBAP via VGBEL (reference document) = VBAP.VBELN and VGPOS (reference item) = VBAP.POSNR

**Key Fields:** LIKP: VBELN, LFDAT (planned delivery date), WADAT_IST (actual goods issue date), KUNNR (ship-to) | LIPS: VBELN, POSNR, MATNR, LFIMG (delivery quantity), VGBEL, VGPOS

**Transformation Note:** VGBEL/VGPOS is the mechanism that links a delivery item back to its originating sales order item — use this (or VBFA, see BR-04) to compute order-to-delivery lead time = WADAT_IST − ERDAT (from VBAK).

**Priority:** M

### BR-03 — Billing / Invoice Fact

**Business Need:** Report invoiced revenue and link it back to the originating order and delivery for full pipeline visibility.

**Source Tables:** VBRK (header) joined to VBRP (item) on VBELN; VBRP joined to VBAP/LIPS via VGBEL/VGPOS (reference document/item, which may point to the order or the delivery depending on billing type)

**Key Fields:** VBRK: VBELN, FKDAT (billing date), WAERK (document currency), KUNRG (payer) | VBRP: VBELN, POSNR, MATNR, FKIMG (billed quantity), NETWR (net value), VGBEL, VGPOS

**Transformation Note:** NETWR here is in the billing document currency (WAERK) — must go through currency conversion (BR-06) before being combined with Sales Order value in a single reporting currency.

**Priority:** M

### BR-04 — End-to-End Order-to-Cash Document Chain

**Business Need:** A single reporting table that traces each Sales Order line through to its Delivery and Billing document(s), so partially-delivered or partially-billed orders are visible.

**Source Tables:** VBFA (document flow), combined with VBAK/VBAP, LIKP/LIPS and VBRK/VBRP

**Key Fields:** VBFA: VBELV (preceding doc), POSNV (preceding item), VBELN (subsequent doc), POSNN (subsequent item), VBTYP_N (subsequent document category, e.g. 'J' = Delivery, 'M' = Invoice)

**Transformation Note:** This requires chaining more than two SAP tables: start at VBAK/VBAP, use VBFA to find the linked LIKP/LIPS delivery, use VBFA again (or LIPS/VBRP VGBEL/VGPOS) to find the linked VBRK/VBRP billing document. Preserve 1-to-many relationships at every hop — one order line can have zero, one, or many deliveries/invoices.

**Priority:** M

### BR-05 — Customer Dimension

**Business Need:** A single, deduplicated customer dimension with both identity and sales-area attributes, tracking changes over time (e.g. customer moves sales group/region).

**Source Tables:** KNA1 (general data) joined to KNVV (sales area data) on KUNNR

**Key Fields:** KNA1: KUNNR, NAME1 (customer name), LAND1 (country), ORT01 (city), REGIO (region), KTOKD (account group) | KNVV: KUNNR, VKORG, VTWEG, SPART, KDGRP (customer group), VWERK (delivering plant)

**Transformation Note:** A customer can have multiple KNVV rows (one per Sales Org/Channel/Division combination) — do not assume one row per customer. This dimension must be built as SCD Type 2 (see Section 9) since customer sales-area attributes can change over time.

**Priority:** M

### BR-06 — Material Dimension

**Business Need:** A single material dimension with description and sales-relevant attributes.

**Source Tables:** MARA (general) joined to MAKT (description, filtered to SPRAS = 'E' for English) joined to MVKE (sales area data), all on MATNR

**Key Fields:** MARA: MATNR, MTART (material type), MATKL (material group), MEINS (base unit of measure) | MAKT: MATNR, SPRAS, MAKTX (material description) | MVKE: MATNR, VKORG, VTWEG, MVGR1 (material group 1 / product hierarchy proxy)

**Transformation Note:** MAKT is language-dependent — always filter to a single language (English) before joining, or you will fan out rows per language. MVKE can have multiple rows per material (per sales area); pick the relevant sales org/channel combination or model as SCD Type 2 if attributes vary.

**Priority:** M

### BR-07 — Currency Conversion for Group Reporting (Key Use Case)

**Business Need:** Sales Orders and Billing Documents are captured in whatever currency the customer / sales organization transacts in (field WAERK on VBAK and VBRK). Corporate management reporting requires every monetary value — order value, delivered value, billed/invoiced value — to be expressed in a single Group Reporting Currency so that totals can be compared and aggregated across countries.

**Source Tables:** TCURR (Exchange Rates) and TCURF (Exchange Rate Ratios)

**Key Fields:** TCURR: KURST (rate type, use 'M' = standard average rate for reporting), FCURR (from-currency, e.g. document currency), TCURR (to-currency, e.g. group currency), GDATU (rate valid-from date), UKURS (exchange rate) | TCURF: KURST, FCURR, TCURR, GDATU, FFACT (from-currency factor), TFACT (to-currency factor)

**Business Rule / Logic:**

- Rate type: always use KURST = 'M' (standard month-end/average rate) unless a specific report requests a different rate type.
- Rate lookup is "as of", not exact match: for a transaction dated D, find the TCURR row with the same FCURR/TCURR/KURST where GDATU is the maximum date that is ≤ D (i.e. the most recently published rate on or before the transaction date). SAP does not publish a rate for every single calendar date.
- Ratio adjustment: some currency pairs are not quoted 1-to-1 (e.g. rates for JPY, KRW, IDR are often quoted per 100 units). Always apply the TCURF ratio: Converted Amount = Document Amount × UKURS × (TFACT ÷ FFACT).
- Group Reporting Currency for this project: USD (assumption — confirm with Finance in a real engagement; document the assumption if changed).
- Every fact table that carries a monetary amount (Sales Order NETWR, Delivery value, Billing NETWR) must carry both the original document-currency amount and the converted group-currency amount, plus the WAERK and the exchange rate/date used, for auditability.

**Priority:** M — blocking requirement; no cross-currency total may be shown in Power BI without this conversion applied upstream in Databricks.

### BR-08 — Sales Performance Semantic Model (Power BI)

**Business Need:** Data Analysts need a self-service semantic model (no SQL required) covering the OTC process, in a single reporting currency.

**Required Dimensions:** Dim_Customer, Dim_Material, Dim_Date, Dim_SalesOrganization (VKORG/VTWEG/SPART, can be derived from KNVV/MVKE/VBAK), Dim_CurrencyRate (for transparency/audit)

**Required Facts:** Fact_SalesOrder, Fact_Delivery, Fact_Billing, Fact_OrderToCash (the BR-04 chained view)

**Required Measures (examples):** Net Sales Value (Group Currency), Order Quantity, Delivered Quantity, Billed Quantity, Order-to-Delivery Lead Time (days), Delivery-to-Billing Lead Time (days), On-Time Delivery %, Open Order Value (ordered but not yet delivered), Unbilled Delivery Value

**Priority:** M

## 8. Data Quality & Validation Requirements

- Referential integrity: every VBAP.VBELN must exist in VBAK; every LIPS.VBELN must exist in LIKP; every VBRP.VBELN must exist in VBRK. Orphan rows must be logged, not silently dropped.
- Currency lookup gaps: if no TCURR rate is found on or before the transaction date for a required currency pair, the record must be routed to a data-quality exception table, not defaulted to a rate of 1.0.
- Duplicate detection: primary keys (VBELN+POSNR, KUNNR, MATNR+SPRAS) must be unique after cleansing; duplicates must be logged and resolved by "latest change wins" unless a change-date field indicates otherwise.
- Reconciliation: total row counts and sum(NETWR) per source table must be compared between the Bronze (raw) layer and Gold (curated) layer after each load, with variances reported.

## 9. Non-Functional Requirements

| Area | Requirement |
|---|---|
| Load pattern | Batch, full historical load initially; incremental/delta load thereafter where a change-date field is available |
| Refresh frequency | Daily (simulated — dataset is static, but pipelines must be built to run on a daily schedule) |
| History tracking | Customer and Material dimensions must be SCD Type 2 (retain history of attribute changes) |
| Auditability | Every converted monetary value must retain the original amount, source currency, exchange rate and rate date used |
| Data volume | Training-scale (Kaggle extract); design should not assume in-memory processing only — use Spark-scale patterns |
| Access | Data Analysts consume via Power BI only; no direct Azure SQL Database or Databricks access required for reporting |

## 10. Assumptions & Constraints

- No SAP functional specialist is available; all table/field definitions in this document are based on standard, documented SAP SD/MM table structures and must be treated as authoritative for this project.
- The Kaggle dataset (mustafakeser4/sap-dataset-bigquery-dataset, based on cloud-training-demos.SAP_REPLICATED_DATA) is a static, one-time extract — there is no live SAP connection. Pipelines should nonetheless be designed as if a live, recurring extract were being received (metadata-driven, idempotent, re-runnable).
- Group Reporting Currency is assumed to be USD; change this assumption only after confirming with the (simulated) Finance stakeholder.
- Rate type 'M' is assumed for all standard reporting; other rate types in TCURR (e.g. bank buying/selling rates) are out of scope.
- Not every SAP table referenced may be physically present in the Kaggle extract with every field listed — students must profile the actual files first and flag gaps back to the BA (instructor) rather than inventing data.

## 11. Glossary

| Term | Meaning |
|---|---|
| OTC | Order-to-Cash: the Sales Order → Delivery → Billing process |
| VKORG / VTWEG / SPART | Sales Organization / Distribution Channel / Division — together called the "Sales Area" |
| WAERK | Document currency key on a sales/billing header |
| NETWR | Net value (monetary amount) field, appears on order/delivery/billing items |
| KURST | Exchange rate type in SAP (e.g. 'M' = standard average rate) |
| SCD Type 2 | Slowly Changing Dimension pattern that preserves history by adding new rows for changed attributes, with effective/expiry dates and a current-row flag |
