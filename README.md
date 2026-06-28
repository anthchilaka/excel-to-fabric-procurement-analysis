# 📦 Procurement Inventory Intelligence | Microsoft Fabric | FMCG & Supply Chain

---

## 📋 Executive Summary

A 72-site global MRO [Maintenance, Repair and Operations] operation was managing £57.77M of inventory across 29 currencies with no unified view — stock-outs running at 76.8%, 71.7% of item-site combinations classified as non-moving, and inter-site transfer opportunities invisible to the business. I built an end-to-end analytics solution in Microsoft Fabric that ingests 8 SAP [Systems, Applications & Products] source files, transforms 803K+ rows through a medallion pipeline, and delivers a live two-page procurement dashboard — Overview KPIs plus an Opportunity Table that cross-filters slow/idle stock against inter-site transfer candidates in real time.

**Next steps:** build a savings realisation model quantifying £ value of transfer opportunities; surface the top 10 excess stock items by GBP value as a prioritised action list for the procurement team.

---

## 🔍 Business Problem

Procurement teams managing large, multi-site MRO [Maintenance, Repair and Operations] inventories face a structural problem: the data exists in the ERP, but it arrives as disconnected exports that nobody has time to cross-reference. This team's eight SAP files covered movements, stock positions, item master records, site data, and FX rates — but they were never combined. Slow and non-moving stock was accumulating undetected across 72 sites. Stock-outs were running near 77%. Items sitting idle at one site while urgently needed at another were invisible. And with operations spanning 29 currencies, no one had a reliable GBP-normalised view of total stock exposure.

The procurement leadership team needed a single platform that could surface what's moving, what's idle, and where the transfer opportunities are — with the right people seeing only the sites they're responsible for.

---

## 🧪 Methodology

End-to-end Microsoft Fabric migration — raw SAP exports through a medallion pipeline (bronze ingestion → silver cleaning → gold modelling) into a governed procurement dashboard. Inventory movement analysis used a 36-month rolling window to classify each item-site combination across five movement bands (Fast, Medium, Slow, No Mover, Non-moving). FX normalisation converted stock values across 29 currencies into GBP using the SAP CX rate convention. A cross-filter Opportunity Table links 'Stock to Move Out' (Slow/Non-moving items) to 'Transfer Candidates' (Fast/Medium demand for the same item at other sites) — enabling procurement teams to act on transfer opportunities directly from the report.

---

## 🛠️ Skills Demonstrated

**Microsoft Fabric**
- Lakehouse medallion pipeline (OneLake/Delta) — bronze, silver, gold layers
- PySpark notebooks — data cleaning, FX normalisation, movement classification, physical column pre-computation
- Direct Lake semantic model — sub-1.2s query times, no refresh lag
- Row-level security (RLS) — CEO (all 72 sites) + Site Manager (single-site view)
- Sensitivity labels via Microsoft Purview — Confidential classification across all report items
- Git integration — Fabric Source Control → GitHub

**DAX & Power BI**
- 8 KPI measures: Inventory Turnover, Stock-Out Rate, Inventory Accuracy, Items Flagged for Review, Excess Inventory Rate, Reorder Frequency, Currency Fluctuation Impact, Supplier Performance
- Movement Classification calculation group (5 bands)
- Field parameters for dynamic column/currency toggling

**Analytics**
- Inventory turnover analysis, stock-out rate, inventory accuracy, excess stock identification
- Multi-currency GBP normalisation (29 currencies, SAP CX rate convention)
- Cross-site inter-site transfer opportunity identification
- 36-month rolling movement window classification

---

## 📊 Results & Business Recommendations

The dashboard surfaced inventory health data that had never been visible to the procurement leadership team in a unified format:

| Finding | Value |
|---|---|
| Total GBP-normalised stock exposure | £57.77M |
| Stock-Out Rate | 76.8% |
| Inventory Accuracy | 16.4% |
| Non-moving item-site combinations | 71.7% |
| Items with active inventory flag | 108,497 |
| Inventory Turnover | 64× |

**Recommendation 1 — Use the Opportunity Table as a weekly transfer review.** With 71.7% of item-site combinations non-moving, the highest-value immediate action is cross-referencing idle items against Fast/Medium demand at other sites. A weekly review cadence using the cross-filter table can systematically eliminate transfer candidates before any write-off decision is made.

**Recommendation 2 — Investigate the 76.8% stock-out rate by site.** The bar chart (Inventory Turnover by Site, top 20) already shows significant site-level variance — GKSP1 turns at 755× vs the portfolio average of 64×. The sites with the lowest turnover and highest stock-out rate represent the priority intervention targets.

**Recommendation 3 — Fix the FX rate maintenance process at source.** 11% of stock rows had no GBP conversion rate in the SAP export, creating a £12.46M blind spot in the stock value figure. This is a data ownership issue — treasury/finance should maintain the SAP CX table rather than leaving gaps that the analytics layer has to patch.

---

## 🔭 Next Steps

- **Phase 2 — DIO [Days Inventory Outstanding] metric:** pre-compute a daily stock snapshot joined to movements in a single gold table (gold_fact_dio), enabling Days Inventory Outstanding without cross-table Direct Lake limitations
- **Savings quantification:** build a transfer value model to put a £ number on the inter-site transfer opportunity surfaced by the Opportunity Table — the business case for acting on the 71.7% non-moving classification
- **Movement status recompute:** the 36-month recompute uses the MM.csv classification as an interim source; once the stakeholder provides the calculated_maximum formula, the full recompute can be enabled in one notebook re-run
- **Data limitation:** 11.6% of movement rows have unparsed posting dates from the SAP [Systems, Applications & Products] export — excluded from window calculations with a diagnostic cell surfacing affected item-site pairs. Root fix is upstream in the ERP export configuration.

---
---
