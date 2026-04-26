# 🎵 Incremental ETL and Dimensional Modeling Platform on Azure Lakehouse Architecture

An enterprise-grade data platform implementing a Medallion Architecture on Azure. This project demonstrates high-scale streaming logic, Change Data Capture (CDC), and professional DevOps workflows using Databricks Asset Bundles (DABs).

## 💡 The Problem & Solution
Most data pipelines fail at scale due to schema drift and messy historical tracking. I built this platform to handle incremental streaming data, ensuring high data quality with Delta Live Tables (DLT) and historical auditing via SCD Type 2 dimensions.

## 🏗️ Architecture & Design
<img width="1513" height="704" alt="Archi" src="https://github.com/user-attachments/assets/17dcb4b7-9549-48ea-8c2e-e058dd7fc74d" />

### Technical Components
- **Storage**: ADLS Gen2 (The Data Lakehouse foundation).
- **Orchestration**: Azure Data Factory for raw ingestion.
- **Processing Engine**: Azure Databricks (PySpark) & Spark Structured Streaming.
- **Framework**: Delta Live Tables (DLT) for declarative pipeline management.
- **DevOps**: Databricks Asset Bundles (DABs) for CI/CD ready deployments.
---

## 📊 Pipeline Lineage & Orchestration
**DLT Dependency Graph (DAG)**

<img width="984" height="515" alt="image" src="https://github.com/user-attachments/assets/95349963-5439-4374-9fbc-2b024c0eab6b" />

---

## ⚙️ Technical Skills & Tools

#### **Data Engineering & Orchestration**
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat-square&logo=databricks&logoColor=white) 
![Azure Data Factory](https://img.shields.io/badge/Azure_Data_Factory-0078D4?style=flat-square&logo=microsoft-azure&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat-square&logo=apache-spark&logoColor=white)
![Spark_Structured_Streaming](https://img.shields.io/badge/Spark_Streaming-E25A1C?style=flat-square&logo=apache-spark&logoColor=white)

#### **Infrastructure & DevOps**
![Azure ADLS Gen2](https://img.shields.io/badge/ADLS_Gen2-0078D4?style=flat-square&logo=microsoft-azure&logoColor=white)
![Databricks Asset Bundles](https://img.shields.io/badge/DABs-FF3621?style=flat-square&logo=databricks&logoColor=white)
![Git](https://img.shields.io/badge/Git-F05032?style=flat-square&logo=git&logoColor=white)
![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-FF3621?style=flat-square&logo=databricks&logoColor=white)

#### **Languages & Frameworks**
* **Languages:** Python (PySpark), SQL (Spark SQL, T-SQL), Jinja2 (Dynamic SQL)
* **Architecture:** Medallion Architecture (Bronze, Silver, Gold), Star Schema Modeling
* **CDC Patterns:** SCD Type 2 Implementation, Delta Live Tables (DLT)
* **Data Quality:** DLT Expectations, Schema Enforcement

#### **Analytics & Visualization**
* **Power BI:** DAX, Data Modeling, Integrated Lakehouse Reporting

--- 
#### **💎Data Quality Guardrails**: Implemented DLT Expectations to automatically drop records that fail business logic.
```bash
Python
@dlt.expect_or_drop("valid_user", "user_id IS NOT NULL")
```
---
---
## 🛰️ Ingestion & Monitoring (ADF + Logic Apps)
To ensure the Lakehouse is always up-to-date with the latest streaming activity, I implemented a robust ingestion layer.

* **Incremental Ingestion**: Developed an ADF pipeline using a `Lookup` and `ForEach` activity pattern to identify and pull only new records from source systems, reducing processing costs.
* **Automated Alerting**: Integrated **Azure Logic Apps** to trigger real-time Web alerts. If any stage of the pipeline fails, a notification is sent immediately, ensuring high availability.
* **Metadata-Driven**: Used a watermark-based logic to track the `last_cdc` state, ensuring zero data loss during scheduled runs.

**ADF Pipeline Logic**

<img width="852" height="357" alt="ADF" src="https://github.com/user-attachments/assets/9660ef3a-f7c5-45f4-a535-bc11dec61704" />

## 🔄 Metadata-Driven Incremental Ingestion (ADF)
To optimize costs and performance, I built a watermark-based ingestion framework in Azure Data Factory. This ensures only new or updated records are processed during each cycle.

* **Watermark Logic**: The `last_cdc` lookup identifies the last processed timestamp. If new data exists (`ifIncrementalData` = True), the pipeline triggers a `Copy Data` activity and subsequently updates the control table with the new `max_cdc` value.
* **Batch Orchestration**: I implemented an `incremental_loop` pipeline utilizing a `ForEach` activity. This allows the same logic to scale across multiple source entities (Users, Tracks, Streams) dynamically by passing table metadata as parameters.
* **Fault Tolerance**: Includes a `DeleteEmptyFile` cleanup task to ensure the Bronze layer remains clean if a source pull results in an empty dataset.

**Incremental Logic Flow**
<img width="1427" height="563" alt="incremental_loop inside" src="https://github.com/user-attachments/assets/04dc4f7f-3466-4e27-98df-a6106192111d" />

---

## 🧬 Change Data Capture (CDC) Deep Dive
Implementing **Slowly Changing Dimensions (SCD) Type 2** was critical for maintaining the analytical integrity of this Spotify dataset. 

* **The Scenario:** If a user upgrades their subscription from "Free" to "Premium," a standard overwrite would lose the historical context. By using SCD Type 2, we can accurately attribute historical streams to the "Free" tier while tracking new activity under "Premium."
* **The Implementation:** I utilized the `dlt.apply_changes()` API to handle the complex merge logic. This automatically manages row versioning (`is_current`, `start_date`, `end_date`), ensuring the **Gold Layer** remains a reliable source of truth for time-series analysis.

## 🚀 Deployment Workflow (CI/CD & DABs)
To bridge the gap between Data Engineering and DevOps, this project is fully containerized for the Databricks environment using **Databricks Asset Bundles (DABs)**. This ensures that the pipeline is reproducible, version-controlled, and ready for production deployment.

**Key Commands used for Orchestration:**
```bash
# Validate the bundle configuration and pathing
databricks bundle validate

# Deploy the pipeline and resources to the development workspace
databricks bundle deploy --target dev

# Manually trigger a run of the deployed pipeline
databricks bundle run gold_pipeline
```

## 🛠️ Data Modeling (The "Gold" Layer)
I implemented a Star Schema to optimize query performance for BI tools like Power BI:
  - FactStream: Grain at the individual stream level, capturing listening duration and timestamps.
  - DimUser / DimTrack: Implemented as SCD Type 2, allowing the business to track how user preferences or track metadata change over time without losing history.
  - DimDate: A custom-generated calendar dimension for advanced time-series analysis.

## 🌟 Professional Highlights
  - **Declarative Pipelines**: Instead of messy notebook chains, I used DLT to define tables, which automatically handles data dependencies and quality constraints.
  - **CI/CD Ready**: Used databricks.yml and project bundling to treat data pipelines like software, allowing for easy environment promotion (Dev -> Prod).
  - **Advanced SQL logic**: Integrated Jinja2 Templating within Databricks to generate dynamic SQL joins, reducing code redundancy by 40%.
  - **Data Quality Guardrails**: Implemented DLT Expectations to automatically drop or quarantine records that fail business logic (e.g., user_id IS NOT NULL).

