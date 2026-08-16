# Spark Data Warehouse Project (Scala)

Welcome to the Data Warehouse and Analytics Project repository! 🚀

This project demonstrates a comprehensive data warehousing and analytics solution built with **Apache Spark and Scala**, using **PostgreSQL** as the data warehouse. It covers the complete data engineering lifecycle, from ingesting raw source data to building a clean, business-ready analytical model.

Designed as a portfolio project, it highlights practical data engineering principles, scalable transformations, data quality validation, and dimensional modeling using the **Medallion Architecture**.

---

## 📖 Project Overview

This project involves:

1. **Data Architecture:** Designing a modern data warehouse using the **Medallion Architecture** with **Bronze**, **Silver**, and **Gold** layers.
2. **ETL Pipelines:** Extracting data from CSV source systems, transforming it using **Apache Spark with Scala**, and loading it into **PostgreSQL**.
3. **Data Quality:** Implementing validation and quality checks across the Silver and Gold layers.
4. **Data Modeling:** Developing fact and dimension tables optimized for analytical queries using a **Star Schema**.
5. **Analytics & Reporting:** Providing SQL-based analytical datasets that can be consumed by reporting and visualization tools.
6. **Reusable Architecture:** Separating data access, transformations, and quality checks into reusable Scala components.

---

## 🛠️ Tools & Technologies

Everything used in this project is free and open source! 🚀

* **[Apache Spark](https://spark.apache.org/):** Distributed data processing and transformation engine.
* **[Scala](https://www.scala-lang.org/):** Programming language used to develop the ETL and data transformation pipelines.
* **[PostgreSQL](https://www.postgresql.org/):** Relational database used to store the Bronze, Silver, and Gold warehouse layers.
* **[JDBC](https://jdbc.postgresql.org/):** Used by Spark to read from and write data to PostgreSQL.
* **[Draw.io](https://app.diagrams.net/):** Used to create architecture diagrams, data models, ETL flows, and process diagrams.
* **[Notion](https://www.notion.so/):** Used to organize project documentation, tasks, and technical notes.

---

## 🚀 Project Requirements

## Building the Data Warehouse (Data Engineering)

### 🎯 Objective

Develop a modern data warehouse using **Apache Spark, Scala, and PostgreSQL** to consolidate sales data from CRM and ERP source systems, enabling analytical reporting and business insights. 

The project focuses on:

* Building a complete end-to-end data pipeline.
* Separating raw, cleansed, and business-ready data.
* Applying scalable transformations with Spark.
* Implementing reusable Scala components.
* Validating data quality throughout the pipeline.
* Creating a dimensional model suitable for analytics.
* Following practical data engineering and software engineering principles.

### 📋 Specifications

* **Data Sources:** Import data from two source systems (**ERP** and **CRM**) provided as CSV files.
* **Data Ingestion:** Load raw source data into the PostgreSQL Bronze layer using Spark and Scala.
* **Data Quality:** Identify, cleanse, standardize, and resolve data quality issues during the Silver transformation process.
* **Integration:** Combine data from CRM and ERP systems into a unified analytical model.
* **Transformation:** Use Spark DataFrame APIs and Spark SQL to perform data cleansing, joins, standardization, and business transformations.
* **Data Modeling:** Build business-ready fact and dimension tables following a **Star Schema**.
* **Scope:** Focus on the latest dataset only; historization of data is not required.
* **Data Validation:** Implement automated quality checks for the Silver and Gold layers.
* **Documentation:** Provide clear documentation of the data architecture, transformations, data model, and quality checks.

---

## 🏗️ Project Architecture

The data architecture follows the **Medallion Architecture**, with each layer having a clearly defined responsibility:

![Architecture](docs/data_architecture.png)

| Layer         | Description                                                                                                                                                                                             |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 🥉 **Bronze** | Stores raw data as-is from the source systems. CSV files from the CRM and ERP systems are ingested into PostgreSQL using Spark and Scala.                                                               |
| 🥈 **Silver** | Contains cleansed, standardized, and integrated data. Spark transformations are used to handle data quality issues, normalize fields, standardize values, and prepare the data for analytical modeling. |
| 🥇 **Gold**   | Contains business-ready fact and dimension tables modeled using a Star Schema. These tables are optimized for analytical queries and reporting.                                                         |

---

## 🔄 Data Pipeline

The ETL pipeline follows the flow below:

![Flow](docs/data_flow.png)

The pipeline is implemented using **Apache Spark with Scala**, while PostgreSQL is used as the persistent storage layer.

Spark communicates with PostgreSQL through JDBC for reading and writing warehouse tables.

---

## 📁 Repository Structure


---

## Compile and Run:
    sbt clean assembly

    spark-submit --class WareHouseApp \
        target/scala-2.12/spark-warehouse-assembly-1.0-SNAPSHOT.jar

----

## 📊 Skills Demonstrated

* Data Warehousing
* Data Engineering
* Apache Spark
* Scala
* PostgreSQL
* JDBC
* ETL Development
* Data Modeling (Star Schema)
* SQL Development
* Data Cleaning & Transformation
* Analytical Reporting
* Database Design
* Documentation

---

## 🛡️ License

This project is licensed under the [MIT License](LICENSE). You are free to use, modify, and share this project with proper attribution.






