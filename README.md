# 🚀 End-to-End ETL Pipeline in Azure Databricks with Autoloader & Delta Live Tables (DLT)

This project showcases a complete ETL pipeline using **Azure Databricks**, **Autoloader**, **PySpark**, and **Delta Live Tables (DLT)** to build a Medallion Architecture pipeline: Bronze → Silver → Gold. It includes transformation logic, data validation, and workflow orchestration.

---

## 🏗️ Project Architecture

![Pipeline Screenshot](pipeline/Screenshot%202025-06-06%20190221.png)


![Pipeline Screenshot](pipeline/Screenshot%202025-06-07%20194737.png)

> 📝 *Note: Due to Azure free tier cluster quota limits, the full ETL pipeline execution was interrupted. The screenshot shows the initial stages of the pipeline configuration and logic setup.*


---

## 🔁 ETL Pipeline Flow

1. **Bronze Layer – Raw Ingestion**
   - Ingest data using **Autoloader** from source directories.
   - File: 1_Autoloader.py

2. **Silver Layer – Data Cleaning & Enrichment**
   - Handle schema evolution, nulls, and data casting.
   - Apply business logic and add metadata columns.
   - Files: 2_silver.py, 3_Lookup Notebook.py, 4_Silver_Transformations.py

3. **Gold Layer – Business Aggregations**
   - Build curated tables for reporting using Delta Live Tables.
   - File: 5_DLT_Gold_Layer.py

4. **Pipeline Workflow**
   - Screenshots provided in pipeline/ folder to show Databricks pipeline flow and notebook execution stages.

---

<pre> ## 📁 Project Structure  ``` ADBPROJECT2/ ├── notebooks/ │ ├── 1_Autoloader.py │ ├── 2_silver.py │ ├── 3_Lookup Notebook.py │ ├── 4_Silver_Transformations.py │ └── 5_DLT_Gold_Layer.py ├── pipeline/ │ ├── Screenshot 2025-06-06 190221.png │ ├── Screenshot 2025-06-07 152213.png │ └── Screenshot 2025-06-07 194737.png └── README.md ``` </pre>


---

## 🧱 Technologies Used

- **Azure Databricks (Free/Community Edition)**
- **PySpark (DataFrame API)**
- **Delta Lake & Delta Live Tables**
- **Autoloader**
- **Unity Catalog** (initially enabled)

---

## 📊 Output Layers Summary

| Layer   | Description                                           |
|---------|-------------------------------------------------------|
| Bronze  | Raw ingested data using Autoloader and streaming API |
| Silver  | Cleaned & enriched data, ready for analytics          |
| Gold    | Final curated tables using DLT and business rules     |

---

## 🚀 How to Run the Project

1. **Clone the Repo:**
   ```bash
   git clone https://github.com/Pranith-cloud/ADBPROJECT2.git
   cd ADBPROJECT2

Upload Notebooks to Databricks Workspace

Run Notebooks in This Order:

1_Autoloader.py

2_silver.py

3_Lookup Notebook.py

4_Silver_Transformations.py

5_DLT_Gold_Layer.py

Refer to Screenshots in pipeline/ for Visual Workflow

Copy
Edit
.

## 🧑‍💻 Author

**Pranith Dongari**  
🎓 Master’s Student, Florida Atlantic University (FAU)  
🔗 [GitHub](https://github.com/Pranith-cloud)  
💼 Passionate about building scalable data pipelines  


# 📌 Notes

This project was developed using Unity Catalog in a Free Azure trial (East US region).

Databricks Community Edition was used for testing and experimentation.

 # 🧠 Future Enhancements

- Add orchestration using **Azure Data Factory** or **Databricks Workflows**
- Implement **parameterization** and **modularization** of notebooks
- Add **data quality checks** and **alerting**
- Push curated Gold data to **Power BI** or **Looker** for reporting
- Implement **unit tests** for critical transformation logic
- Configure **CI/CD pipelines** for notebook deployment (via Azure DevOps or GitHub Actions)
- Run full pipeline execution on a paid Azure subscription to validate performance and output end-to-end
does this look good like above
