# IT-Incident-Log-Analysis

Analysis of IT incident logs generated by the ServiceNow™ platform used by an IT company. This project includes data extraction, transformation, and visualization using Azure Data Factory and Azure Databricks.

---
## Dashboard  
![Dashboard](screenshots/dashboard.png)  
---
## Tools Used
- Orchestraction:
    - Azure Data Factory
- Azure Data Bricks
- Pyspark
- Spark SQL
- Azure CosmosDB
- Azure Storage Gen2
- Azure SQL 

---
## Steps

1. **Data Extraction** from multiple sources.  
2. **Loading Data** into an Azure Cosmos Database.  
3. **Data Cleaning and Transformations** using PySpark in Azure Databricks.  
4. **Data Analysis** using Spark SQL and Databricks. 

---

## ADF Pipeline  
![Pipeline](screenshots/pipeline.png)  

### Extraction Layer  
**Sources:**  
- Blob files in Azure Storage Gen2  
- HTTP source from GitHub  
- Relational data from Azure SQL Database  

### Transformation Layer  
**Transformations performed using Azure Databricks with PySpark:**  
- **Data Cleaning and Reformatting:**  
  - Removed columns with more than 30% null values (invalid for analysis).  
  - Removed duplicate rows.  
  - Reformatted datetime data into an appropriate format.  
  - Renamed columns for better understanding.  
  - Extracted month and year information from datetime fields.  

### Visualization Layer  
- Visualizations performed using Azure SQL in Databricks.  
- Analyses focused on **SLA adherence** and **MTTR**. 

---





