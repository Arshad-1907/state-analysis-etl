**State Housing and Income Analysis: ETL, Automation & Visualization**

**Overview**

This project replicates and extends the "Analysis of States" Google Sheet using a robust Python ETL pipeline and data visualization. The goal is to automate the production of the OUTPUT tab, generate insights, and demonstrate scalable data engineering and analytics skills.
Data sources: Provided CSVs and Excel files (see below)
Output: Automated, reproducible CSV and SQLite database matching the OUTPUT tab, plus additional analysis and visualizations.

**Data Sources**

CENSUS_MHI_STATE.csv: Median household income by state
CENSUS_POPULATION_STATE.csv: Population by state
REDFIN_MEDIAN_SALE_PRICE.csv: Median home sale price by state and month
KEYS.csv: Region/state mapping and metadata
Analysis of States.xlsx: Reference Google Sheet and additional demographic data

ETL Pipeline

1. Extract

Reads all raw data files (CSV and Excel)
Cleans and normalizes columns for all 52 regions (states, DC, Puerto Rico)
Handles special cases (e.g., missing Puerto Rico sale price)
Outputs staged CSVs for population, income, and sale price

2. Transform

Merges staged data on region
Calculates:
Population, income, and sale price ranks
House affordability ratio (sale price / income)
Natural language blurbs for each metric (matching OUTPUT tab)
Produces output.csv matching the Google Sheet OUTPUT tab

3. Load

Loads the final output into a SQLite database (`state_analysis.db`).
Table: `state_analysis` (all processed state metrics and blurbs).
**Production Note:**  
The load script can be easily adapted to load data into PostgreSQL or MySQL by changing the SQLAlchemy connection string. This makes the pipeline scalable and ready for enterprise environments.

**ETL Automation with Apache Airflow**

To ensure the pipeline runs reliably and without manual intervention, the entire ETL process is automated using Apache Airflow:
The ETL steps (extract, transform, load) are defined as tasks in an Airflow Directed Acyclic Graph (DAG) (etl_pipeline.py).
Scheduling: The DAG is configured to run automatically at regular intervals (e.g., monthly or daily), so the pipeline always processes the latest available data.
Task orchestration: Airflow manages dependencies, ensuring each step runs in sequence and only when previous steps succeed.
Monitoring: Airflow’s web UI provides real-time visibility into pipeline runs, logs, and task statuses.
Extensibility: The workflow can be easily extended with sensors (for new data), notifications, or integration with production databases like PostgreSQL/MySQL

**Additional Analysis & Visualizations**

To demonstrate further skills, the project includes a Jupyter notebook with:
Descriptive statistics for all key metrics
Correlation heatmaps (population, income, sale price, affordability)
Histograms for household income and sale price distributions
Barplots for top/bottom states by affordability and income
Scatterplots (income vs. sale price)
Time series line plots for Redfin median sale price trends by state
Boxplots for affordability ratio (outlier detection)
Equity analysis: Median income by race (from race/ethnicity tab)
All code and visualizations are available in analysis_and_visualization.ipynb.


This project automates the extraction, transformation, and loading (ETL) of state-level housing and income data using Python and Airflow. 
Raw data from multiple sources—including Census population, household income, and Redfin median sale prices—is ingested, cleaned, merged, and enriched 
to produce a comprehensive output matching the "Analysis of States" Google Sheet. The pipeline calculates key metrics such as population, income, sale price ranks,
and affordability ratios, and generates natural-language blurbs for each region. Results are saved as both CSV and SQLite database files, and the workflow is fully automated 
and scheduled using Airflow to ensure reliability and reproducibility. An accompanying Jupyter notebook provides additional analysis and visualizations, offering deeper 
insights into trends and disparities across states.

