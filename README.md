# ☁️ Climate Data Warehouse Project using Apache Airflow

This project builds a scalable data pipeline using **Apache Airflow** to create a **climate data warehouse** for four major Indian cities: **Mumbai**, **Delhi**, **Chennai**, and **Kolkata**. The pipeline integrates static city metadata with dynamic weather data from the **OpenWeatherMap API**, stages it in **PostgreSQL**, stores hourly snapshots in **Amazon S3**, and loads daily aggregates into **Amazon Redshift**.

---

## 🎯 Project Objectives

- Build an automated, hourly ETL pipeline to collect real-time weather data
- Enrich weather data with static city metadata
- Store the joined data as timestamped CSVs in S3 for historical tracking
- Load daily data into a centralized data warehouse on Amazon Redshift
- Apply best practices such as temp tables, table optimization, and validation

---

## 🧱 Technology Stack

| Component     | Tool/Service         |
|---------------|----------------------|
| Workflow Orchestration | Apache Airflow |
| Weather Data Source    | OpenWeatherMap API |
| Relational DB (Staging) | PostgreSQL |
| Cloud Storage           | Amazon S3 |
| Data Warehouse          | Amazon Redshift |
| Cloud Platform          | AWS (EC2, S3, Redshift) |
| Programming Language    | Python (requests, pandas, psycopg2) |

---

## 📌 DAG Overview

### 🔁 `weather_dag` (Runs **Hourly**)

This DAG performs the following steps every hour:

1. **Setup Tables (PostgreSQL)**  
   - Creates/Truncates `cities` and `weather_data` tables
   - Loads static city metadata from S3 into `cities` table

2. **API Check**  
   - Validates if the OpenWeatherMap API is available

3. **ETL in Parallel** for 4 cities  
   - Extracts current weather using OpenWeather API
   - Transforms data to Celsius and structures it as a DataFrame
   - Loads transformed data into the `weather_data` table

4. **Join Data**  
   - Joins `weather_data` with `cities` table to enrich the weather with city attributes

5. **Upload to S3**  
   - Saves joined data as CSV in S3 in the format:  
     `s3://open-weather-data-vedant/weather_data_YYYY-MM-DD/hour_HH.csv`

---

### 📦 `daily_redshift_load` (Runs **Daily at 11:30 PM IST**)

This DAG performs the following:

1. **Create Staging Table** (`temp_weather_data`)
2. **Load Daily Data from S3** using Redshift `COPY`
3. **Validate Load** (optional)
4. **Insert into Main Table** (`weather_data`)
5. **Drop Temp Table**
6. **Run ANALYZE** for Redshift query optimization

---

## 🔐 Required Airflow Connections

Make sure the following Airflow connections are configured:

- `postgres_conn` → PostgreSQL connection (staging DB)
- `aws_default` → AWS credentials for S3 access
- `redshift_conn` → Amazon Redshift connection
- `weather_api` → HTTP connection to OpenWeatherMap API
