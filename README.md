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

---

## 🔧 Environment Setup (AWS + Airflow on EC2)

### ✅ AWS Infrastructure

- **EC2 Instance:** `t2.medium` or `t3.medium` (Ubuntu 22.04)
- **RDS PostgreSQL Instance:** `db.t3.micro`
- **Redshift:** Serverless (Free Tier)

---

### 🔓 Security Groups

Allow the following ports in the EC2 **inbound rules**:
- **8080** → Airflow Web UI
- **5432** → PostgreSQL (from EC2)

---

### 🖥 SSH Configuration for VS Code

1. Open VS Code → Command Palette → `Remote-SSH: Configure SSH Hosts`
2. Add the following to your SSH config:

   ```ssh
   Host ec2_instance_name
       HostName <EC2_PUBLIC_IP>
       User ubuntu
       IdentityFile <path_to_downloaded_key.pem>

---

## 🔧 Local Setup on EC2 (Ubuntu 22.04)

Follow these steps to configure the EC2 environment and run Apache Airflow with all required dependencies.

---

### 📦 Step 1: System Setup & Python Environment

```
sudo apt update
sudo apt install -y python3.10 python3.10-venv
python3.10 -m venv airflow_venv
source airflow_venv/bin/activate
pip install "apache-airflow==2.8.2" \
            "apache-airflow-providers-amazon==8.18.0" \
            "apache-airflow-providers-postgres==5.10.1" \
            --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.2/constraints-3.10.txt"
pip install pandas requests
```


---


### 🐘 Install PostgreSQL Locally (If Needed)

If you plan to run or test PostgreSQL commands locally on your EC2 instance or development environment, you'll need to install the PostgreSQL client and server tools manually. Follow the official instructions for Ubuntu:

🔗 **PostgreSQL Installation Guide (Ubuntu)**  
https://www.postgresql.org/download/linux/ubuntu/

This step is optional if you're only using Amazon RDS for PostgreSQL and connecting remotely.

---

### ☁️ Importing Static Data from S3 to Amazon RDS

To import static city metadata from an S3 bucket into your PostgreSQL database hosted on Amazon RDS, follow the official AWS documentation:

🔗 **Amazon RDS for PostgreSQL – Import from S3**  
https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_PostgreSQL.S3Import.html

Make sure:
- Your S3 bucket and RDS instance are in the same region.
- The RDS instance has a role attached with proper S3 read permissions.
- The `aws_s3.table_import_from_s3` extension is enabled in your RDS PostgreSQL database.

---

### 🔐 Credentials and API Keys

All AWS credentials, IAM roles, and API keys used in the sample code have been **revoked and disabled**.  
To run this project in your own environment, you must configure your own:

- OpenWeatherMap API Key (via Airflow HTTP connection)
- AWS Access Key/Secret (via Airflow AWS connection or instance role)
- PostgreSQL and Redshift connection credentials (via Airflow Postgres connections)

---
