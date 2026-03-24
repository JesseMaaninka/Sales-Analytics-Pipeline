# Sales Analytics Pipeline

## Setup
 
**Requirements**
- Python 3.11
- Java 17 (Eclipse Temurin)
- Redis running in WSL
- Virtual environment
 
**Install dependencies**
```bash
pip install redis pyspark scikit-learn mlflow streamlit pandas matplotlib
```
 
**Start Redis (in WSL)**
```bash
sudo service redis-server start
redis-cli ping   # should return PONG
```
 
---
 
## Run the pipeline (in order)
 
**1. Push sales data to Redis**
```bash
python producer.py
```
 
**2. ETL with Spark**
```powershell
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"
$env:Path += ";C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot\bin"
$env:PYSPARK_PYTHON = (Get-Command python).Source
$env:PYSPARK_DRIVER_PYTHON = (Get-Command python).Source
python spark_etl.py
```
 
**3. Train ML model**
```bash
python ml_model.py
```
 
**4. Launch dashboard**
```bash
streamlit run dashboard.py
```
Opens at: http://localhost:8501

## Project structure
 
| File | Description |
|------|-------------|
| `producer.py` | Simulates Kafka producer, pushes 200 orders to Redis |
| `spark_etl.py` | Reads from Redis, transforms with PySpark |
| `ml_model.py` | Trains linear regression, tracked with MLflow |
| `dashboard.py` | Streamlit dashboard with charts and ML predictions |
 
---
 
## Pipeline overview
 
```
producer.py  →  Redis (sales_stream)
                     ↓
             spark_etl.py (PySpark)
                     ↓
             Redis (sales_processed, sales_summary)
                     ↓
             ml_model.py (scikit-learn + MLflow)
                     ↓
             dashboard.py (Streamlit UI)
```