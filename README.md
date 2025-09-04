# 🚖 Spark + Airflow Project: NYC Taxi Data Processing

This project demonstrates a complete data pipeline using Apache Spark for data processing and Apache Airflow for workflow orchestration. The system processes NYC Yellow Taxi trip data with both narrow and wide transformations, running on a scheduled basis.

## 📋 Project Overview

- **Dataset**: NYC Yellow Taxi Trip Records (January 2024, ~180 MB)
- **Processing Engine**: Apache Spark 3.5.0
- **Orchestration**: Apache Airflow 3.0.1
- **Schedule**: Every 10 minutes
- **Architecture**: Docker containerized environment

## 🗂️ Project Structure

```
airflow_3.0.1/
├── config/
│   └── airflow.cfg              # Airflow configuration
├── dags/
│   ├── process_taxi_data.py     # Spark processing script
│   ├── taxi_processing_dag.py   # Airflow DAG definition
│   └── test.py                  # Simple test DAG
├── data/
│   ├── yellow_tripdata_2024-01.parquet  # NYC taxi data
│   └── taxi_zone_lookup.csv             # Zone reference data
├── logs/                        # Airflow execution logs
├── output/                      # Processed data output
├── docker-compose.yml           # Docker services configuration
├── requirements.txt             # Python dependencies
```

## 🔄 Data Transformations

### Narrow Transformations (No Shuffling)
- **Filter**: Remove invalid trips, outliers, and data quality issues
- **WithColumn**: Calculate derived metrics:
  - Trip duration in minutes
  - Fare per mile
  - Fare per minute
  - Pickup hour extraction
  - Weekend/weekday flag

### Wide Transformations (Require Shuffling)
- **Join**: Enrich trip data with pickup and dropoff zone information
- **GroupBy Aggregations**:
  - Borough-level statistics (total trips, average fare, total revenue)
  - Hourly trip patterns (trip count by hour of day)
  - Weekend vs weekday analysis

## 🚀 Getting Started

### Prerequisites
- Docker and Docker Compose
- At least 4GB RAM
- 10GB free disk space

### Setup Instructions

1. **Clone the repository**
   ```bash
   git clone https://github.com/nurlan-abdullazada/spark-airflow-taxi-processing.git
   cd spark-airflow-taxi-processing
   ```

2. **Create required directories**
   ```bash
   mkdir -p data output logs
   ```

3. **Download the dataset**
   ```bash
   cd data
   wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
   wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
   ```

4. **Create environment file**
   ```bash
   echo "AIRFLOW_UID=$(id -u)" > .env
   echo "_AIRFLOW_WWW_USER_USERNAME=admin" >> .env
   echo "_AIRFLOW_WWW_USER_PASSWORD=admin123" >> .env
   ```

5. **Start the services**
   ```bash
   docker network create spark-net
   docker-compose up -d
   ```

6. **Initialize Airflow database**
   ```bash
   docker-compose exec airflow-scheduler airflow db migrate
   ```

7. **Access Airflow Web UI**
   - URL: http://localhost:8086
   - Username: admin
   - Password: admin123

## 📊 Pipeline Workflow

The Airflow DAG (`taxi_data_processing_spark`) consists of 8 tasks:

1. **start_processing**: Pipeline initialization
2. **check_data_files**: Verify input data availability
3. **create_output_directory**: Prepare output location
4. **check_spark_connection**: Test Spark cluster connectivity
5. **run_spark_processing**: Execute main Spark job
6. **verify_output**: Confirm output file generation
7. **generate_summary**: Create processing report
8. **end_processing**: Pipeline completion marker

## 📈 Output Data

The pipeline generates the following processed datasets:

- **processed_trips/**: Cleaned and enriched trip data with calculated fields
- **borough_stats/**: Aggregated statistics by NYC borough
- **hourly_stats/**: Trip patterns by hour of day
- **weekend_stats/**: Weekend vs weekday comparison metrics

## 🔧 Configuration

### Airflow Settings
- **Schedule**: `*/10 * * * *` (every 10 minutes)
- **Retries**: 2 attempts with 2-minute delays
- **Parallelism**: 16 concurrent tasks
- **Executor**: LocalExecutor

### Spark Configuration
- **Master**: spark://spark-master:7077
- **Driver Memory**: 2GB
- **Executor Memory**: 2GB
- **Executor Cores**: 2
- **Total Cores**: 4

## 📋 Key Features

- **Automated Data Quality Checks**: Validates input files before processing
- **Error Handling**: Comprehensive retry logic and failure notifications
- **Health Monitoring**: Built-in connectivity and service health checks
- **Scalable Architecture**: Docker-based deployment for easy scaling
- **Comprehensive Logging**: Detailed execution logs for debugging

## 🎯 Learning Objectives Achieved

- ✅ **Data Pipeline Orchestration**: Implemented production-ready workflow with Airflow
- ✅ **Spark Transformations**: Applied both narrow and wide transformations
- ✅ **Data Quality**: Implemented data validation and cleaning processes
- ✅ **Scheduling**: Automated recurring data processing every 10 minutes
- ✅ **Monitoring**: Real-time pipeline monitoring and alerting
- ✅ **Containerization**: Docker-based deployment for reproducibility

## 📱 Monitoring and Maintenance

- Monitor DAG execution via Airflow Web UI
- Check logs for individual task failures
- Verify output data quality and completeness
- Scale resources based on data volume growth

## 🚀 Future Enhancements

- Add data visualization dashboard
- Implement real-time streaming processing
- Add data lineage tracking
- Extend to multiple data sources
- Add machine learning predictions

## 👨‍💻 Author

**Nurlan Abdullazada**
- GitHub: [@nurlan-abdullazada](https://github.com/nurlan-abdullazada)

## 📄 License

This project is created for educational purposes as part of a Big Data processing course assignment.

---

*This project demonstrates real-world data engineering practices using industry-standard tools and frameworks.*
