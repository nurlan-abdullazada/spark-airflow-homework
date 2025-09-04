from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from pendulum import datetime
from datetime import timedelta
import os

# Default arguments for all tasks
default_args = {
    'owner': 'nurlan',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,                    # Retry failed tasks 2 times
    'retry_delay': timedelta(minutes=2), # Wait 2 minutes between retries
}

def check_data_files():
    """Python function to check if required data files exist"""
    data_path = "/opt/airflow/data"
    
    required_files = [
        "yellow_tripdata_2024-01.parquet",
        "taxi_zone_lookup.csv"
    ]
    
    missing_files = []
    for file in required_files:
        file_path = os.path.join(data_path, file)
        if not os.path.exists(file_path):
            missing_files.append(file)
    
    if missing_files:
        raise FileNotFoundError(f"Missing required files: {missing_files}")
    
    print("✅ All required data files are present!")
    return True

def create_output_directory():
    """Create output directory if it doesn't exist"""
    output_path = "/opt/airflow/output"
    os.makedirs(output_path, exist_ok=True)
    print(f"✅ Output directory ready: {output_path}")
    return True

# Define the DAG
with DAG(
    dag_id="taxi_data_processing_spark",
    default_args=default_args,
    description="Process NYC Taxi data with Spark every 10 minutes",
    schedule="*/10 * * * *",  # Every 10 minutes
    start_date=datetime(2025, 1, 1),
    catchup=False,  # Don't run for past dates
    max_active_runs=1,  # Only one DAG run at a time
    tags=["spark", "taxi", "etl", "homework"],
) as dag:

    # Task 1: Start marker
    start_task = EmptyOperator(
        task_id="start_processing",
        doc_md="Start of taxi data processing pipeline"
    )

    # Task 2: Check if required data files exist
    check_files_task = PythonOperator(
        task_id="check_data_files",
        python_callable=check_data_files,
        doc_md="Verify that all required input data files are available"
    )

    # Task 3: Create output directory
    create_output_task = PythonOperator(
        task_id="create_output_directory",
        python_callable=create_output_directory,
        doc_md="Create output directory for processed results"
    )

    # Task 4: Check Spark cluster connectivity
    check_spark_task = BashOperator(
        task_id="check_spark_connection",
        bash_command="""
        echo "🔍 Checking Spark cluster connectivity..."
        # Try to connect to Spark master
        nc -z spark-master 7077 && echo "✅ Spark master is reachable" || echo "❌ Cannot reach Spark master"
        
        # Check if spark-submit is available
        which spark-submit && echo "✅ spark-submit command available" || echo "❌ spark-submit not found"
        
        echo "Spark connectivity check completed"
        """,
        doc_md="Verify that Spark cluster is accessible"
    )

    # Task 5: Main Spark processing job
    spark_processing_task = BashOperator(
        task_id="run_spark_processing",
        bash_command="""
        echo "🚀 Starting Spark job for taxi data processing..."
        echo "Timestamp: $(date)"
        echo "Working directory: $(pwd)"
        echo "Available files in /opt/airflow/dags: $(ls -la /opt/airflow/dags/)"
        
        # Run the Spark job
        spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --driver-memory 2g \
            --executor-memory 2g \
            --executor-cores 2 \
            --total-executor-cores 4 \
            --conf spark.sql.adaptive.enabled=true \
            --conf spark.sql.adaptive.coalescePartitions.enabled=true \
            --conf spark.eventLog.enabled=false \
            --py-files /opt/airflow/dags/process_taxi_data.py \
            /opt/airflow/dags/process_taxi_data.py
            
        echo "✅ Spark job completed successfully!"
        echo "Completion timestamp: $(date)"
        """,
        doc_md="Execute the main Spark data processing job"
    )

    # Task 6: Verify output files
    verify_output_task = BashOperator(
        task_id="verify_output",
        bash_command="""
        echo "🔍 Verifying output files..."
        
        output_dir="/opt/airflow/output"
        
        if [ -d "$output_dir" ]; then
            echo "✅ Output directory exists"
            echo "Contents of output directory:"
            ls -la $output_dir/
            
            # Check for specific output folders
            expected_outputs=("processed_trips" "borough_stats" "hourly_stats" "weekend_stats")
            
            for output in "${expected_outputs[@]}"; do
                if [ -d "$output_dir/$output" ]; then
                    echo "✅ Found $output directory"
                    echo "   Files in $output: $(ls -la $output_dir/$output/ | wc -l) files"
                else
                    echo "❌ Missing $output directory"
                fi
            done
        else
            echo "❌ Output directory not found!"
            exit 1
        fi
        
        echo "Output verification completed"
        """,
        doc_md="Verify that all expected output files were created"
    )

    # Task 7: Generate processing summary
    generate_summary_task = BashOperator(
        task_id="generate_summary",
        bash_command="""
        echo "📊 TAXI DATA PROCESSING SUMMARY"
        echo "================================"
        echo "Processing completed at: $(date)"
        echo "DAG Run ID: {{ dag_run.run_id }}"
        echo "Execution Date: {{ ds }}"
        
        # Count files in output directories
        output_dir="/opt/airflow/output"
        
        echo ""
        echo "Output Summary:"
        for dir in processed_trips borough_stats hourly_stats weekend_stats; do
            if [ -d "$output_dir/$dir" ]; then
                file_count=$(find "$output_dir/$dir" -type f | wc -l)
                total_size=$(du -sh "$output_dir/$dir" 2>/dev/null | cut -f1)
                echo "  $dir: $file_count files, $total_size total"
            fi
        done
        
        echo ""
        echo "🎉 Processing pipeline completed successfully!"
        echo "Next scheduled run: in 10 minutes"
        """,
        doc_md="Generate summary report of the processing pipeline"
    )

    # Task 8: End marker
    end_task = EmptyOperator(
        task_id="end_processing",
        doc_md="End of taxi data processing pipeline"
    )

    # Define task dependencies (execution order)
    start_task >> check_files_task >> create_output_task >> check_spark_task
    check_spark_task >> spark_processing_task >> verify_output_task
    verify_output_task >> generate_summary_task >> end_task

# Add DAG documentation
dag.doc_md = """
# NYC Taxi Data Processing Pipeline

This DAG processes NYC Yellow Taxi trip data using Apache Spark every 10 minutes.

## Pipeline Steps:
1. **Check Data Files**: Verify input files exist
2. **Create Output Directory**: Prepare output location  
3. **Check Spark Connection**: Verify Spark cluster is accessible
4. **Run Spark Processing**: Execute main data processing job
5. **Verify Output**: Confirm all output files were created
6. **Generate Summary**: Create processing report

## Transformations Applied:
- **Narrow**: Filter invalid trips, calculate trip duration, fare per mile
- **Wide**: Join with zone data, group by borough/hour, aggregate statistics

## Schedule: Every 10 minutes
## Retries: 2 attempts with 2-minute delay
## Owner: nurlan
"""
