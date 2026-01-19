import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

import os
import sys
sys.path.append('/opt/airflow')

from src.core.connectors import (
    S3Connector, DuckDBConnector, PostgresConnector, ConnectionConfig
)
from src.core.data_layers import RawDataLayer, StagingLayer, DataMartLayer
from src.pipeline.etl_processor import (
    APIToS3Processor, S3ToPostgresProcessor, AggregationProcessor
)
from src.pipeline.orchestrator import ETLOrchestrator, PipelineTask
from src.pipeline.pipeline_builder import EarthquakePipelineBuilder
from src.core.data_quality import DataQualityChecker, DataQualityRule
from src.utils.secrets import get_secret


OWNER = "VladyslavFS"
DAG_ID = "earthquake_daily_pipeline_oop"

# Load credentials
aws_creds = get_secret("earthquake/aws/credentials")
db_creds = get_secret("earthquake/rds/postgres")
url_style = os.getenv("S3_URL_STYLE", "")

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 9, 1, tz="Europe/Kyiv"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}


def initialize_connectors(**context):
    """Initialize all connectors and store in XCom"""
    execution_date = context['data_interval_start']
    
    # Initialize connectors
    s3_conn = S3Connector(
        aws_access_key=aws_creds['aws_access_key_id'],
        aws_secret_key=aws_creds['aws_secret_access_key'],
        region=aws_creds['aws_region'],
        bucket=aws_creds['s3_bucket_name'],
        endpoint_url=aws_creds.get('s3_endpoint') or None
    )
    s3_conn.connect()
    
    duckdb_conn = DuckDBConnector(
        db_path=":memory:",
        s3_config={
            'access_key': aws_creds['aws_access_key_id'],
            'secret_key': aws_creds['aws_secret_access_key'],
            'region': aws_creds['aws_region'],
            'endpoint': aws_creds.get('s3_endpoint') or None,
            'url_style': url_style or None
        }
    )
    duckdb_conn.connect()
    
    postgres_config = ConnectionConfig(
        host=db_creds['host'],
        port=db_creds['port'],
        database=db_creds['dbname'],
        user=db_creds['username'],
        password=db_creds['password']
    )
    
    postgres_conn = PostgresConnector(postgres_config)
    postgres_conn.connect()
    
    context['task_instance'].xcom_push(
        key='connectors_initialized',
        value=True
    )
    
    return "Connectors initialized successfully"


def extract_api_to_s3(**context):
    """Extract data from API and load to S3"""
    from datetime import datetime
    
    execution_date = context['data_interval_start'].to_datetime_string()
    
    # Re-initialize connectors (in real scenario, use connection pooling)
    s3_conn = S3Connector(
        aws_access_key=aws_creds['aws_access_key_id'],
        aws_secret_key=aws_creds['aws_secret_access_key'],
        region=aws_creds['aws_region'],
        bucket=aws_creds['s3_bucket_name'],
        endpoint_url=aws_creds.get('s3_endpoint') or None
    )
    s3_conn.connect()
    
    duckdb_conn = DuckDBConnector(
        db_path=":memory:",
        s3_config={
            'access_key': aws_creds['aws_access_key_id'],
            'secret_key': aws_creds['aws_secret_access_key'],
            'region': aws_creds['aws_region'],
            'endpoint': aws_creds.get('s3_endpoint') or None,
            'url_style': url_style or None
        }
    )
    duckdb_conn.connect()
    
    # Use OOP processor
    processor = APIToS3Processor(duckdb_conn, s3_conn)
    
    from src.pipeline.etl_processor import ETLConfig
    config = ETLConfig(
        source="earthquake",
        target_schema="",
        target_table="",
        date=context['data_interval_start'],
        s3_bucket=aws_creds['s3_bucket_name'],
        s3_prefix="raw"
    )
    
    success = processor.run(config)
    
    duckdb_conn.disconnect()
    s3_conn.disconnect()
    
    if not success:
        raise Exception("API to S3 extraction failed")
    
    return "API extraction completed"


def load_s3_to_postgres(**context):
    """Load data from S3 to PostgreSQL"""
    
    # Initialize connectors
    s3_conn = S3Connector(
        aws_access_key=aws_creds['aws_access_key_id'],
        aws_secret_key=aws_creds['aws_secret_access_key'],
        region=aws_creds['aws_region'],
        bucket=aws_creds['s3_bucket_name'],
        endpoint_url=aws_creds.get('s3_endpoint') or None
    )
    s3_conn.connect()
    
    duckdb_conn = DuckDBConnector(
        db_path=":memory:",
        s3_config={
            'access_key': aws_creds['aws_access_key_id'],
            'secret_key': aws_creds['aws_secret_access_key'],
            'region': aws_creds['aws_region'],
            'endpoint': aws_creds.get('s3_endpoint') or None,
            'url_style': url_style or None
        }
    )
    duckdb_conn.connect()
    
    postgres_config = ConnectionConfig(
        host=db_creds['host'],
        port=db_creds['port'],
        database=db_creds['dbname'],
        user=db_creds['username'],
        password=db_creds['password']
    )
    
    # Use OOP processor
    processor = S3ToPostgresProcessor(duckdb_conn, s3_conn, postgres_config)
    
    from src.pipeline.etl_processor import ETLConfig
    config = ETLConfig(
        source="earthquake",
        target_schema="ods",
        target_table="fct_earthquake",
        date=context['data_interval_start'],
        s3_bucket=aws_creds['s3_bucket_name'],
        s3_prefix="raw"
    )
    
    success = processor.run(config)
    
    duckdb_conn.disconnect()
    s3_conn.disconnect()
    
    if not success:
        raise Exception("S3 to PostgreSQL load failed")
    
    return "S3 load completed"


def create_aggregations(**context):
    """Create aggregated data marts"""
    
    postgres_config = ConnectionConfig(
        host=db_creds['host'],
        port=db_creds['port'],
        database=db_creds['dbname'],
        user=db_creds['username'],
        password=db_creds['password']
    )
    
    postgres_conn = PostgresConnector(postgres_config)
    postgres_conn.connect()
    
    # Create avg magnitude aggregation
    from src.pipeline.etl_processor import ETLConfig
    avg_config = ETLConfig(
        source="earthquake",
        target_schema="dm",
        target_table="fct_avg_day_earthquake",
        date=context['data_interval_start'],
        s3_bucket="",
        s3_prefix=""
    )
    
    avg_processor = AggregationProcessor(postgres_conn)
    avg_success = avg_processor.run(avg_config)
    
    # Create count aggregation
    count_config = ETLConfig(
        source="earthquake",
        target_schema="dm",
        target_table="fct_count_day_earthquake",
        date=context['data_interval_start'],
        s3_bucket="",
        s3_prefix=""
    )
    
    count_processor = AggregationProcessor(postgres_conn)
    count_success = count_processor.run(count_config)
    
    postgres_conn.disconnect()
    
    if not (avg_success and count_success):
        raise Exception("Aggregation creation failed")
    
    return "Aggregations completed"


def run_data_quality_checks(**context):
    """Run data quality checks"""
    
    postgres_config = ConnectionConfig(
        host=db_creds['host'],
        port=db_creds['port'],
        database=db_creds['dbname'],
        user=db_creds['username'],
        password=db_creds['password']
    )
    
    postgres_conn = PostgresConnector(postgres_config)
    postgres_conn.connect()
    
    checker = DataQualityChecker(postgres_conn)
    
    date_str = context['data_interval_start'].format('YYYY-MM-DD')
    
    # Define quality rules
    rules = [
        DataQualityRule(
            name="row_count_check",
            query=f"SELECT COUNT(*) FROM ods.fct_earthquake WHERE time::date = '{date_str}'",
            threshold=0,
            operator=">"
        ),
        DataQualityRule(
            name="null_magnitude_check",
            query=f"""
                SELECT COUNT(*) * 100.0 / NULLIF(
                    (SELECT COUNT(*) FROM ods.fct_earthquake WHERE time::date = '{date_str}'), 0
                )
                FROM ods.fct_earthquake 
                WHERE time::date = '{date_str}' AND mag IS NULL
            """,
            threshold=5.0,
            operator="<"
        ),
        DataQualityRule(
            name="magnitude_range_check",
            query=f"""
                SELECT COUNT(*) 
                FROM ods.fct_earthquake 
                WHERE time::date = '{date_str}' 
                AND (mag < -2.0 OR mag > 10.0)
            """,
            threshold=0,
            operator="=="
        ),
        DataQualityRule(
            name="duplicate_check",
            query=f"""
                SELECT COUNT(*) - COUNT(DISTINCT id)
                FROM ods.fct_earthquake
                WHERE time::date = '{date_str}'
            """,
            threshold=0,
            operator="=="
        )
    ]
    
    results = checker.run_checks(rules)
    
    postgres_conn.disconnect()
    
    # Check if all rules passed
    failed_checks = [name for name, passed in results.items() if not passed]
    
    if failed_checks:
        raise Exception(f"Data quality checks failed: {', '.join(failed_checks)}")
    
    return f"All {len(results)} quality checks passed"


def send_success_notification(**context):
    """Send success notification (placeholder for email/Slack)"""
    execution_date = context['data_interval_start'].format('YYYY-MM-DD')
    
    message = f"""
    ✅ Earthquake Pipeline Completed Successfully
    
    Date: {execution_date}
    DAG: {DAG_ID}
    Status: SUCCESS
    
    All tasks completed and data quality checks passed.
    """
    
    # In production, send to Slack/Email
    print(message)
    
    return "Notification sent"


# Define the DAG
dag = DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",  # Daily at 5 AM
    default_args=args,
    tags=["earthquake", "oop", "production"],
    catchup=True,
    max_active_runs=1,
    description="Production earthquake data pipeline with OOP architecture"
)

# Define tasks
start = EmptyOperator(
    task_id="start",
    dag=dag
)

init_task = PythonOperator(
    task_id="initialize_connectors",
    python_callable=initialize_connectors,
    dag=dag
)

extract_task = PythonOperator(
    task_id="extract_api_to_s3",
    python_callable=extract_api_to_s3,
    dag=dag
)

# Sensor to wait for S3 file
s3_sensor = S3KeySensor(
    task_id="wait_for_s3_file",
    bucket_key="raw/earthquake/{{ ds }}/{{ ds }}_00-00-00.gz.parquet",
    bucket_name=aws_creds['s3_bucket_name'],
    aws_conn_id=None,  # Uses default boto3 config
    poke_interval=60,
    timeout=3600,
    mode="reschedule",
    dag=dag
)

load_task = PythonOperator(
    task_id="load_s3_to_postgres",
    python_callable=load_s3_to_postgres,
    dag=dag
)

aggregate_task = PythonOperator(
    task_id="create_aggregations",
    python_callable=create_aggregations,
    dag=dag
)

quality_check_task = PythonOperator(
    task_id="data_quality_checks",
    python_callable=run_data_quality_checks,
    dag=dag
)

notify_task = PythonOperator(
    task_id="send_notification",
    python_callable=send_success_notification,
    dag=dag
)

end = EmptyOperator(
    task_id="end",
    dag=dag
)

# Define task dependencies
(
    start 
    >> init_task 
    >> extract_task 
    >> s3_sensor 
    >> load_task 
    >> aggregate_task 
    >> quality_check_task 
    >> notify_task 
    >> end
)
