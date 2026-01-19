from datetime import datetime
from typing import Dict
from pipeline.etl_processor import APIToS3Processor, AggregationProcessor, ETLConfig, S3ToPostgresProcessor
from pipeline.orchestrator import ETLOrchestrator, PipelineTask


class EarthquakePipelineBuilder:
    """Builder for earthquake data pipeline"""
    
    def __init__(
        self,
        duckdb_conn,
        s3_conn,
        postgres_conn,
        postgres_config,
        s3_config: Dict[str, str]
    ):
        self.duckdb = duckdb_conn
        self.s3 = s3_conn
        self.postgres = postgres_conn
        self.postgres_config = postgres_config
        self.s3_config = s3_config
        self.orchestrator = ETLOrchestrator()
    
    def build_daily_pipeline(self, execution_date: datetime) -> ETLOrchestrator:
        """Build complete daily pipeline"""
        
        # Task 1: API to S3
        api_config = ETLConfig(
            source="earthquake",
            target_schema="",
            target_table="",
            date=execution_date,
            s3_bucket=self.s3_config['bucket'],
            s3_prefix="raw"
        )
        
        api_processor = APIToS3Processor(self.duckdb, self.s3)
        api_task = PipelineTask(
            task_id="extract_api_to_s3",
            processor=api_processor,
            config=api_config,
            dependencies=[]
        )
        self.orchestrator.add_task(api_task)
        
        # Task 2: S3 to PostgreSQL ODS
        ods_config = ETLConfig(
            source="earthquake",
            target_schema="ods",
            target_table="fct_earthquake",
            date=execution_date,
            s3_bucket=self.s3_config['bucket'],
            s3_prefix="raw"
        )
        
        s3_processor = S3ToPostgresProcessor(
            self.duckdb, self.s3, self.postgres_config
        )
        s3_task = PipelineTask(
            task_id="load_s3_to_ods",
            processor=s3_processor,
            config=ods_config,
            dependencies=["extract_api_to_s3"]
        )
        self.orchestrator.add_task(s3_task)
        
        # Task 3: Aggregation - Average magnitude
        avg_config = ETLConfig(
            source="earthquake",
            target_schema="dm",
            target_table="fct_avg_day_earthquake",
            date=execution_date,
            s3_bucket="",
            s3_prefix=""
        )
        
        avg_processor = AggregationProcessor(self.postgres)
        avg_task = PipelineTask(
            task_id="aggregate_avg_magnitude",
            processor=avg_processor,
            config=avg_config,
            dependencies=["load_s3_to_ods"]
        )
        self.orchestrator.add_task(avg_task)
        
        # Task 4: Aggregation - Count
        count_config = ETLConfig(
            source="earthquake",
            target_schema="dm",
            target_table="fct_count_day_earthquake",
            date=execution_date,
            s3_bucket="",
            s3_prefix=""
        )
        
        count_processor = AggregationProcessor(self.postgres)
        count_task = PipelineTask(
            task_id="aggregate_count_earthquakes",
            processor=count_processor,
            config=count_config,
            dependencies=["load_s3_to_ods"]
        )
        self.orchestrator.add_task(count_task)
        
        return self.orchestrator