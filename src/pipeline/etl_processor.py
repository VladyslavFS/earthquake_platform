from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass
import logging


@dataclass
class ETLConfig:
    """ETL configuration"""
    source: str
    target_schema: str
    target_table: str
    date: datetime
    s3_bucket: str
    s3_prefix: str


class BaseETLProcessor(ABC):
    """Abstract base class for ETL processors"""
    
    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def extract(self, config: ETLConfig) -> Any:
        """Extract data from source"""
        pass
    
    @abstractmethod
    def transform(self, data: Any, config: ETLConfig) -> Any:
        """Transform data"""
        pass
    
    @abstractmethod
    def load(self, data: Any, config: ETLConfig) -> bool:
        """Load data to target"""
        pass
    
    def run(self, config: ETLConfig) -> bool:
        """Execute full ETL pipeline"""
        try:
            self.logger.info(f"Starting ETL: {self.name} for {config.date}")
            
            # Extract
            extracted = self.extract(config)
            self.logger.info(f"Extraction completed")
            
            # Transform
            transformed = self.transform(extracted, config)
            self.logger.info(f"Transformation completed")
            
            # Load
            success = self.load(transformed, config)
            
            if success:
                self.logger.info(f"ETL completed successfully: {self.name}")
            else:
                self.logger.error(f"ETL failed during load: {self.name}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"ETL failed: {self.name} - {str(e)}")
            return False


class APIToS3Processor(BaseETLProcessor):
    """Extract from API and load to S3"""
    
    def __init__(self, duckdb_connector, s3_connector):
        super().__init__("API_to_S3")
        self.duckdb = duckdb_connector
        self.s3 = s3_connector
    
    def extract(self, config: ETLConfig) -> str:
        """Build API URL"""
        start_date = config.date.strftime("%Y-%m-%d")
        end_date = (config.date + timedelta(days=1)).strftime("%Y-%m-%d")
        
        api_url = (
            f"https://earthquake.usgs.gov/fdsnws/event/1/query?"
            f"format=csv&starttime={start_date}&endtime={end_date}"
        )
        return api_url
    
    def transform(self, api_url: str, config: ETLConfig) -> Dict[str, str]:
        """No transformation needed, return API URL and S3 key"""
        date_str = config.date.strftime("%Y-%m-%d")
        s3_key = f"{config.s3_prefix}/{config.source}/{date_str}/{date_str}_00-00-00.gz.parquet"
        
        return {
            "api_url": api_url,
            "s3_key": s3_key
        }
    
    def load(self, data: Dict[str, str], config: ETLConfig) -> bool:
        """Load from API to S3 using DuckDB"""
        try:
            query = f"""
            COPY
            (
                SELECT * FROM read_csv_auto('{data['api_url']}')
            ) TO '{self.s3.get_s3_path(data['s3_key'])}';
            """
            self.duckdb.execute(query)
            return True
        except Exception as e:
            self.logger.error(f"Load to S3 failed: {e}")
            return False


class S3ToPostgresProcessor(BaseETLProcessor):
    """Extract from S3 and load to PostgreSQL"""
    
    def __init__(self, duckdb_connector, s3_connector, postgres_config):
        super().__init__("S3_to_Postgres")
        self.duckdb = duckdb_connector
        self.s3 = s3_connector
        self.postgres_config = postgres_config
    
    def extract(self, config: ETLConfig) -> str:
        """Build S3 path"""
        date_str = config.date.strftime("%Y-%m-%d")
        s3_key = f"{config.s3_prefix}/{config.source}/{date_str}/{date_str}_00-00-00.gz.parquet"
        return self.s3.get_s3_path(s3_key)
    
    def transform(self, s3_path: str, config: ETLConfig) -> Dict[str, Any]:
        """Prepare transformation SQL"""
        transform_sql = f"""
        SELECT
            time,
            latitude,
            longitude,
            depth,
            mag,
            magType AS mag_type,
            nst,
            gap,
            dmin,
            rms,
            net,
            id,
            updated,
            place,
            type,
            horizontalError AS horizontal_error,
            depthError AS depth_error,
            magError AS mag_error,
            magNst AS mag_nst,
            status,
            locationSource AS location_source,
            magSource AS mag_source
        FROM '{s3_path}'
        """
        
        return {
            "s3_path": s3_path,
            "transform_sql": transform_sql,
            "target": f"{config.target_schema}.{config.target_table}"
        }
    
    def load(self, data: Dict[str, Any], config: ETLConfig) -> bool:
        """Load to PostgreSQL via DuckDB"""
        try:
            # Setup PostgreSQL connection in DuckDB
            self.duckdb.create_postgres_secret(self.postgres_config)
            
            # Insert data
            insert_sql = f"""
            INSERT INTO dwh_postgres_db.{data['target']}
            {data['transform_sql']}
            """
            
            self.duckdb.execute(insert_sql)
            return True
        except Exception as e:
            self.logger.error(f"Load to PostgreSQL failed: {e}")
            return False


class AggregationProcessor(BaseETLProcessor):
    """Process aggregations for data marts"""
    
    def __init__(self, postgres_connector):
        super().__init__("Aggregation_Processor")
        self.postgres = postgres_connector
    
    def extract(self, config: ETLConfig) -> str:
        """Build extraction query from ODS"""
        return f"SELECT * FROM ods.fct_earthquake WHERE time::date = '{config.date.strftime('%Y-%m-%d')}'"
    
    def transform(self, query: str, config: ETLConfig) -> Dict[str, str]:
        """Build aggregation query"""
        # Determine aggregation type from target table name
        if "avg" in config.target_table:
            agg_function = "AVG(mag::float)"
        elif "count" in config.target_table:
            agg_function = "COUNT(*)"
        elif "sum" in config.target_table:
            agg_function = "SUM(mag::float)"
        else:
            agg_function = "COUNT(*)"
        
        date_str = config.date.strftime('%Y-%m-%d')
        
        staging_table = f"tmp_{config.target_table}_{date_str}"
        
        create_staging_sql = f"""
        CREATE TABLE stg.{staging_table} AS
        SELECT
            time::date AS date,
            {agg_function} as value
        FROM ods.fct_earthquake
        WHERE time::date = '{date_str}'
        GROUP BY 1
        """
        
        return {
            "staging_table": staging_table,
            "create_staging_sql": create_staging_sql,
            "target_table": f"{config.target_schema}.{config.target_table}"
        }
    
    def load(self, data: Dict[str, str], config: ETLConfig) -> bool:
        """Load aggregated data to data mart"""
        try:
            # Create staging table
            self.postgres.execute(data['create_staging_sql'])
            
            # Delete existing data for this date
            delete_sql = f"""
            DELETE FROM {data['target_table']}
            WHERE date IN (SELECT date FROM stg.{data['staging_table']})
            """
            self.postgres.execute(delete_sql)
            
            # Insert new data
            insert_sql = f"""
            INSERT INTO {data['target_table']}
            SELECT * FROM stg.{data['staging_table']}
            """
            self.postgres.execute(insert_sql)
            
            # Drop staging table
            drop_sql = f"DROP TABLE IF EXISTS stg.{data['staging_table']}"
            self.postgres.execute(drop_sql)
            
            return True
        except Exception as e:
            self.logger.error(f"Aggregation load failed: {e}")
            return False