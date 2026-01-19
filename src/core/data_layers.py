from abc import ABC, abstractmethod
from enum import Enum
import logging
from typing import List, Dict, Any
from datetime import datetime

from core.connectors import BaseConnector, DuckDBConnector, PostgresConnector, S3Connector


class DataLayer(Enum):
    """Data layer enumeration"""
    RAW = "raw"
    STAGING = "stg"
    ODS = "ods"
    DM = "dm"


class BaseDataLayer(ABC):
    """Abstract base class for data layers"""
    
    def __init__(self, connector: BaseConnector, layer: DataLayer):
        self.connector = connector
        self.layer = layer
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def load_data(self, source: str, target: str, date: datetime):
        """Load data into the layer"""
        pass
    
    @abstractmethod
    def validate_data(self, table: str) -> bool:
        """Validate data quality"""
        pass


class RawDataLayer(BaseDataLayer):
    """Handles raw data operations"""
    
    def __init__(self, s3_connector: S3Connector, duckdb_connector: DuckDBConnector):
        super().__init__(duckdb_connector, DataLayer.RAW)
        self.s3_connector = s3_connector
    
    def load_data(self, source_url: str, s3_key: str, date: datetime):
        """Load data from API to S3"""
        query = f"""
        COPY
        (
            SELECT * FROM read_csv_auto('{source_url}')
        ) TO '{self.s3_connector.get_s3_path(s3_key)}';
        """
        self.connector.execute(query)
        self.logger.info(f"Loaded raw data to {s3_key}")
    
    def validate_data(self, s3_key: str) -> bool:
        """Validate raw data exists"""
        return self.s3_connector.file_exists(s3_key)


class StagingLayer(BaseDataLayer):
    """Handles staging operations"""
    
    def __init__(self, duckdb_connector: DuckDBConnector):
        super().__init__(duckdb_connector, DataLayer.STAGING)
    
    def create_temp_table(self, table_name: str, query: str):
        """Create temporary staging table"""
        create_sql = f"CREATE TABLE stg.{table_name} AS {query}"
        self.connector.execute(create_sql)
        self.logger.info(f"Created staging table: {table_name}")
    
    def drop_temp_table(self, table_name: str):
        """Drop temporary table"""
        self.connector.execute(f"DROP TABLE IF EXISTS stg.{table_name}")
    
    def load_data(self, source: str, target: str, date: datetime):
        """Load data from raw to staging"""
        pass  # Implemented in ETL processor
    
    def validate_data(self, table: str) -> bool:
        """Validate staging data"""
        result = self.connector.execute(f"SELECT COUNT(*) FROM stg.{table}")
        count = result[0][0] if result else 0
        return count > 0


class DataMartLayer(BaseDataLayer):
    """Handles data mart operations"""
    
    def __init__(self, postgres_connector: PostgresConnector):
        super().__init__(postgres_connector, DataLayer.DM)
    
    def load_data(self, source: str, target: str, date: datetime):
        """Load data into data mart"""
        pass  # Implemented in ETL processor
    
    def validate_data(self, table: str) -> bool:
        """Validate data mart data"""
        result = self.connector.execute(f"SELECT COUNT(*) FROM dm.{table}")
        count = result[0][0] if result else 0
        return count > 0
    
    def upsert_data(self, table: str, staging_table: str):
        """Perform upsert operation"""
        delete_sql = f"""
        DELETE FROM dm.{table}
        WHERE date IN (SELECT date FROM stg.{staging_table})
        """
        insert_sql = f"""
        INSERT INTO dm.{table}
        SELECT * FROM stg.{staging_table}
        """
        self.connector.execute(delete_sql)
        self.connector.execute(insert_sql)
        self.logger.info(f"Upserted data into {table}")