from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import boto3
import duckdb
import logging
from dataclasses import dataclass


@dataclass
class ConnectionConfig:
    """Configuration for database connections"""
    host: str
    port: int
    database: str
    user: str
    password: str


class BaseConnector(ABC):
    """Abstract base class for all connectors"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
        self._connection = None
    
    @abstractmethod
    def connect(self):
        """Establish connection"""
        pass
    
    @abstractmethod
    def disconnect(self):
        """Close connection"""
        pass
    
    @abstractmethod
    def execute(self, query: str, params: Optional[Dict] = None):
        """Execute query"""
        pass


class S3Connector(BaseConnector):
    """Handles all S3 operations"""
    
    def __init__(self, aws_access_key: str, aws_secret_key: str, region: str, bucket: str):
        super().__init__()
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.region = region
        self.bucket = bucket
        self._client = None
    
    def connect(self):
        """Initialize S3 client"""
        self._client = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key,
            region_name=self.region
        )
        self.logger.info(f"Connected to S3 bucket: {self.bucket}")
        return self._client
    
    def disconnect(self):
        """S3 client doesn't need explicit disconnect"""
        self._client = None
    
    def execute(self, query: str, params: Optional[Dict] = None):
        """Not applicable for S3"""
        raise NotImplementedError("Use specific S3 methods")
    
    def upload_file(self, local_path: str, s3_key: str) -> bool:
        """Upload file to S3"""
        try:
            self._client.upload_file(local_path, self.bucket, s3_key)
            self.logger.info(f"Uploaded {local_path} to s3://{self.bucket}/{s3_key}")
            return True
        except Exception as e:
            self.logger.error(f"S3 upload failed: {e}")
            return False
    
    def file_exists(self, s3_key: str) -> bool:
        """Check if file exists in S3"""
        try:
            self._client.head_object(Bucket=self.bucket, Key=s3_key)
            return True
        except:
            return False
    
    def get_s3_path(self, key: str) -> str:
        """Get full S3 path"""
        return f"s3://{self.bucket}/{key}"


class DuckDBConnector(BaseConnector):
    """Handles DuckDB operations with S3 integration"""
    
    def __init__(self, db_path: str = ":memory:", s3_config: Optional[Dict] = None):
        super().__init__()
        self.db_path = db_path
        self.s3_config = s3_config or {}
        self._conn = None
    
    def connect(self):
        """Initialize DuckDB connection"""
        self._conn = duckdb.connect(self.db_path)
        self._setup_extensions()
        if self.s3_config:
            self._configure_s3()
        self.logger.info(f"Connected to DuckDB: {self.db_path}")
        return self._conn
    
    def _setup_extensions(self):
        """Install and load required extensions"""
        self._conn.execute("INSTALL httpfs;")
        self._conn.execute("LOAD httpfs;")
        self._conn.execute("SET TIMEZONE='UTC';")
    
    def _configure_s3(self):
        """Configure S3 access"""
        self._conn.execute(f"SET s3_region = '{self.s3_config.get('region')}';")
        self._conn.execute(f"SET s3_access_key_id = '{self.s3_config.get('access_key')}';")
        self._conn.execute(f"SET s3_secret_access_key = '{self.s3_config.get('secret_key')}';")
        self._conn.execute("SET s3_use_ssl = TRUE;")
    
    def disconnect(self):
        """Close DuckDB connection"""
        if self._conn:
            self._conn.close()
            self._conn = None
    
    def execute(self, query: str, params: Optional[Dict] = None):
        """Execute SQL query"""
        try:
            result = self._conn.execute(query)
            return result.fetchall()
        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            raise
    
    def execute_and_fetch_df(self, query: str):
        """Execute query and return as DataFrame"""
        return self._conn.execute(query).df()
    
    def create_postgres_secret(self, config: ConnectionConfig):
        """Create PostgreSQL secret for DuckDB"""
        secret_sql = f"""
        CREATE SECRET IF NOT EXISTS dwh_postgres (
            TYPE postgres,
            HOST '{config.host}',
            PORT {config.port},
            DATABASE {config.database},
            USER {config.user},
            PASSWORD '{config.password}'
        );
        """
        self._conn.execute(secret_sql)
        self._conn.execute("ATTACH '' AS dwh_postgres_db (TYPE postgres, SECRET dwh_postgres);")


class PostgresConnector(BaseConnector):
    """Handles PostgreSQL operations"""
    
    def __init__(self, config: ConnectionConfig):
        super().__init__()
        self.config = config
        self._conn = None
    
    def connect(self):
        """Initialize PostgreSQL connection"""
        import psycopg2
        self._conn = psycopg2.connect(
            host=self.config.host,
            port=self.config.port,
            database=self.config.database,
            user=self.config.user,
            password=self.config.password
        )
        self.logger.info(f"Connected to PostgreSQL: {self.config.host}")
        return self._conn
    
    def disconnect(self):
        """Close PostgreSQL connection"""
        if self._conn:
            self._conn.close()
            self._conn = None
    
    def execute(self, query: str, params: Optional[Dict] = None):
        """Execute SQL query"""
        cursor = self._conn.cursor()
        try:
            cursor.execute(query, params)
            self._conn.commit()
            return cursor.fetchall() if cursor.description else None
        except Exception as e:
            self._conn.rollback()
            self.logger.error(f"Query execution failed: {e}")
            raise
        finally:
            cursor.close()
