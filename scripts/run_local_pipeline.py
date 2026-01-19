#!/usr/bin/env python
import os
from datetime import datetime

from src.core.connectors import ConnectionConfig, DuckDBConnector, PostgresConnector, S3Connector
from src.pipeline.pipeline_builder import EarthquakePipelineBuilder


def env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def build_connectors():
    s3_conn = S3Connector(
        aws_access_key=env("AWS_ACCESS_KEY_ID"),
        aws_secret_key=env("AWS_SECRET_ACCESS_KEY"),
        region=env("AWS_REGION"),
        bucket=env("S3_BUCKET"),
        endpoint_url=os.getenv("S3_ENDPOINT"),
    )
    s3_conn.connect()

    duckdb_conn = DuckDBConnector(
        db_path=":memory:",
        s3_config={
            "access_key": env("AWS_ACCESS_KEY_ID"),
            "secret_key": env("AWS_SECRET_ACCESS_KEY"),
            "region": env("AWS_REGION"),
            "endpoint": os.getenv("S3_ENDPOINT"),
            "url_style": os.getenv("S3_URL_STYLE", "path"),
        },
    )
    duckdb_conn.connect()

    postgres_config = ConnectionConfig(
        host=env("DB_HOST"),
        port=int(env("DB_PORT")),
        database=env("DB_NAME"),
        user=env("DB_USER"),
        password=env("DB_PASSWORD", ""),
    )
    postgres_conn = PostgresConnector(postgres_config)
    postgres_conn.connect()

    return duckdb_conn, s3_conn, postgres_conn, postgres_config


def main():
    execution_date = datetime.utcnow()

    duckdb_conn, s3_conn, postgres_conn, postgres_config = build_connectors()

    builder = EarthquakePipelineBuilder(
        duckdb_conn=duckdb_conn,
        s3_conn=s3_conn,
        postgres_conn=postgres_conn,
        postgres_config=postgres_config,
        s3_config={"bucket": env("S3_BUCKET")},
    )

    orchestrator = builder.build_daily_pipeline(execution_date)
    results = orchestrator.execute()

    print("Pipeline results:")
    for task_id, status in results.items():
        print(f"- {task_id}: {status.value}")

    postgres_conn.disconnect()
    duckdb_conn.disconnect()
    s3_conn.disconnect()


if __name__ == "__main__":
    main()
