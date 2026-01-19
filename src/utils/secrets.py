import os
from typing import Dict


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if value is None:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def get_secret(secret_name: str) -> Dict[str, str]:
    if secret_name == "earthquake/aws/credentials":
        return {
            "aws_access_key_id": _require_env("AWS_ACCESS_KEY_ID"),
            "aws_secret_access_key": _require_env("AWS_SECRET_ACCESS_KEY"),
            "aws_region": _require_env("AWS_REGION"),
            "s3_bucket_name": _require_env("S3_BUCKET"),
            "s3_endpoint": os.getenv("S3_ENDPOINT", ""),
        }
    if secret_name == "earthquake/rds/postgres":
        return {
            "host": _require_env("DB_HOST"),
            "port": int(_require_env("DB_PORT")),
            "dbname": _require_env("DB_NAME"),
            "username": _require_env("DB_USER"),
            "password": os.getenv("DB_PASSWORD", ""),
        }
    raise ValueError(f"Unknown secret: {secret_name}")
