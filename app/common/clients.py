import json

import boto3
import httpx
from botocore.config import Config

from app.common.config import CH_PASSWORD, CH_URL, CH_USER, S3_ACCESS_KEY, S3_SECRET_KEY, S3_URL


class ClickHouse:
    def execute(self, sql, parameters=None, data=None):
        params = {f"param_{key}": str(value) for key, value in (parameters or {}).items()}
        if data is None:
            body = sql.encode()
        else:
            params["query"] = sql
            body = data.encode()
        response = httpx.post(
            CH_URL, params=params, content=body, auth=(CH_USER, CH_PASSWORD), timeout=60,
        )
        if response.is_error:
            raise RuntimeError(f"ClickHouse HTTP {response.status_code}: {response.text[:3000]}")
        return response

    def query(self, sql, parameters=None):
        return self.execute(sql + " FORMAT JSON", parameters).json()["data"]

    def insert(self, table, rows):
        if rows:
            data = "\n".join(json.dumps(row, ensure_ascii=False, allow_nan=False) for row in rows)
            self.execute(f"INSERT INTO {table} FORMAT JSONEachRow", data=data)


def s3_client():
    return boto3.client(
        "s3", endpoint_url=S3_URL, aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY, region_name="us-east-1",
        config=Config(connect_timeout=5, read_timeout=15, retries={"max_attempts": 3},
                      s3={"addressing_style": "path"}),
    )
