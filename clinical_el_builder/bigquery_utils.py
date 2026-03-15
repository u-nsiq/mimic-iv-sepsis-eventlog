"""
BigQuery 클라이언트 유틸리티.
"""

from __future__ import annotations

from google.cloud import bigquery


def get_client(project: str) -> bigquery.Client:
    """지정한 GCP 프로젝트로 BigQuery 클라이언트를 생성한다."""
    return bigquery.Client(project=project)


def run_query(client: bigquery.Client, sql: str) -> bigquery.table.RowIterator:
    """쿼리를 실행하고 결과를 반환한다."""
    return client.query(sql).result()
