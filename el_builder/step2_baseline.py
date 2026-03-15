from __future__ import annotations

"""
파이프라인 스텝 2: Baseline Event Log 구축.

10개 임상 테이블의 이벤트를 단일 BigQuery 테이블로 통합한다.
step1_mappings에서 생성한 L1/L2 매핑 테이블을 CTE 체인으로 JOIN하여
activity_l0 / activity_l1 / activity_l2 세 레벨을 함께 저장한다.

처리 순서:
  1) event_sql.build_events_union_sql: 10개 테이블 → UNION ALL
  2) L1 매핑 테이블 LEFT JOIN
  3) L2 매핑 테이블 LEFT JOIN
  4) 환자 메타데이터(gender, anchor_age) LEFT JOIN
  5) BigQuery CREATE OR REPLACE TABLE (파티션: MONTH, 클러스터: hadm_id, source_table)

실행: python run.py build-baseline --config configs/mimic_iv_complete.yaml
"""

from typing import List, Tuple

from .config_parser import ELConfig, EventTableConfig
from .event_sql import build_events_union_sql, RESERVED_COLUMNS


def _collect_extra_columns(config: ELConfig) -> List[str]:
    """모든 이벤트 테이블의 extra 컬럼명을 수집한다 (예약 컬럼 제외, 순서 유지)."""
    seen: set = set()
    result: List[str] = []
    for tables in config.event_tables.values():
        for tbl in tables:
            for name in tbl.extra:
                if name not in RESERVED_COLUMNS and name not in seen:
                    seen.add(name)
                    result.append(name)
    return result


def _build_mappings_cte_sql(config: ELConfig) -> Tuple[str, str, str]:
    """
    L1/L2 매핑을 CTE로 단계적 처리하여 반환한다.
    
    Returns:
        (cte_sql, final_select_columns, where_clause)
    """
    # L1도 L2도 비활성화면 단순 처리
    if not (config.mappings.l1.enabled or config.mappings.l2.enabled):
        select_cols = [
            "events.activity_l0 AS activity_l0",
            "CAST(NULL AS STRING) AS activity_l1",
            "CAST(NULL AS STRING) AS activity_l2"
        ]
        return "", ",\n  ".join(select_cols), "events"
    
    cte_parts = []
    where_clauses = []
    
    # L1 매핑
    if config.mappings.l1.enabled and config.mappings.l1.bq_table:
        l1_table = f"`{config.target.project}.{config.target.dataset}.{config.mappings.l1.bq_table}`"
        cte_parts.append(f"""
events_with_l1 AS (
  SELECT
    events.*,
    COALESCE(m1.activity_l1, events.activity_l0) AS activity_l1_computed
  FROM events
  LEFT JOIN {l1_table} AS m1
    ON m1.source_table = events.source_table
   AND m1.activity_raw = events.activity_l0
  WHERE IFNULL(m1.is_excluded, FALSE) = FALSE
)
""".strip())
        
        # L2 매핑
        if config.mappings.l2.enabled and config.mappings.l2.bq_table:
            l2_table = f"`{config.target.project}.{config.target.dataset}.{config.mappings.l2.bq_table}`"
            cte_parts.append(f"""
events_with_l2 AS (
  SELECT
    events_with_l1.*,
    COALESCE(m2.activity_l2, events_with_l1.activity_l1_computed) AS activity_l2_computed
  FROM events_with_l1
  LEFT JOIN {l2_table} AS m2
    ON m2.activity_l1 = events_with_l1.activity_l1_computed
  WHERE IFNULL(m2.is_excluded, FALSE) = FALSE
)
""".strip())
            
            # 최종 SELECT는 events_with_l2에서
            select_cols = [
                "events_with_l2.activity_l0 AS activity_l0",
                "events_with_l2.activity_l1_computed AS activity_l1",
                "events_with_l2.activity_l2_computed AS activity_l2"
            ]
            final_alias = "events_with_l2"
        else:
            # L1만 있음
            select_cols = [
                "events_with_l1.activity_l0 AS activity_l0",
                "events_with_l1.activity_l1_computed AS activity_l1",
                "CAST(NULL AS STRING) AS activity_l2"
            ]
            final_alias = "events_with_l1"
    
    elif config.mappings.l2.enabled and config.mappings.l2.bq_table:
        # L2만 활성화 (드문 경우, L1 없이 L2만)
        # 이 경우 L1은 activity_l0를 그대로 사용
        l2_table = f"`{config.target.project}.{config.target.dataset}.{config.mappings.l2.bq_table}`"
        cte_parts.append(f"""
events_with_l2 AS (
  SELECT
    events.*,
    events.activity_l0 AS activity_l1_computed,
    COALESCE(m2.activity_l2, events.activity_l0) AS activity_l2_computed
  FROM events
  LEFT JOIN {l2_table} AS m2
    ON m2.activity_l1 = events.activity_l0
  WHERE IFNULL(m2.is_excluded, FALSE) = FALSE
)
""".strip())
        select_cols = [
            "events_with_l2.activity_l0 AS activity_l0",
            "events_with_l2.activity_l1_computed AS activity_l1",
            "events_with_l2.activity_l2_computed AS activity_l2"
        ]
        final_alias = "events_with_l2"
    
    cte_sql = ",\n".join(cte_parts) if cte_parts else ""
    select_sql = ",\n  ".join(select_cols)
    
    # WHERE 절은 CTE 내부로 이동했으므로 여기서는 빈 문자열
    return cte_sql, select_sql, final_alias if cte_parts else "events"


def _build_metadata_join_sql(config: ELConfig, base_alias: str = "events") -> Tuple[str, str]:
    """
    메타데이터 JOIN과 SELECT 절을 생성한다.
    
    Args:
        base_alias: 기본 테이블 alias (events 또는 events_with_l1/l2)
    
    Returns:
        (joins_sql, selects_sql)
    """
    if not config.metadata.enabled or not config.metadata.columns:
        return "", ""  # joins, selects

    joins: List[str] = []
    select_additions: List[str] = []

    for col_name, spec in config.metadata.columns.items():
        # dataset 결정
        if spec.source_dataset:
            ds = spec.source_dataset
        elif spec.source_module:
            ds = config.source.datasets.get(spec.source_module)
            if not ds:
                raise ValueError(f"Unknown source_module '{spec.source_module}' for metadata '{col_name}'")
        else:
            # module/dataset 둘 다 없으면 default dataset 사용
            ds = config.source.datasets.get("default")
            if not ds:
                raise ValueError("source.datasets.default must be defined for metadata")

        # 테이블 경로 생성
        if '.' in ds:
            # 이미 "project.dataset" 형식
            src_table = f"`{ds}.{spec.source_table}`"
        else:
            src_table = f"`{config.source.project}.{ds}.{spec.source_table}`"
        
        alias = f"meta_{col_name}"

        joins.append(
            f"LEFT JOIN {src_table} AS {alias}\n"
            f"  ON CAST({alias}.{spec.join_on} AS STRING) = {base_alias}.{spec.join_on}"
        )
        select_additions.append(f"{alias}.{spec.column} AS {col_name}")

    joins_sql = "\n".join(joins)
    select_sql = ",\n  ".join(select_additions)
    return joins_sql, select_sql


def build_baseline_sql(config: ELConfig) -> str:
    """
    Baseline Event Log 전체를 생성하는 CREATE OR REPLACE TABLE 쿼리 문자열을 반환한다.
    """
    events_union = build_events_union_sql(config)
    mapping_cte_sql, mapping_select_sql, final_alias = _build_mappings_cte_sql(config)
    metadata_joins_sql, metadata_select_sql = _build_metadata_join_sql(config, final_alias)

    # extra 컬럼 (resource 등)
    extra_columns = _collect_extra_columns(config)
    extra_select_sql = ""
    if extra_columns:
        extra_parts = [f"{final_alias}.{col}" for col in extra_columns]
        extra_select_sql = ",\n  " + ",\n  ".join(extra_parts)

    # 메타데이터 컬럼 (gender, age 등)
    if metadata_select_sql:
        metadata_select_sql = ",\n  " + metadata_select_sql

    destination_table = f"`{config.target.project}.{config.target.dataset}.{config.output.baseline_table}`"

    # CTE 구성
    cte_section = f"WITH events AS (\n  {events_union}\n)"
    if mapping_cte_sql:
        cte_section += f",\n{mapping_cte_sql}"

    sql = f"""
CREATE OR REPLACE TABLE {destination_table}
PARTITION BY DATE_TRUNC(event_timestamp, MONTH)
CLUSTER BY hadm_id, source_table AS
{cte_section}
SELECT
  {final_alias}.subject_id,
  {final_alias}.hadm_id,
  {final_alias}.stay_id,
  {final_alias}.event_timestamp,
  {mapping_select_sql},
  {final_alias}.source_table,
  {final_alias}.event_type{extra_select_sql}
  {metadata_select_sql}
FROM {final_alias}
{metadata_joins_sql}
""".strip()

    return sql