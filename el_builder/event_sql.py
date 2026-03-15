from __future__ import annotations

"""
이벤트 타입별 BigQuery SQL 생성 — 파이프라인 내부 모듈.

임상 테이블의 행을 4가지 이벤트 타입으로 분류하여 공통 스키마로 변환한다.
변환된 이벤트들은 UNION ALL로 통합되어 Baseline EL의 기반이 된다.

공통 출력 스키마: subject_id / hadm_id / stay_id / event_timestamp / activity_l0 / source_table / event_type

이벤트 타입 정의:
  journey  : 1행 → 2 이벤트 (start/end). 서로 다른 activity명.
             예) admissions: admittime → admit_[type] / dischtime → disch_[type]
  in_out   : 1행 → 2 이벤트 (in/out + 위치). 이동 이벤트.
             (설계됐으나 모든 사용 사례가 journey로 처리 가능해 현재 미사용)
  duration : 1행 → 2 이벤트 (start/end, 동일 activity명). 처치 기간 이벤트.
             예) inputevents: starttime → start_[category] / endtime → complete_[category]
  one_off  : 1행 → 1 이벤트. 단일 시점 기록.
             예) labevents: charttime → activity_l0
"""

import re
from typing import List

from .config_parser import ELConfig, EventTableConfig, JoinConfig

# 예약된 컬럼명 (extra에서 중복 방지)
RESERVED_COLUMNS = {
    'subject_id', 'hadm_id', 'stay_id', 
    'event_timestamp', 'activity_l0', 
    'source_table', 'event_type'
}


def _resolve_dataset(config: ELConfig, tbl: EventTableConfig) -> str:
    """module / dataset / default 규칙에 따라 dataset 이름을 결정한다."""
    if tbl.dataset:
        return tbl.dataset
    if tbl.module:
        ds = config.source.datasets.get(tbl.module)
        if not ds:
            raise ValueError(f"Unknown module '{tbl.module}' in table '{tbl.table}'")
        return ds
    # fallback: default
    default_ds = config.source.datasets.get("default")
    if not default_ds:
        raise ValueError("source.datasets.default must be defined")
    return default_ds


def _build_full_table_path(config: ELConfig, dataset: str, table: str) -> str:
    """
    dataset과 table을 조합하여 완전한 테이블 경로를 생성한다.

    - dataset에 "."이 있으면 (예: "other-project.other_dataset")
      이미 project.dataset 형식이므로 그대로 사용
    - 없으면 source.project를 앞에 붙임
    """
    if '.' in dataset:
        # 이미 "project.dataset" 형식
        return f"`{dataset}.{table}`"
    else:
        # dataset만 있음 → source.project 추가
        return f"`{config.source.project}.{dataset}.{table}`"


def _qualify_column(expr: str, alias: str = "src") -> str:
    """단순 식별자이면 alias를 붙이고, 그 외(리터럴·SQL 표현식)는 그대로 반환."""
    if re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', expr):
        return f"{alias}.{expr}"
    return expr


def _resolve_join_dataset(config: ELConfig, join: JoinConfig, parent_tbl: EventTableConfig) -> str:
    """JOIN 대상 테이블의 dataset을 결정한다. 미지정 시 부모 테이블의 dataset을 따른다."""
    if join.dataset:
        return join.dataset
    if join.module:
        ds = config.source.datasets.get(join.module)
        if not ds:
            raise ValueError(f"Unknown module '{join.module}' in join table '{join.table}'")
        return ds
    return _resolve_dataset(config, parent_tbl)


def _build_from_clause(config: ELConfig, src_table: str, tbl: EventTableConfig) -> str:
    """filter가 있으면 서브쿼리로 감싸고, join이 있으면 추가 JOIN 절을 붙인다."""
    if tbl.filter:
        base = f"(SELECT * FROM {src_table} WHERE {tbl.filter}) AS src"
    else:
        base = f"{src_table} AS src"

    if tbl.join:
        join_ds = _resolve_join_dataset(config, tbl.join, tbl)
        join_table = _build_full_table_path(config, join_ds, tbl.join.table)
        base += f"\n  INNER JOIN {join_table} AS {tbl.join.alias}\n    ON {tbl.join.on}"

    return base


def _cohort_join_clause(config: ELConfig, tbl: EventTableConfig) -> str:
    """코호트 필터링을 위한 INNER JOIN 절 또는 빈 문자열."""
    if not config.cohort.enabled:
        return ""
    cohort = config.cohort
    assert cohort.bq_table and cohort.case_id_column
    
    # cohort.bq_table 경로 처리
    if cohort.bq_table.count('.') >= 2:
        # 이미 "project.dataset.table" 형식
        cohort_full_table = f"`{cohort.bq_table}`"
    elif cohort.bq_table.count('.') == 1:
        # "dataset.table" 형식 → target.project만 붙임
        cohort_full_table = f"`{config.target.project}.{cohort.bq_table}`"
    else:
        # "table"만 있음 → target.project.target.dataset 붙임
        cohort_full_table = f"`{config.target.project}.{config.target.dataset}.{cohort.bq_table}`"
    
    return (
        f"INNER JOIN {cohort_full_table} AS cohort\n"
        f"  ON CAST(src.{tbl.case_id_column} AS STRING) = CAST(cohort.{cohort.case_id_column} AS STRING)"
    )


def _extra_selects(tbl: EventTableConfig) -> str:
    """
    extra 컬럼을 SELECT 절에 추가한다.
    예약된 컬럼명(subject_id, hadm_id 등)은 스킵한다.
    """
    if not tbl.extra:
        return ""
    parts = []
    for name, expr in tbl.extra.items():
        if name not in RESERVED_COLUMNS:
            parts.append(f", {_qualify_column(expr)} AS {name}")
    return "\n".join(parts)


def _common_ids_select(tbl: EventTableConfig) -> str:
    """
    모든 ID는 STRING 캐스팅 (NULL 허용).
    subject_id_column / hadm_id_column 이 지정되면 해당 표현식 사용,
    없으면 기본값(subject_id / hadm_id)을 auto-qualify.
    """
    subj_col = tbl.subject_id_column or "subject_id"
    hadm_col = tbl.hadm_id_column or "hadm_id"

    subj_expr = f"CAST({_qualify_column(subj_col)} AS STRING) AS subject_id"
    hadm_expr = f"CAST({_qualify_column(hadm_col)} AS STRING) AS hadm_id"

    if tbl.stay_id_column:
        stay_expr = f"CAST({_qualify_column(tbl.stay_id_column)} AS STRING) AS stay_id"
    else:
        stay_expr = "CAST(NULL AS STRING) AS stay_id"

    return f"{subj_expr},\n{hadm_expr},\n{stay_expr}"


def _journey_sql(config: ELConfig, tbl: EventTableConfig) -> str:
    ds = _resolve_dataset(config, tbl)
    src_table = _build_full_table_path(config, ds, tbl.table)
    from_clause = _build_from_clause(config, src_table, tbl)
    cohort_join = _cohort_join_clause(config, tbl)
    extra = _extra_selects(tbl)

    # 1행 → 2 이벤트: start, end
    start_activity = _qualify_column(tbl.start_activity) if tbl.start_activity else f"'journey_start_from_{tbl.table}'"
    end_activity = _qualify_column(tbl.end_activity) if tbl.end_activity else f"'journey_end_from_{tbl.table}'"

    start_ts = _qualify_column(tbl.start_column)
    end_ts = _qualify_column(tbl.end_column)

    return f"""
-- journey from {src_table}
SELECT
  {_common_ids_select(tbl)},
  {start_ts} AS event_timestamp,
  {start_activity} AS activity_l0,
  '{tbl.table}' AS source_table,
  'journey_start' AS event_type
  {extra}
FROM {from_clause}
{cohort_join}

UNION ALL

SELECT
  {_common_ids_select(tbl)},
  {end_ts} AS event_timestamp,
  {end_activity} AS activity_l0,
  '{tbl.table}' AS source_table,
  'journey_end' AS event_type
  {extra}
FROM {from_clause}
{cohort_join}
""".strip()


def _in_out_sql(config: ELConfig, tbl: EventTableConfig) -> str:
    ds = _resolve_dataset(config, tbl)
    src_table = _build_full_table_path(config, ds, tbl.table)
    from_clause = _build_from_clause(config, src_table, tbl)
    cohort_join = _cohort_join_clause(config, tbl)
    extra = _extra_selects(tbl)

    in_ts = _qualify_column(tbl.in_column)
    out_ts = _qualify_column(tbl.out_column)

    return f"""
-- in_out from {src_table}
SELECT
  {_common_ids_select(tbl)},
  {in_ts} AS event_timestamp,
  CONCAT('ICU_in_', CAST(src.{tbl.location_column} AS STRING)) AS activity_l0,
  '{tbl.table}' AS source_table,
  'in' AS event_type
  {extra}
FROM {from_clause}
{cohort_join}

UNION ALL

SELECT
  {_common_ids_select(tbl)},
  {out_ts} AS event_timestamp,
  CONCAT('ICU_out_', CAST(src.{tbl.location_column} AS STRING)) AS activity_l0,
  '{tbl.table}' AS source_table,
  'out' AS event_type
  {extra}
FROM {from_clause}
{cohort_join}
""".strip()


def _duration_sql(config: ELConfig, tbl: EventTableConfig) -> str:
    ds = _resolve_dataset(config, tbl)
    src_table = _build_full_table_path(config, ds, tbl.table)
    from_clause = _build_from_clause(config, src_table, tbl)
    cohort_join = _cohort_join_clause(config, tbl)
    extra = _extra_selects(tbl)

    activity_expr = _qualify_column(tbl.activity_column) if tbl.activity_column else f"'duration_from_{tbl.table}'"

    start_ts = _qualify_column(tbl.start_column)
    end_ts = _qualify_column(tbl.end_column)

    return f"""
-- duration from {src_table}
SELECT
  {_common_ids_select(tbl)},
  {start_ts} AS event_timestamp,
  {activity_expr} AS activity_l0,
  '{tbl.table}' AS source_table,
  'duration_start' AS event_type
  {extra}
FROM {from_clause}
{cohort_join}

UNION ALL

SELECT
  {_common_ids_select(tbl)},
  {end_ts} AS event_timestamp,
  {activity_expr} AS activity_l0,
  '{tbl.table}' AS source_table,
  'duration_end' AS event_type
  {extra}
FROM {from_clause}
{cohort_join}
""".strip()


def _one_off_sql(config: ELConfig, tbl: EventTableConfig) -> str:
    ds = _resolve_dataset(config, tbl)
    src_table = _build_full_table_path(config, ds, tbl.table)
    from_clause = _build_from_clause(config, src_table, tbl)
    cohort_join = _cohort_join_clause(config, tbl)
    extra = _extra_selects(tbl)

    activity_expr = _qualify_column(tbl.activity_column) if tbl.activity_column else f"'one_off_from_{tbl.table}'"

    ts = _qualify_column(tbl.timestamp_column)

    return f"""
-- one_off from {src_table}
SELECT
  {_common_ids_select(tbl)},
  {ts} AS event_timestamp,
  {activity_expr} AS activity_l0,
  '{tbl.table}' AS source_table,
  'one_off' AS event_type
  {extra}
FROM {from_clause}
{cohort_join}
""".strip()


def build_events_union_sql(config: ELConfig) -> str:
    """
    4가지 유형(journey, in_out, duration, one_off)의 이벤트 테이블을
    모두 UNION ALL 한 서브쿼리 SQL을 생성한다.

    반환되는 스키마:
      subject_id, hadm_id, stay_id,
      event_timestamp,
      activity_l0,
      source_table,
      event_type,
      + extra 컬럼들
    """
    parts: List[str] = []
    for etype, tables in config.event_tables.items():
        if not tables:
            # 빈 유형은 조용히 스킵
            continue
        for tbl in tables:
            if etype == "journey":
                parts.append(_journey_sql(config, tbl))
            elif etype == "in_out":
                parts.append(_in_out_sql(config, tbl))
            elif etype == "duration":
                parts.append(_duration_sql(config, tbl))
            elif etype == "one_off":
                parts.append(_one_off_sql(config, tbl))

    if not parts:
        # 이벤트 테이블이 하나도 없으면 빈 SELECT를 반환 (실제 운영에서는 피하는 것이 좋음)
        return """
SELECT
  CAST(NULL AS STRING) AS subject_id,
  CAST(NULL AS STRING) AS hadm_id,
  CAST(NULL AS STRING) AS stay_id,
  TIMESTAMP(NULL) AS event_timestamp,
  CAST(NULL AS STRING) AS activity_l0,
  CAST(NULL AS STRING) AS source_table,
  CAST(NULL AS STRING) AS event_type
WHERE 1 = 0
""".strip()

    return "\n\nUNION ALL\n\n".join(parts)