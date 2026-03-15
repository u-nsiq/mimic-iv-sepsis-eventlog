from __future__ import annotations

"""
파이프라인 스텝 1: L1/L2 추상화 매핑 테이블 생성.

각 이벤트 테이블의 고카디널리티 activity_l0를 분석 가능한 수준으로 축소하는 것이 목적.
YAML 규칙(mappings_config.yaml)에 따라 L1/L2 매핑 테이블을 BigQuery에 생성한다.

  예) chartevents: itemid(1,864종) → d_items.category(40종) → 커스텀 그룹(11종)
  예) emar: pharmacy_id(878,946종) → NDC 제형(31종) → 투여 경로 그룹(6종)

emar 매핑은 pharmacy_id → prescriptions.ndc → ndc_to_doseform 테이블을 거치며,
매핑 손실률 9.8%(95,051건)이 발생한다. 손실 원인:
  - prescriptions 미존재 또는 NDC NULL: 64,343건
  - NDC → dose_form 매핑 실패: 34,117건

실행: python run.py build-mappings --config configs/mimic_iv_complete.yaml \\
                                   --mappings-config configs/mappings_config.yaml
"""

from typing import List

from .config_parser import (
    ELConfig,
    MappingRuleConfig,
    MappingsRulesConfig,
)


# ---------------------------------------------------------------------------
# 공통 헬퍼 (event_extractor와 독립적으로 유지)
# ---------------------------------------------------------------------------

def _resolve_module_dataset(config: ELConfig, module: str | None, dataset: str | None) -> str:
    """module / dataset / default 규칙에 따라 dataset 이름을 결정한다."""
    if dataset:
        return dataset
    if module:
        ds = config.source.datasets.get(module)
        if not ds:
            raise ValueError(f"알 수 없는 모듈 '{module}'")
        return ds
    default_ds = config.source.datasets.get("default")
    if not default_ds:
        raise ValueError("source.datasets.default가 정의되어 있어야 합니다")
    return default_ds


def _full_table_path(config: ELConfig, dataset: str, table: str) -> str:
    """dataset과 table을 조합하여 완전한 BigQuery 테이블 경로를 생성한다."""
    if '.' in dataset:
        return f"`{dataset}.{table}`"
    return f"`{config.source.project}.{dataset}.{table}`"


# ---------------------------------------------------------------------------
# L1 규칙별 SQL 생성
# ---------------------------------------------------------------------------

def _l1_reference_join_sql(config: ELConfig, rule: MappingRuleConfig) -> str:
    """reference_join: 소스 테이블과 참조 테이블을 JOIN하여 L1 매핑 생성."""
    src_ds = _resolve_module_dataset(config, rule.source_module, rule.source_dataset)
    src_table = _full_table_path(config, src_ds, rule.source_table)

    ref_ds = _resolve_module_dataset(config, rule.reference_module, rule.reference_dataset)
    ref_table = _full_table_path(config, ref_ds, rule.reference_table)

    from_clause = src_table
    if rule.filter:
        from_clause = f"(SELECT * FROM {src_table} WHERE {rule.filter})"

    return f"""\
-- reference_join: {rule.source_table} → {rule.reference_table}.{rule.l1_column}
SELECT DISTINCT
  '{rule.source_table}' AS source_table,
  CAST(src.{rule.activity_raw_column} AS STRING) AS activity_raw,
  CAST(ref.{rule.l1_column} AS STRING) AS activity_l1,
  FALSE AS is_excluded
FROM {from_clause} AS src
JOIN {ref_table} AS ref
  ON src.{rule.join_column} = ref.{rule.join_column}"""


def _l1_dict_sql(rule: MappingRuleConfig) -> str:
    """dict: 명시적 값 → 값 매핑을 SELECT 리터럴로 생성."""
    parts: List[str] = []
    for raw_val, l1_val in rule.mapping.items():
        parts.append(
            f"SELECT '{rule.source_table}' AS source_table, "
            f"'{raw_val}' AS activity_raw, "
            f"'{l1_val}' AS activity_l1, "
            f"FALSE AS is_excluded"
        )
    return f"-- dict: {rule.source_table}\n" + "\nUNION ALL\n".join(parts)


def _l1_expression_sql(config: ELConfig, rule: MappingRuleConfig) -> str:
    """expression: SQL 표현식으로 activity_raw와 L1을 생성."""
    src_ds = _resolve_module_dataset(config, rule.source_module, rule.source_dataset)
    src_table = _full_table_path(config, src_ds, rule.source_table)

    from_clause = src_table
    if rule.filter:
        from_clause = f"(SELECT * FROM {src_table} WHERE {rule.filter})"

    return f"""\
-- expression: {rule.source_table}
SELECT DISTINCT
  '{rule.source_table}' AS source_table,
  CAST({rule.activity_raw_expr} AS STRING) AS activity_raw,
  CAST({rule.l1_expr} AS STRING) AS activity_l1,
  FALSE AS is_excluded
FROM {from_clause} AS src"""


# ---------------------------------------------------------------------------
# L2 규칙별 SQL 생성
# ---------------------------------------------------------------------------

def _l2_dict_sql(rule: MappingRuleConfig) -> str:
    """dict: 명시적 L1 → L2 매핑을 SELECT 리터럴로 생성."""
    parts: List[str] = []
    for l1_val, l2_val in rule.mapping.items():
        parts.append(
            f"SELECT '{l1_val}' AS activity_l1, "
            f"'{l2_val}' AS activity_l2, "
            f"FALSE AS is_excluded"
        )
    return "-- dict\n" + "\nUNION ALL\n".join(parts)


def _l2_conditional_sql(config: ELConfig, mappings_rules: MappingsRulesConfig, rule: MappingRuleConfig) -> str:
    """conditional: L1 테이블에서 CASE WHEN으로 L2 매핑 생성."""
    l1_table = f"`{config.target.project}.{config.target.dataset}.{mappings_rules.l1.output_table}`"

    case_parts: List[str] = []
    for cond in rule.conditions:
        case_parts.append(f"    WHEN activity_l1 {cond['when']} THEN '{cond['value']}'")

    # default=None이면 ELSE 절 생략 (매칭 안 된 행은 NULL → 제외)
    if rule.default is not None:
        else_clause = f"\n    ELSE '{rule.default}'"
    else:
        else_clause = ""

    case_expr = "\n".join(case_parts)

    # source_tables가 지정되면 해당 테이블의 L1만 대상
    if rule.source_tables:
        in_list = ", ".join(f"'{t}'" for t in rule.source_tables)
        where_clause = f"\nWHERE source_table IN ({in_list})"
    else:
        where_clause = ""

    # default=None이면 NULL 결과를 필터링하여 중복 방지
    if rule.default is None:
        return f"""\
-- conditional: L1 테이블 기반 CASE WHEN
SELECT DISTINCT
  activity_l1,
  activity_l2,
  FALSE AS is_excluded
FROM (
  SELECT DISTINCT
    activity_l1,
    CASE
{case_expr}
    END AS activity_l2
  FROM {l1_table}{where_clause}
)
WHERE activity_l2 IS NOT NULL"""
    else:
        return f"""\
-- conditional: L1 테이블 기반 CASE WHEN
SELECT DISTINCT
  activity_l1,
  CASE
{case_expr}
    ELSE '{rule.default}'
  END AS activity_l2,
  FALSE AS is_excluded
FROM {l1_table}{where_clause}"""


# ---------------------------------------------------------------------------
# 공개 함수: 전체 매핑 SQL 생성
# ---------------------------------------------------------------------------

def build_l1_mapping_sql(config: ELConfig, mappings_rules: MappingsRulesConfig) -> str | None:
    """L1 매핑 테이블을 생성하는 CREATE OR REPLACE TABLE SQL을 반환한다."""
    if not mappings_rules.l1:
        return None

    dest = f"`{config.target.project}.{config.target.dataset}.{mappings_rules.l1.output_table}`"
    parts: List[str] = []

    for rule in mappings_rules.l1.rules:
        if rule.type == "reference_join":
            parts.append(_l1_reference_join_sql(config, rule))
        elif rule.type == "dict":
            parts.append(_l1_dict_sql(rule))
        elif rule.type == "expression":
            parts.append(_l1_expression_sql(config, rule))
        elif rule.type == "custom_sql":
            parts.append(f"-- custom_sql\nSELECT * FROM (\n{rule.sql}) AS _custom")

    if not parts:
        return None

    union_sql = "\n\nUNION ALL\n\n".join(parts)

    return f"""\
CREATE OR REPLACE TABLE {dest} AS
{union_sql}"""


def build_l2_mapping_sql(config: ELConfig, mappings_rules: MappingsRulesConfig) -> str | None:
    """L2 매핑 테이블을 생성하는 CREATE OR REPLACE TABLE SQL을 반환한다."""
    if not mappings_rules.l2:
        return None

    dest = f"`{config.target.project}.{config.target.dataset}.{mappings_rules.l2.output_table}`"
    parts: List[str] = []

    for rule in mappings_rules.l2.rules:
        if rule.type == "dict":
            parts.append(_l2_dict_sql(rule))
        elif rule.type == "conditional":
            parts.append(_l2_conditional_sql(config, mappings_rules, rule))
        elif rule.type == "custom_sql":
            parts.append(f"-- custom_sql\nSELECT * FROM (\n{rule.sql}) AS _custom")

    if not parts:
        return None

    union_sql = "\n\nUNION ALL\n\n".join(parts)

    return f"""\
CREATE OR REPLACE TABLE {dest} AS
{union_sql}"""
