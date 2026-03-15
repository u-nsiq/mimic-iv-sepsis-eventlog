from __future__ import annotations

"""
YAML 설정 파서 — 파이프라인 공통 기반 모듈.

YAML 설정 파일(configs/*.yaml)을 읽어 타입이 지정된 dataclass로 변환하고
필수 필드 누락·잘못된 값 등을 빌드 전에 검증한다.

YAML 기반 설계 배경:
  연구 초반에는 MIMIC-IV 전용 하드코딩 노트북으로 작업했다.
  eICU 확장이 결정되면서 데이터셋마다 별도 코드를 유지하는 것이 비효율적임이 명확해졌고,
  설정 파일만 바꿔 동일 파이프라인을 재사용할 수 있도록 YAML 주도 설계로 전환했다.

주요 설정 구조:
  source   : 원시 데이터가 있는 BigQuery 프로젝트/데이터셋
  target   : 결과 테이블을 저장할 BigQuery 프로젝트/데이터셋
  cohort   : 코호트 필터링 (패혈증 코호트 13,471 hadm_id 기준)
  event_tables : 4가지 이벤트 타입별 테이블 목록
  mappings : L1/L2 추상화 매핑 테이블 경로
  analysis : 분석 뷰의 case_id·activity_level 선택
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional

import yaml


EventType = Literal["journey", "in_out", "duration", "one_off"]


@dataclass
class SourceConfig:
    project: str
    datasets: Dict[str, str]


@dataclass
class TargetConfig:
    project: str
    dataset: str


@dataclass
class CohortConfig:
    enabled: bool = False
    bq_table: Optional[str] = None
    case_id_column: Optional[str] = None


@dataclass
class JoinConfig:
    """이벤트 테이블에 추가로 JOIN할 참조 테이블 설정."""
    table: str
    alias: str
    on: str
    module: Optional[str] = None
    dataset: Optional[str] = None


@dataclass
class EventTableConfig:
    """각 이벤트 테이블 공통 필드 + 유형별 옵션."""

    type: EventType
    table: str
    # module 또는 dataset 중 하나/둘 다/없음 가능
    module: Optional[str] = None
    dataset: Optional[str] = None
    case_id_column: str = "hadm_id"

    # journey / duration / in_out / one_off 별 필드
    start_column: Optional[str] = None
    end_column: Optional[str] = None
    in_column: Optional[str] = None
    out_column: Optional[str] = None
    timestamp_column: Optional[str] = None

    # activity 관련
    start_activity: Optional[str] = None
    end_activity: Optional[str] = None
    activity_column: Optional[str] = None
    location_column: Optional[str] = None

    # 추가 옵션
    filter: Optional[str] = None
    extra: Dict[str, str] = field(default_factory=dict)
    stay_id_column: Optional[str] = None

    # ID 컬럼 오버라이드 (기본: subject_id, hadm_id)
    # 단순 식별자이면 src.로 auto-qualify, SQL 표현식이면 그대로 통과
    subject_id_column: Optional[str] = None
    hadm_id_column: Optional[str] = None

    # 참조 테이블 JOIN (예: eICU patient 테이블에서 base timestamp 가져오기)
    join: Optional[JoinConfig] = None


@dataclass
class MetadataColumnConfig:
    source_table: str
    source_module: Optional[str] = None
    source_dataset: Optional[str] = None
    join_on: str = "subject_id"
    column: str = ""


@dataclass
class MetadataConfig:
    enabled: bool = False
    columns: Dict[str, MetadataColumnConfig] = field(default_factory=dict)


@dataclass
class MappingLevelConfig:
    enabled: bool = False
    bq_table: Optional[str] = None


@dataclass
class MappingsConfig:
    l1: MappingLevelConfig = field(default_factory=MappingLevelConfig)
    l2: MappingLevelConfig = field(default_factory=MappingLevelConfig)


@dataclass
class OutputConfig:
    baseline_table: str = "baseline_event_log"
    analysis_view: str = "analysis_event_log"


@dataclass
class AnalysisConfig:
    case_id: Literal["subject_id", "hadm_id", "stay_id"] = "hadm_id"
    activity_level: Literal["l0", "l1", "l2"] = "l1"
    include_columns: List[str] = field(default_factory=list)


@dataclass
class ELConfig:
    source: SourceConfig
    target: TargetConfig
    cohort: CohortConfig
    event_tables: Dict[EventType, List[EventTableConfig]]
    metadata: MetadataConfig
    mappings: MappingsConfig
    output: OutputConfig
    analysis: AnalysisConfig


@dataclass
class MappingRuleConfig:
    """매핑 빌더의 개별 규칙."""
    type: str
    source_table: Optional[str] = None
    source_module: Optional[str] = None
    source_dataset: Optional[str] = None
    activity_raw_column: Optional[str] = None
    reference_table: Optional[str] = None
    reference_module: Optional[str] = None
    reference_dataset: Optional[str] = None
    join_column: Optional[str] = None
    l1_column: Optional[str] = None
    mapping: Optional[Dict[str, str]] = None
    activity_raw_expr: Optional[str] = None
    l1_expr: Optional[str] = None
    conditions: Optional[List[Dict[str, str]]] = None
    default: Optional[str] = None
    sql: Optional[str] = None
    filter: Optional[str] = None
    source_tables: Optional[List[str]] = None


@dataclass
class MappingLevelRulesConfig:
    """L1 또는 L2 매핑 빌더 설정."""
    output_table: str
    rules: List[MappingRuleConfig]


@dataclass
class MappingsRulesConfig:
    """매핑 빌더 전체 설정."""
    l1: Optional[MappingLevelRulesConfig] = None
    l2: Optional[MappingLevelRulesConfig] = None


class ConfigError(Exception):
    """설정 관련 예외."""


def _require(d: Dict[str, Any], key: str, ctx: str) -> Any:
    if key not in d:
        raise ConfigError(f"Missing required key '{key}' in {ctx}")
    return d[key]


def _parse_source(raw: Dict[str, Any]) -> SourceConfig:
    source = _require(raw, "source", "root config")
    project = _require(source, "project", "source")
    datasets = _require(source, "datasets", "source")
    if not isinstance(datasets, dict):
        raise ConfigError("source.datasets must be a mapping")
    return SourceConfig(project=project, datasets=datasets)


def _parse_target(raw: Dict[str, Any]) -> TargetConfig:
    target = _require(raw, "target", "root config")
    project = _require(target, "project", "target")
    dataset = _require(target, "dataset", "target")
    return TargetConfig(project=project, dataset=dataset)


def _parse_cohort(raw: Dict[str, Any]) -> CohortConfig:
    cohort = raw.get("cohort", {}) or {}
    enabled = bool(cohort.get("enabled", False))
    if not enabled:
        return CohortConfig(enabled=False)
    bq_table = _require(cohort, "bq_table", "cohort")
    case_id_column = _require(cohort, "case_id_column", "cohort")
    return CohortConfig(enabled=True, bq_table=bq_table, case_id_column=case_id_column)


def _parse_event_table_list(event_type: EventType, items: List[Dict[str, Any]]) -> List[EventTableConfig]:
    parsed: List[EventTableConfig] = []
    for item in items:
        if not isinstance(item, dict):
            raise ConfigError(f"event_tables.{event_type} entries must be mappings")
        table = _require(item, "table", f"event_tables.{event_type}")
        case_id_column = item.get("case_id_column", "hadm_id")
        # join 설정 파싱
        join_raw = item.get("join")
        join_cfg = None
        if join_raw and isinstance(join_raw, dict):
            join_table = _require(join_raw, "table", f"event_tables.{event_type}['{table}'].join")
            join_alias = _require(join_raw, "alias", f"event_tables.{event_type}['{table}'].join")
            # YAML은 'on'을 boolean True로 파싱할 수 있으므로, 문자열 키와 boolean 키 모두 확인
            join_on = join_raw.get("on") or join_raw.get(True)
            if not join_on:
                raise ConfigError(f"Missing required key 'on' in event_tables.{event_type}['{table}'].join (YAML에서 on을 따옴표로 감싸세요: \"on\")")
            join_cfg = JoinConfig(
                table=join_table,
                alias=join_alias,
                on=join_on,
                module=join_raw.get("module"),
                dataset=join_raw.get("dataset"),
            )

        cfg = EventTableConfig(
            type=event_type,
            table=table,
            module=item.get("module"),
            dataset=item.get("dataset"),
            case_id_column=case_id_column,
            start_column=item.get("start_column"),
            end_column=item.get("end_column"),
            in_column=item.get("in_column"),
            out_column=item.get("out_column"),
            timestamp_column=item.get("timestamp_column"),
            start_activity=item.get("start_activity"),
            end_activity=item.get("end_activity"),
            activity_column=item.get("activity_column"),
            location_column=item.get("location_column"),
            filter=item.get("filter"),
            extra=item.get("extra") or {},
            stay_id_column=item.get("stay_id_column"),
            subject_id_column=item.get("subject_id_column"),
            hadm_id_column=item.get("hadm_id_column"),
            join=join_cfg,
        )

        # 유형별 필수 필드 검증
        if event_type == "journey":
            for k in ["start_column", "end_column"]:
                if getattr(cfg, k) is None:
                    raise ConfigError(f"journey table '{table}' must define {k}")
        elif event_type == "in_out":
            for k in ["in_column", "out_column", "location_column"]:
                if getattr(cfg, k) is None:
                    raise ConfigError(f"in_out table '{table}' must define {k}")
        elif event_type == "duration":
            for k in ["start_column", "end_column"]:
                if getattr(cfg, k) is None:
                    raise ConfigError(f"duration table '{table}' must define {k}")
        elif event_type == "one_off":
            if cfg.timestamp_column is None:
                raise ConfigError(f"one_off table '{table}' must define timestamp_column")

        parsed.append(cfg)
    return parsed


def _parse_event_tables(raw: Dict[str, Any]) -> Dict[EventType, List[EventTableConfig]]:
    event_tables_raw = raw.get("event_tables", {}) or {}
    result: Dict[EventType, List[EventTableConfig]] = {}
    for etype in ("journey", "in_out", "duration", "one_off"):
        items = event_tables_raw.get(etype, []) or []
        if not items:
            # 빈 유형은 조용히 스킵
            result[etype] = []
            continue
        result[etype] = _parse_event_table_list(etype, items)
    return result


def _parse_metadata(raw: Dict[str, Any]) -> MetadataConfig:
    meta_raw = raw.get("metadata", {}) or {}
    enabled = bool(meta_raw.get("enabled", False))
    if not enabled:
        return MetadataConfig(enabled=False)

    cols_raw = meta_raw.get("columns", {}) or {}
    columns: Dict[str, MetadataColumnConfig] = {}
    for name, spec in cols_raw.items():
        if not isinstance(spec, dict):
            raise ConfigError(f"metadata.columns['{name}'] must be a mapping")
        source_table = _require(spec, "source_table", f"metadata.columns['{name}']")
        join_on = spec.get("join_on", "subject_id")
        column = _require(spec, "column", f"metadata.columns['{name}']")
        columns[name] = MetadataColumnConfig(
            source_table=source_table,
            source_module=spec.get("source_module"),
            source_dataset=spec.get("source_dataset"),
            join_on=join_on,
            column=column,
        )
    return MetadataConfig(enabled=True, columns=columns)


def _parse_mappings(raw: Dict[str, Any]) -> MappingsConfig:
    mappings_raw = raw.get("mappings", {}) or {}
    l1_raw = mappings_raw.get("l1", {}) or {}
    l2_raw = mappings_raw.get("l2", {}) or {}
    l1 = MappingLevelConfig(
        enabled=bool(l1_raw.get("enabled", False)),
        bq_table=l1_raw.get("bq_table"),
    )
    l2 = MappingLevelConfig(
        enabled=bool(l2_raw.get("enabled", False)),
        bq_table=l2_raw.get("bq_table"),
    )
    return MappingsConfig(l1=l1, l2=l2)


def _parse_output(raw: Dict[str, Any]) -> OutputConfig:
    out_raw = raw.get("output", {}) or {}
    return OutputConfig(
        baseline_table=out_raw.get("baseline_table", "baseline_event_log"),
        analysis_view=out_raw.get("analysis_view", "analysis_event_log"),
    )


def _parse_analysis(raw: Dict[str, Any]) -> AnalysisConfig:
    a_raw = raw.get("analysis", {}) or {}
    case_id = a_raw.get("case_id", "hadm_id")
    if case_id not in ("subject_id", "hadm_id", "stay_id"):
        raise ConfigError("analysis.case_id must be one of 'subject_id', 'hadm_id', 'stay_id'")
    activity_level = a_raw.get("activity_level", "l1")
    if activity_level not in ("l0", "l1", "l2"):
        raise ConfigError("analysis.activity_level must be one of 'l0', 'l1', 'l2'")
    include_columns = a_raw.get("include_columns", []) or []
    if not isinstance(include_columns, list):
        raise ConfigError("analysis.include_columns must be a list")
    return AnalysisConfig(
        case_id=case_id,  # type: ignore[arg-type]
        activity_level=activity_level,  # type: ignore[arg-type]
        include_columns=include_columns,
    )


def load_config(path: str) -> ELConfig:
    """YAML 파일을 읽어 ELConfig로 변환한다."""
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    source = _parse_source(raw)
    target = _parse_target(raw)
    cohort = _parse_cohort(raw)
    event_tables = _parse_event_tables(raw)
    metadata = _parse_metadata(raw)
    mappings = _parse_mappings(raw)
    output = _parse_output(raw)
    analysis = _parse_analysis(raw)

    return ELConfig(
        source=source,
        target=target,
        cohort=cohort,
        event_tables=event_tables,
        metadata=metadata,
        mappings=mappings,
        output=output,
        analysis=analysis,
    )


def _validate_mapping_rule(rule: MappingRuleConfig, level: str, idx: int) -> None:
    """매핑 규칙의 필수 필드를 검증한다."""
    ctx = f"{level}.rules[{idx}] (type={rule.type})"

    if level == "l1":
        if rule.type == "reference_join":
            for k in ["source_table", "activity_raw_column", "reference_table", "join_column", "l1_column"]:
                if getattr(rule, k) is None:
                    raise ConfigError(f"{ctx}: reference_join 규칙에 '{k}'가 필요합니다")
        elif rule.type == "dict":
            if not rule.source_table:
                raise ConfigError(f"{ctx}: dict 규칙에 'source_table'이 필요합니다")
            if not rule.mapping:
                raise ConfigError(f"{ctx}: dict 규칙에 'mapping'이 필요합니다")
        elif rule.type == "expression":
            for k in ["source_table", "activity_raw_expr", "l1_expr"]:
                if getattr(rule, k) is None:
                    raise ConfigError(f"{ctx}: expression 규칙에 '{k}'가 필요합니다")
        elif rule.type == "custom_sql":
            if not rule.sql:
                raise ConfigError(f"{ctx}: custom_sql 규칙에 'sql'이 필요합니다")
        else:
            raise ConfigError(f"{ctx}: 알 수 없는 L1 규칙 타입 '{rule.type}'")

    elif level == "l2":
        if rule.type == "dict":
            if not rule.mapping:
                raise ConfigError(f"{ctx}: dict 규칙에 'mapping'이 필요합니다")
        elif rule.type == "conditional":
            if not rule.conditions:
                raise ConfigError(f"{ctx}: conditional 규칙에 'conditions'가 필요합니다")
            for i, cond in enumerate(rule.conditions):
                if "when" not in cond or "value" not in cond:
                    raise ConfigError(f"{ctx}: conditions[{i}]에 'when'과 'value'가 필요합니다")
        elif rule.type == "custom_sql":
            if not rule.sql:
                raise ConfigError(f"{ctx}: custom_sql 규칙에 'sql'이 필요합니다")
        else:
            raise ConfigError(f"{ctx}: 알 수 없는 L2 규칙 타입 '{rule.type}'")


def _parse_mapping_level(raw: Dict[str, Any], level: str) -> Optional[MappingLevelRulesConfig]:
    """L1 또는 L2 매핑 레벨 설정을 파싱한다."""
    if not raw:
        return None

    output_table = _require(raw, "output_table", f"mappings.{level}")
    rules_raw = _require(raw, "rules", f"mappings.{level}")
    if not isinstance(rules_raw, list):
        raise ConfigError(f"mappings.{level}.rules must be a list")

    rules: List[MappingRuleConfig] = []
    for idx, item in enumerate(rules_raw):
        if not isinstance(item, dict):
            raise ConfigError(f"mappings.{level}.rules[{idx}] must be a mapping")
        rule_type = _require(item, "type", f"mappings.{level}.rules[{idx}]")
        rule = MappingRuleConfig(
            type=rule_type,
            source_table=item.get("source_table"),
            source_module=item.get("source_module"),
            source_dataset=item.get("source_dataset"),
            activity_raw_column=item.get("activity_raw_column"),
            reference_table=item.get("reference_table"),
            reference_module=item.get("reference_module"),
            reference_dataset=item.get("reference_dataset"),
            join_column=item.get("join_column"),
            l1_column=item.get("l1_column"),
            mapping=item.get("mapping"),
            activity_raw_expr=item.get("activity_raw_expr"),
            l1_expr=item.get("l1_expr"),
            conditions=item.get("conditions"),
            default=item.get("default"),
            sql=item.get("sql"),
            filter=item.get("filter"),
            source_tables=item.get("source_tables"),
        )
        _validate_mapping_rule(rule, level, idx)
        rules.append(rule)

    return MappingLevelRulesConfig(output_table=output_table, rules=rules)


def load_mappings_config(path: str) -> MappingsRulesConfig:
    """매핑 빌더용 YAML 파일을 읽어 MappingsRulesConfig로 변환한다."""
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    l1 = _parse_mapping_level(raw.get("l1"), "l1")
    l2 = _parse_mapping_level(raw.get("l2"), "l2")

    if l1 is None and l2 is None:
        raise ConfigError("매핑 설정에 l1 또는 l2 중 하나 이상이 필요합니다")

    return MappingsRulesConfig(l1=l1, l2=l2)

