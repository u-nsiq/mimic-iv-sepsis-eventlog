from __future__ import annotations

"""
파이프라인 스텝 3: Analysis Event Log 뷰 생성.

Baseline EL(step2)에서 실제 프로세스 마이닝 분석용 뷰를 생성한다.
YAML 설정만 바꿔서 분석 단위(case_id)와 추상화 레벨(activity_level)을 조정할 수 있다.

  case_id 선택:
    - hadm_id: 입원 단위 분석 (MIMIC-IV 기본값 — 전체 여정 포착 목적)
    - stay_id: ICU 재실 단위 분석 (eICU 기본값)
    - subject_id: 환자 단위 분석

  activity_level 선택:
    - l0: 원본 activity (itemid 등 raw 값)
    - l1: 1차 추상화 (d_items.category 등)
    - l2: 2차 추상화 (커스텀 그룹, 최저 카디널리티)

실행: python run.py build-analysis --config configs/mimic_iv_complete.yaml
"""

from .config_parser import ELConfig


def build_analysis_sql(config: ELConfig) -> str:
    baseline_table = f"`{config.target.project}.{config.target.dataset}.{config.output.baseline_table}`"
    analysis_view = f"`{config.target.project}.{config.target.dataset}.{config.output.analysis_view}`"

    case_id_col = config.analysis.case_id
    activity_level = config.analysis.activity_level
    activity_col = f"activity_{activity_level}"

    # 기본 컬럼
    select_cols = [
        f"{case_id_col} AS case_id",
        "event_timestamp",
        f"{activity_col} AS activity",
        "source_table",
        "event_type",
    ]

    # 추가 컬럼
    for col in config.analysis.include_columns:
        select_cols.append(col)

    select_sql = ",\n  ".join(select_cols)

    sql = f"""
CREATE OR REPLACE VIEW {analysis_view} AS
SELECT
  {select_sql}
FROM {baseline_table}
""".strip()

    return sql

