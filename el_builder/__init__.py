"""
Clinical Event Log Builder 패키지.

MIMIC-IV / eICU 임상 데이터로부터 프로세스 마이닝용 이벤트 로그를 생성하는 파이프라인.

파이프라인 구성 (실행 순서):
  config_parser.py   : YAML 설정 파싱 (공통 기반)
  event_sql.py       : 이벤트 타입별 BigQuery SQL 생성 (4가지 타입 구현)
  step1_mappings.py  : L1/L2 추상화 매핑 테이블 생성
  step2_baseline.py  : Baseline Event Log 구축 (10개 테이블 UNION)
  step3_analysis.py  : Analysis Event Log 뷰 생성
  step4_validator.py : 데이터 품질 검증 (Suriadi Imperfection Patterns)

실행 진입점: run.py
  python run.py build-mappings  --config configs/mimic_iv_complete.yaml --mappings-config configs/mappings_config.yaml
  python run.py build-baseline  --config configs/mimic_iv_complete.yaml
  python run.py build-analysis  --config configs/mimic_iv_complete.yaml
  python run.py validate        --config configs/mimic_iv_complete.yaml
"""

