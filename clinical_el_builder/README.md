# clinical_el_builder

> BigQuery-based event log builder for MIMIC-IV and eICU-CRD

MIMIC-IV와 eICU-CRD의 임상 테이블을 프로세스 마이닝용 이벤트 로그로 변환하는 BigQuery 기반 파이프라인이다.
YAML 설정을 통해 코호트 필터링, 이벤트 추출, L0→L1→L2 추상화, 품질 검증 단계를 순차적으로 실행한다.

> 학부연구 과정에서 개발한 이벤트 로그 구축 방법론을 재사용 가능한 형태로 정리한 최종 산출물이다.
> 연구 배경과 설계 맥락은 상위 Repository의 [`README.md`](../README.md)와 [`notebooks/`](../notebooks/)를 참고하면 된다.

---

## Pipeline Overview

```
MIMIC-IV / eICU-CRD (BigQuery)
        │
        ▼
[Step 1] build-mappings   ─── L1/L2 activity 매핑 테이블 생성
        │                      itemid → d_items.category → 커스텀 그룹
        ▼
[Step 2] build-baseline   ─── 10개 소스 테이블 × 4가지 이벤트 타입
        │                      → UNION ALL → Baseline Event Log (BigQuery 테이블)
        ▼
[Step 3] build-analysis   ─── 코호트 필터 + 추상화 레벨 선택
        │                      → Analysis View
        ▼
[Step 4] validate         ─── Suriadi Imperfection Pattern 품질 검증
```

---

## Event Types

| Type | Record-to-Event | Description | Example tables |
|------|:---:|---|---|
| `journey` | 1 → 2 | 서로 다른 start/end activity | `admissions` |
| `in_out` | 1 → 2 | 위치 기반 이동 이벤트 | `transfers` |
| `duration` | 1 → 2 | 동일 activity의 start/complete | `inputevents`, `procedureevents` |
| `one_off` | 1 → 1 | 단일 시점 이벤트 | `labevents`, `emar`, `chartevents` |

---

## Abstraction Levels

| Level | Description | Example (chartevents) | Distinct labels |
|-------|------|-------------------|:---------:|
| L0 | Raw 식별자 | `220045` (itemid) | 1,864 |
| L1 | 임상 카테고리 | `Cardiovascular` (d_items.category) | 40 |
| L2 | 연구 그룹 | `Vitals` (커스텀) | 11 |

---

## Prerequisites

- Python 3.x
- Google Cloud project with BigQuery enabled
- Google Cloud credentials (Application Default Credentials 또는 서비스 계정 키)
- PhysioNet CITI 교육 이수 및 MIMIC-IV / eICU-CRD 접근 승인
- 소스 데이터셋이 BigQuery에 로드되어 있어야 함

---

## Installation

```bash
pip install google-cloud-bigquery pyyaml
```

GCP 인증 설정:

```bash
gcloud auth application-default login
```

---

## Configuration

파이프라인 설정은 `configs/` 아래 세 파일로 관리된다:

| 파일 | 용도 |
|------|------|
| `mimic_iv_complete.yaml` | MIMIC-IV 전체 설정 (L1/L2 매핑 포함) |
| `eicu_config.yaml` | eICU 설정 (L0 only, 매핑 없음) |
| `mappings_config.yaml` | L1/L2 매핑 규칙 정의 |

최소 수정 항목 (`mimic_iv_complete.yaml`):

```yaml
target:
  project: "YOUR_PROJECT_ID"
  dataset: "event_logs"
```

eICU 사용 시 `eicu_config.yaml`에서 동일하게 project ID를 수정한다.
L1/L2 매핑 규칙을 변경하려면 `mappings_config.yaml`을 수정한다.

---

## Usage

| Command | Description | Output |
|---------|-------------|--------|
| `build-mappings` | L1/L2 activity mapping tables 생성 | BigQuery mapping tables |
| `build-baseline` | 소스 테이블 → Baseline Event Log 통합 | BigQuery table |
| `build-analysis` | 코호트 필터 + 추상화 레벨 적용 view 생성 | BigQuery view |
| `build-all` | `build-baseline` + `build-analysis` 순차 실행 | — |
| `validate` | Suriadi Imperfection Patterns 기반 품질 검증 | 콘솔 출력 |

```bash
# Step 1: L1/L2 activity 매핑 테이블 생성 (최초 1회)
python run.py build-mappings \
  --config configs/mimic_iv_complete.yaml \
  --mappings-config configs/mappings_config.yaml

# Step 2: Baseline Event Log 구축
python run.py build-baseline --config configs/mimic_iv_complete.yaml

# Step 3: Analysis View 생성
python run.py build-analysis --config configs/mimic_iv_complete.yaml

# Step 2~3 한 번에 실행
python run.py build-all --config configs/mimic_iv_complete.yaml

# Step 4: 데이터 품질 검증
python run.py validate --config configs/mimic_iv_complete.yaml

# SQL 미리보기 (실행 없음)
python run.py build-baseline --config configs/mimic_iv_complete.yaml --dry-run
```

---

## Outputs

파이프라인 실행 시 BigQuery에 다음 산출물이 생성된다:

- **L1/L2 mapping tables**: 각 임상 테이블의 activity 매핑 테이블
- **Baseline event log table**: 10개 소스 테이블을 단일 스키마로 통합한 전체 이벤트 로그
- **Analysis view**: 코호트 필터 및 추상화 레벨이 적용된 분석용 view
- **Validation results**: Suriadi 패턴별 검증 결과 (콘솔 출력)

---

## Project Structure

```
clinical_el_builder/
├── run.py                      # CLI 진입점 (5개 서브커맨드)
├── bigquery_utils.py           # BigQuery 클라이언트 헬퍼
├── el_builder/
│   ├── config_parser.py        # YAML 설정 파싱 및 데이터클래스
│   ├── event_sql.py            # 4가지 이벤트 타입별 SQL 생성
│   ├── step1_mappings.py       # L1/L2 매핑 테이블 빌더
│   ├── step2_baseline.py       # Baseline Event Log 빌더
│   ├── step3_analysis.py       # Analysis View 빌더
│   └── step4_validator.py      # Suriadi 패턴 검증기
└── configs/
    ├── mimic_iv_complete.yaml  # MIMIC-IV 설정 (L1/L2 매핑 포함)
    ├── eicu_config.yaml        # eICU 설정 (L0 only)
    └── mappings_config.yaml    # L1/L2 매핑 규칙
```
