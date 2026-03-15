# clinical_el_builder

MIMIC-IV / eICU 임상 데이터로부터 프로세스 마이닝용 이벤트 로그를 자동 생성하는 BigQuery 파이프라인.
YAML 설정 하나로 코호트 필터링, 10개 테이블 이벤트 추출, L0→L1→L2 추상화, 품질 검증까지 실행한다.

> 이 도구는 7개월간의 학부연구생 연구 활동 마지막에, 파편화된 Jupyter 노트북 코드를 재현 가능한 형태로 정리한 **인수인계 도구**다.
> 연구 과정과 설계 배경은 상위 레포지터리 [`README.md`](../README.md)와 [`notebooks/`](../notebooks/)를 참고.

---

## 파이프라인 구조

```
MIMIC-IV (BigQuery)
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

### 4가지 이벤트 타입

| 타입 | 행 → 이벤트 | 설명 | 예시 테이블 |
|------|:---:|---|---|
| `journey` | 1 → 2 | 서로 다른 start/end activity | `admissions` |
| `in_out` | 1 → 2 | 위치 기반 이동 이벤트 | `transfers` |
| `duration` | 1 → 2 | 동일 activity의 start/complete | `inputevents`, `procedureevents` |
| `one_off` | 1 → 1 | 단일 시점 이벤트 | `labevents`, `emar`, `chartevents` |

### L0 → L1 → L2 추상화

| 레벨 | 설명 | 예시 (chartevents) | 카디널리티 |
|------|------|-------------------|:---------:|
| L0 | Raw 식별자 | `220045` (itemid) | 1,864종 |
| L1 | 임상 카테고리 | `Cardiovascular` (d_items.category) | 40종 |
| L2 | 연구 그룹 | `Vitals` (커스텀) | 11종 |

---

## 설치

```bash
pip install google-cloud-bigquery pyyaml
```

PhysioNet에서 MIMIC-IV BigQuery 접근 권한이 있는 Google Cloud 프로젝트 필요.

---

## 설정

`configs/mimic_iv_complete.yaml`에서 GCP 프로젝트 ID를 설정한다:

```yaml
target:
  project: "YOUR_PROJECT_ID"
  dataset: "event_logs"
```

eICU 사용 시: `configs/eicu_config.yaml` (L0 only, 매핑 없음)

L1/L2 매핑 규칙 수정: `configs/mappings_config.yaml`

---

## 실행

```bash
# Step 1: L1/L2 activity 매핑 테이블 생성 (최초 1회)
python run.py build-mappings \
  --config configs/mimic_iv_complete.yaml \
  --mappings-config configs/mappings_config.yaml

# Step 2: Baseline Event Log 구축
python run.py build-baseline --config configs/mimic_iv_complete.yaml

# Step 3: Analysis View 생성
python run.py build-analysis --config configs/mimic_iv_complete.yaml

# Step 4: 데이터 품질 검증
python run.py validate --config configs/mimic_iv_complete.yaml

# SQL 미리보기 (실행 없음)
python run.py build-baseline --config configs/mimic_iv_complete.yaml --dry-run
```

---

## 구조

```
clinical_el_builder/
├── run.py                      # CLI 진입점 (4개 서브커맨드)
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
