# Design Decisions

이 문서는 이벤트 로그 설계 과정에서 확정한 주요 결정을 정리한다. 각 결정에 대해 선택 이유와 대안 비교에 집중하며, 검증 수치와 코드는 관련 노트북에 분리되어 있다.

---

## Core Decisions

### 1. Case Notion: `hadm_id`

**Decision**
프로세스 마이닝의 Case ID로 `stay_id`(ICU 입원 건) 대신 `hadm_id`(병원 입원 건)를 채택했다.

**Alternatives considered**
`stay_id`는 ICU 체류 단위로, 하나의 입원 중 ICU를 여러 번 오가는 경우 케이스가 분절된다. 응급실, 일반 병동, ICU, 퇴원/사망까지 이어지는 전체 환자 여정을 단일 케이스로 추적할 수 없다.

**Rationale**
이 연구의 분석 대상은 입원 단위의 전체 치료 프로세스다. `hadm_id` 단위가 그 범위에 부합하며, 응급실 입원부터 퇴원 또는 사망까지의 연속된 흐름을 하나의 케이스로 포착한다.

**Impact**
`stay_id`는 폐기하지 않고 이벤트 attribute로 보존했다. 추후 ICU 체류 단위 분석이 필요할 경우 필터링 기준으로 활용할 수 있다.

---

### 2. Cohort Definition: ICD-10 Code-based Sepsis

**Decision**
패혈증 코호트 기준을 Sepsis-3 임상 정의(SOFA 점수 기반)에서 ICD-10 진단 코드 기반으로 전환했다. A40/A41을 포함 기준으로 하고, 산후기 패혈증(O85)과 비감염성 SIRS(R65.10/R65.11)를 제외해 최종 13,471 admissions / 11,081 patients 코호트를 구성했다.

**Alternatives considered**
Sepsis-3는 감염 의심(항생제 + 배양검사 동시 존재) + SOFA 점수 2점 이상 증가로 정의된다. 임상적 엄밀성은 높지만 SOFA 구성요소는 여러 임상 지표를 조합해 계산해야 하며, 지표 누락 시 일관된 산출이 불가능하다.

**Rationale**
MIMIC-IV의 SOFA 구성요소 일부는 특정 입원 건에서 누락되어 있어 재현성이 낮았다. ICD-10 코드는 의사가 내린 임상 판단의 공식 기록으로 커버리지가 완전하고 정의가 명확하다. MIMIC-IV에 ICD-9와 ICD-10 코드가 혼재하므로 ICD-10 기준으로 통일했다.

**Impact**
코호트 범위가 Sepsis-3 기준(36,248명)과 달라졌다. 두 기준은 보수/관대의 차이가 아닌 다른 축의 정의다. 중증 패혈증 하위 그룹(R65.20/R65.21, 7,336건)은 attribute로 보존해 후속 분석에서 활용 가능하도록 했다. 상세 필터 규칙은 `notebooks/01_cohort.ipynb` 참고.

---

### 3. Abstraction Levels: L0 / L1 / L2

**Decision**
이벤트 activity를 L0(원본 식별자), L1(임상 카테고리), L2(연구 그룹)의 3단계로 추상화했다.

**Alternatives considered**
L0를 그대로 쓰면 chartevents 단독으로 1,864가지 itemid가 존재해 프로세스 맵이 해석 불가능하다. 반대로 단일 레벨 추상화는 임상 세부 정보를 과도하게 손실한다.

**Rationale**
프로세스 마이닝에서 activity의 cardinality는 분석 가능성에 직결된다. 단계별 추상화를 두어 분석 목적에 따라 레벨을 선택할 수 있게 했다. 각 테이블의 native 분류 체계(d_items.category, d_labitems 등)를 L1 기준으로 우선 활용해 외부 온톨로지 의존 없이 100% 커버리지를 확보했다.

**Impact**
L0(76M 건) → L2 + 5분 윈도우 병합 적용 시 10.5M 건(−86.2%)으로 압축됐다. 분석 단계에서 L1 또는 L2를 선택해 cardinality와 세부도 사이의 트레이드오프를 조정할 수 있다.

---

### 4. Medication Source: `emar`

**Decision**
약물 투여 이벤트의 소스로 `prescriptions`(처방 기록) 대신 `emar`(실제 투약 기록)를 채택했다.

**Alternatives considered**
`prescriptions`는 의사가 처방한 기록이며, 실제 투약 여부와 무관하게 존재한다. 처방이 취소되거나 환자 상태 변화로 투약되지 않은 경우도 포함된다.

**Rationale**
프로세스 이벤트는 실제 발생한 행위를 기록해야 한다. 처방 기록보다 실제 투약 기록이 임상 프로세스 이벤트에 더 직접 대응한다. `emar`의 "not given" 레코드는 제외했다.

**Impact**
`emar` → `pharmacy_id` → `prescriptions` join → NDC → dose form group 매핑 체계를 구성했다. NDC 누락, 처방 불일치 등으로 9.8%(95,051건)의 매핑 손실이 발생했다. 이는 데이터 품질 문제가 아니라 emar 채택에 따른 구조적 제약이다.

---

### 5. Event Window Policy: 삭제 방식 채택

**Decision**
입원 기간(Master Window)을 벗어나는 이벤트를 시간 보정(capping)하지 않고 제거하는 방식을 채택했다.

**Alternatives considered**
ICU 체류 기간(Child Window)이 입원 기간 밖으로 벗어나는 케이스(2,340건)에 대해, `intime`/`outtime`을 입원 범위 안으로 당기는 capping 방식을 검토했다.

**Rationale**
시간 보정은 원본 데이터의 시간 구조를 인위적으로 변형한다. 임상 이벤트의 실제 발생 시점을 수정하면 분석 결과의 신뢰성이 저하된다. 범위 이탈 이벤트의 규모가 전체 대비 작고, 제거 시 정보 손실보다 해석 오염 위험이 더 크다고 판단했다.

**예외**: ICU `outtime` NULL 7건은 제거 시 해당 환자의 연쇄 삭제(136,692건)가 발생하므로, 이 케이스에 한해 `TRUE_END`로 보간 처리했다.

**Impact**
필터링 로직이 단순화되어 재현 가능성이 높아졌다. `TRUE_END = COALESCE(deathtime, dischtime)` 정의 하에, 논리 오류가 있는 hadm_id 1건(admittime > TRUE_END)은 코호트에서 제외했다.

---

### 6. Infrastructure: BigQuery Migration

**Decision**
로컬 SQLite 기반 워크플로를 Google BigQuery로 이전했다.

**Alternatives considered**
로컬 SQLite에서 계속 분석하거나, PostgreSQL 같은 다른 로컬 RDBMS로 전환하는 방법이 있었다.

**Rationale**
연구 후반에 raw 이벤트 규모가 111M 건을 초과하면서 로컬 환경의 처리 한계에 도달했다. 총 151GB에 달하는 DB 파일 4개를 유지하는 것도 비효율적이었다. BigQuery는 PhysioNet 공식 데이터셋(`physionet-data`)에 직접 접근 가능해 CSV 다운로드 없이 원본 테이블을 쿼리할 수 있으며, 서버리스 분산 처리로 동일 쿼리 속도가 크게 개선됐다.

**Impact**
`clinical_el_builder`의 BigQuery 기반 설계가 이 결정에서 비롯됐다. eICU-CRD 확장도 BigQuery 마이그레이션 이후에 가능했다.

---

### 7. Multi-Dataset Expansion: eICU-CRD

**Decision**
MIMIC-IV 분석이 안정화된 시점에 파이프라인의 범용성을 검증하기 위해 eICU-CRD를 두 번째 데이터셋으로 채택했다. 최종 코호트: 15,731 ICU stays / 13,420 patients.

**Alternatives considered**
후보로 eICU-CRD(미국 다기관, ~200K 입원)와 AmsterdamUMCdb(네덜란드, 23K 입원) 두 가지를 검토했다. AmsterdamUMCdb는 접근 시 별도 연구계획서 제출이 필요해 PhysioNet 승인이 이미 있는 eICU-CRD를 선택했다.

**Rationale**
목적은 특정 데이터셋 전용 파이프라인이 아닌 범용 방법론의 검증이었다. eICU-CRD는 미국 전역 208개 병원 335개 ICU의 다기관 데이터로 MIMIC-IV(단일 병원)와 대조적인 특성을 가지며, 논문에서 광범위하게 사용된 공신력 있는 공개 데이터셋이다. 코호트는 ICD-9 코드 기반(`038.x`, `995.91/92`, `785.52`)으로 정의했다.

**Impact**
eICU-CRD의 상대 오프셋 타임스탬프 구조로 인해 별도 변환 전략이 필요했다(→ [eICU Timestamp Synchronization](#eicu-timestamp-synchronization)). `clinical_el_builder`의 `eicu_config.yaml`은 이 확장 작업의 산출물이다.

---

### 8. Raw EL vs. Baseline EL

**Decision**
이벤트 로그 구축을 Raw EL과 Baseline EL의 두 단계로 분리했다.

**Rationale**
품질 검증 전 상태(Raw EL)를 감사 기록으로 보존하면서, 검증 기반 정제를 적용한 분석용 산출물(Baseline EL)을 별도로 유지해야 했다. 단일 버전만 유지하면 정제 과정의 재현 가능성이 떨어진다.

**Impact**
- Raw EL(111M 건): 코호트 필터링 + 기본 타임스탬프 경계만 적용. 모든 소스 테이블 포함.
- Baseline EL(~21M 건): Imperfection Pattern 기반 정제 적용. chartevents 제외 및 P2/P6/P7/P8/P9 처리 반영.

두 버전의 분리로 Raw EL 기준 재검증과 Baseline EL 기준 분석을 독립적으로 수행할 수 있다.

---

## Supporting Decisions

### Per-table Mapping Strategy

**Decision**
외부 온톨로지(OMOP CDM, RxNorm, ATC 등) 대신 각 테이블의 native 분류 체계를 L1 매핑 기준으로 채택했다.

**Rationale**
외부 온톨로지 기반 매핑은 커버리지 불완전으로 데이터 손실이 발생했다. native 분류 체계(d_items.category, d_labitems 등)를 우선 활용하면 외부 의존 없이 100% 커버리지를 확보할 수 있다. 약물(emar)은 예외적으로 NDC → dose form group 매핑을 적용했다. 약물 성분(ATC) 기반은 1:N 매핑 문제가 발생했으나, 제형(경구/주사/흡입 등) 기반 분류는 1:1 매핑이 가능했다.

**Impact**
매핑 손실 없이 10개 소스 테이블 전체를 커버했다. 약물은 제형 기반 분류로 9.8% 구조적 손실이 발생했다(→ [Medication Source](#4-medication-source-emar) 참조).

---

### transfers Deduplication

**Decision**
`transfers` 테이블에서 `admissions`의 입퇴원 이벤트와 일치하는 레코드, `icustays` 체류 기간과 겹치는 레코드, `eventtype = 'admit'/'discharge'` 레코드를 제거했다.

**Rationale**
이들은 다른 테이블에서 이미 포함하는 이벤트다. 중복 포함 시 동일 의미의 이벤트가 케이스 내에 두 번 나타나 프로세스 모델을 왜곡한다.

**Impact**
병동 이동 이벤트만 transfers에서 추출되어 다른 소스 테이블과의 의미 충돌을 방지했다.

---

### Duration Event Timestamp Selection

**Decision**
시작·종료 시점이 모두 있는 이벤트(duration 타입)에서 활동 완료 시점을 프로세스 마이닝용 타임스탬프로 선택했다(`inputevents`/`procedureevents` → `endtime`, `prescriptions` → `stoptime`).

**Rationale**
완료 시점이 임상적으로 의미 있는 전환점이다(수액 주입 완료, 처치 종료, 약물 중단). 시작 시점은 준비 단계에 가까워 프로세스 전이의 기준으로 덜 적합하다.

**Impact**
이벤트 순서 안정성이 높아지고, 완료 기반 프로세스 모델이 임상 흐름을 더 정확히 반영한다.

---

### Attribute Column Design

**Decision**
핵심 스키마(`hadm_id`, `event_timestamp`, `activity`) 외에 `stay_id`, `gender`, `age`, `resource` 컬럼을 보조 attribute로 포함했다.

**Rationale**
ICU 체류 단위 하위 분석(`stay_id`), 인구통계 기반 분석(`gender`, `age`), Object-Centric PM에서의 의료진 객체 연결(`resource`)을 별도 전처리 없이 분석 시점에서 바로 활용하기 위함이다.

**Impact**
`resource` 채워진 비율은 테이블별로 크게 다르다(`admissions`·`inputevents`·`outputevents` 100%, `chartevents` 96.9%, `procedureevents` 84.9%, `emar` 14.75%, `labevents` 0.02%). NULL 값은 보존해 데이터 충실도를 유지했다.

---

### eICU Timestamp Synchronization

**Decision**
eICU-CRD의 상대 오프셋(분 단위)을 절대 DATETIME으로 변환할 때, 병원 퇴원 시점(TFixed)을 고정점으로 잡고 역산하는 방식을 채택했다.

**Rationale**
입원 시점을 기준점으로 삼으면 다중 ICU 방문 간 오프셋 오차가 누적된다. 퇴원 시점은 모든 방문이 공유하는 단일 기준점이므로, 역산하면 다중 방문 간 순서가 일관되게 유지된다.

**Impact**
변환 정합성 99.9% 검증. BigQuery의 `eicu_ts_version` 데이터셋에 31개 테이블 전체 변환 결과를 저장했다.

---

## Validation-informed Adjustments

품질 검증(Suriadi et al. 11 Imperfection Patterns) 결과가 직접 설계 변경으로 이어진 항목들이다. 검증 프레임워크 전체와 패턴별 정량 결과는 `notebooks/04_quality.ipynb`를 참고.

### chartevents Exclusion from Baseline

P1 검증에서 chartevents 배치 이벤트가 87.0%로 압도적인 비중을 차지했다. NCP(Nursing Care Plan) 루틴이 배치 상위를 독점하고, 간호사 교대 직후(08:00, 20:00)에 입력 피크가 집중됐다. chartevents의 타임스탬프가 임상 행위 시점이 아닌 차트 입력 시점을 반영한다는 결론에 따라 Baseline EL에서 제외했다. Raw EL에는 감사 목적으로 보존된다.

### Zombie Event Deletion

P2 검증에서 사망 후 이벤트의 91.8%가 사망 당일에 집중됐다. 이는 사후 행정 정리(차트 마감, ICU 퇴실 처리)로 판단해 유효 데이터로 인정했다. 사망 24시간 이후 이벤트(0.48%)만 노이즈로 분류해 Baseline EL에서 제외했다.

### ED Linkage Recovery

P5 검증에서 `hadm_id` 결측 레코드 9.09M 건이 발견됐다. ED 시스템이 병원 입원 결정 이전에 독립적으로 운영되어, 응급실 체류 중 수집된 일부 데이터가 `hadm_id`와 소급 연결된다는 구조적 특성이 원인이었다. 4단계 분류(ED Link 복구 → In-Stay 복구 → ED 전용 제외 → 범위 외 제외)를 통해 1.6M 건을 복구하고, 나머지 7.5M 건은 코호트 정의에 해당하지 않는 데이터(외래·귀가)로 정당 제외했다.

### Cross-table Precedence (P7)

P7 검증에서 chartevents와 labevents 사이에 동일 L2 레이블이 대규모로 동시 존재하는 것을 확인했다. labevents를 마스터 소스로 지정하고 chartevents의 해당 레코드를 중복으로 처리했다.

### Label Standardization (P8/P9)

P8에서 숫자 코드가 혼입된 레이블(예: `start_5-Imaging`), P9에서 대소문자 불일치 레이블(예: `Blood`/`BLOOD`)이 탐지됐다. 각각 숫자 접두사 제거 및 대문자 통일로 정제했다.

---

## Related Documents

| 문서 | 역할 |
|------|------|
| `design_decisions.md` (이 문서) | **왜** — 설계 결정의 근거와 대안 비교 |
| [`notebooks/`](../notebooks/) | **어떻게** — 분석 과정, 코드, 검증 수치 |
| [`clinical_el_builder/README.md`](../clinical_el_builder/README.md) | **실행** — 재현 가능한 파이프라인 산출물 |
