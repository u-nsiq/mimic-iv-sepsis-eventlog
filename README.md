# 🏥 MIMIC-IV Sepsis Event Log

<div align="center">
  <img src="https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=Python&logoColor=white"/>
  <img src="https://img.shields.io/badge/Google%20BigQuery-4285F4?style=flat-square&logo=Google%20BigQuery&logoColor=white"/>
  <img src="https://img.shields.io/badge/Process%20Mining-FF6F00?style=flat-square&color=FF6F00"/>
  <img src="https://img.shields.io/badge/MIMIC--IV-v3.1-green?style=flat-square"/>
</div>

<br/>

> **광운대학교 Data Analytics Lab 학부연구생 연구 활동 기록 (2025.07 – 2026.02)**

---

## 📖 이 레포지터리에 대해

이 레포는 **도구 레포가 아닌 연구 기록 레포**다.

7개월간(21회 미팅) MIMIC-IV 임상 데이터를 다루며 패혈증 환자의 케어 프로세스를 분석하기 위한 이벤트 로그를 설계·구축한 과정을 담는다. 연구 목표는 Traditional PM과 Object-Centric PM(OCPM) 관점의 비교 분석이었고, 7개월은 그 전 단계인 **데이터 구축**에 전부 쓰였다.

- **[`notebooks/`](./notebooks/)** — 연구 단위별 핵심 코드와 결과
- **[`docs/`](./docs/)** — 주요 설계 결정 기록
- **[`clinical_el_builder/`](./clinical_el_builder/)** — 마지막에 노트북 코드를 재현 가능한 형태로 정리한 인수인계 파이프라인

---

## 👤 연구자 / 🔬 랩

<!-- TODO: 본인 GitHub 프로필 링크 등 추가 -->

| | |
|--|--|
| **학교 / 학부** | 광운대학교 소프트웨어학부 |
| **역할** | 학부연구생 |
| **기간** | 2025.07 – 2026.02 |
| **담당** | MIMIC-IV 이벤트 로그 파이프라인 설계·구현, 데이터 품질 검증 |

| | |
|--|--|
| **랩** | Data Analytics Lab, School of Information Convergence, Kwangwoon University |
| **참여** | 2인 (MIMIC-IV 담당 + eICU 보조) |

---

## 📊 핵심 수치

| 항목 | 수치 |
|------|-----:|
| 패혈증 코호트 (MIMIC-IV) | **13,471건** 입원 / **11,081명** 환자 |
| 이벤트 소스 테이블 | 10개 |
| Raw EL (BigQuery) | 111,208,580건 |
| Baseline EL — L0 (필터링 후) | **76,062,216건** |
| L1 추상화 후 (5분 윈도우) | 13,617,405건 **(−82.1%)** |
| L2 추상화 후 (5분 윈도우) | 10,483,531건 **(−86.2%)** |
| chartevents 카디널리티 L0→L2 | 1,864종 → 11종 **(−99.4%)** |
| emar 매핑 손실률 | 9.8% (95,051건) |
| 품질 검증 (Suriadi 11패턴) | **P2 / P3 / P6 / P7 — PASS** |
| 비교 데이터셋 (eICU) | 15,731 stays / 13,420명 |

---

## 🔬 Part 1. 연구 과정

### 주요 성과

**코호트 & 이벤트 로그 설계**
- ICD-10 A40.–/A41.– 기반 성인 패혈증 코호트 13,471건 구축. 응급실 입원부터 퇴원/사망까지 전체 여정을 포착하기 위해 `stay_id` 대신 `hadm_id`를 Case ID로 확정했다.
- 10개 소스 테이블의 이벤트를 4가지 타입(journey / in_out / duration / one_off)으로 분류해 통합 스키마로 변환했다.
- 고카디널리티 raw 식별자(chartevents itemid 1,864종)를 L0 → L1 → L2 계층적 추상화로 분석 가능한 수준으로 줄였다.

**데이터 품질 검증 (Suriadi 11 Imperfection Patterns)**
- 임상 데이터 특유의 결함 패턴을 학술 프레임워크 기반으로 체계적으로 검증했다.
- P1(배치 입력): 전체 이벤트의 97.5%가 배치로 입력됨을 확인 → 5분 윈도우 병합으로 처리
- P7(교차 테이블 중복): chartevents ↔ labevents 644,760건 중복 → labevents를 Master 소스로 확정

### 🚧 주요 문제 해결

<!-- TODO: 블로그 포스트 작성 후 링크 추가 -->

> 아래 내용은 각 블로그 포스트에서 상세히 다룬다.

- **타임스탬프 버그** — 이벤트 병합 로직에서 timestamp를 무시하는 버그로 수천만 건 이벤트가 누락됨. 교수님의 "환자 수랑 이벤트 수가 비슷한 건 이상하지 않냐"는 한 마디로 발견.
- **SQLite → BigQuery 마이그레이션** — 로컬 SQLite DB가 합계 151GB에 달해 협업·재현성의 한계에 도달. BigQuery로 전환 후 수 시간 걸리던 쿼리가 수십 초로 단축.
- **NDC 결측치 복구** — emar 매핑 과정에서 2,552,081건의 NDC 결측 발생. 4단계 복구 전략(drug명 매칭, GSN, formulary 코드 등)으로 58.2%(1,484,195건) 복구.
- **icustays Window 위반** — stay_id 2,338개가 병원 입원 기간 밖에 존재. 케이스 단위 삭제(코호트 17.3% 손실)를 기각하고 stay_id 단위 삭제로 결정.

### 📚 연구 단위별 기록

| # | 연구 단위 | 기간 | 노트북 | 블로그 포스트 |
|:---:|---------|------|--------|-------------|
| 1 | 코호트 설계 | 2025.07–08 | [01_cohort.ipynb](./notebooks/01_cohort.ipynb) | *(작성 중)* |
| 2 | 이벤트 추상화 설계 | 2025.08–09 | [02_abstraction.ipynb](./notebooks/02_abstraction.ipynb) | *(작성 중)* |
| 3 | 이벤트 로그 구축 | 2025.09–11 | [03_baseline_build.ipynb](./notebooks/03_baseline_build.ipynb) | *(작성 중)* |
| 4 | 정제 + 품질 검증 | 2025.11–2026.01 | [04_quality.ipynb](./notebooks/04_quality.ipynb) | *(작성 중)* |
| 5 | BigQuery + eICU 확장 | 2026.01 | [05_eicu_bq.ipynb](./notebooks/05_eicu_bq.ipynb) | *(작성 중)* |
| — | 7개월 회고 | — | — | *(나중에)* |

---

## 🛠 Part 2. 최종 산출물 (`clinical_el_builder`)

연구 활동의 마지막 단계로, 파편화된 노트북 코드를 후임 연구자가 재현 가능한 형태로 쓸 수 있도록 YAML 설정 기반 Python CLI로 정리했다.

👉 **[`clinical_el_builder/` — 설치·실행 방법 및 아키텍처](./clinical_el_builder/README.md)**

---

## 🔒 데이터 접근

MIMIC-IV는 접근 제한 데이터셋이다. 코드 실행을 위해서는:
1. [PhysioNet](https://physionet.org) 계정 생성 + CITI 교육 이수
2. MIMIC-IV 데이터 사용 동의(DUA) 서명

이 레포지터리에는 환자 데이터가 포함되어 있지 않다.
