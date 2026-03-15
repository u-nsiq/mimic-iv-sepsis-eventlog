# MIMIC-IV Sepsis Event Log

[![Python](https://img.shields.io/badge/Python-3.x-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![BigQuery](https://img.shields.io/badge/Google_BigQuery-4285F4?style=flat-square&logo=googlebigquery&logoColor=white)](https://cloud.google.com/bigquery)
[![MIMIC-IV](https://img.shields.io/badge/MIMIC--IV-v3.1-green?style=flat-square)](https://physionet.org/content/mimiciv/)
[![Data](https://img.shields.io/badge/Data-PhysioNet_Credentialed-red?style=flat-square)](https://physionet.org/)

> Clinical event log construction for process mining — MIMIC-IV v3.1 & eICU-CRD
> Undergraduate Research Archive and Final Pipeline Artifact
> Data Analytics Lab, Kwangwoon University — 2025.07 ~ 2026.02

---

## Overview

MIMIC-IV와 eICU-CRD를 활용해 패혈증 환자 기록으로부터 프로세스 마이닝용 임상 이벤트 로그를 구축한 학부연구 프로젝트를 정리한 Repository다.

연구는 코호트 설계, 이벤트 추상화, 이벤트 로그 구축, 데이터 품질 검증으로 이어지는 전체 데이터 준비 파이프라인을 다뤘다. 최종 목표는 전통적 프로세스 마이닝과 Object-Centric PM의 비교 분석이었으며, 이 Repository는 그에 앞선 데이터 준비 단계를 중심으로 정리되어 있다.

이 Repository는 두 부분으로 구성된다:
1. 설계 과정과 의사결정을 기록한 **연구 아카이브**(`notebooks/`, `docs/`)
2. notebook(`.ipynb`) 기반 작업들을 재사용 가능한 CLI로 정리한 **최종 파이프라인 산출물**(`clinical_el_builder/`)

---

## Key Highlights

- **Sepsis cohort**: 13,471 admissions / 11,081 patients (MIMIC-IV)
- **Source integration**: 10개 임상 테이블을 전처리해 단일 이벤트 로그 스키마로 통합
- **Baseline event log**: 76,062,216 events (L0)
- **Abstraction effect**: L0 → L2 + 5분 윈도우 병합 기준 86.2% reduction
- **Quality validation**: Suriadi 11 Imperfection Patterns 기준 P2, P3, P6, P7 통과
- **Multi-center extension**: eICU-CRD cohort 15,731 stays에 파이프라인 적용 및 검증

---

## What I Worked On

- **Cohort design**: ICD-10 A40.–/A41.– 기반의 성인 패혈증 코호트를 구축하고, `stay_id` 대신 `hadm_id`를 Case ID로 선택해 입원 단위의 전체 환자 여정을 포착했다.
- **Event abstraction design**: 테이블별 Raw Cardinality를 L1 임상 카테고리와 L2 연구 그룹으로 축소하는 3단계 계층 구조를 설계하고, 테이블 특성에 따라 매핑 전략을 다르게 적용했다.
- **Event log construction**: 10개 임상 테이블을 `journey`, `in_out`, `duration`, `one_off`의 4가지 이벤트 타입으로 정규화해 76M 건 규모의 Baseline Event Log를 구축했다.
- **Data quality validation**: Suriadi et al. 프레임워크를 바탕으로 배치 입력, 시간 역전, 교차 테이블 중복 등 임상 데이터 고유의 결함 패턴을 검증하고 처리했다.
- **BigQuery migration and eICU extension**: 151GB 규모의 로컬 SQLite 워크플로를 BigQuery로 이전하고, eICU-CRD까지 확장해 다기관 데이터셋 적용 가능성을 점검했다.

---

## Repository Guide

```text
mimic-iv-sepsis-eventlog/
├── notebooks/                # 5-stage research notebooks
├── docs/                     # design notes and methodological decisions
│   └── design_decisions.md
└── clinical_el_builder/      # final reusable pipeline artifact
    ├── README.md             # installation and usage guide
    ├── el_builder/
    ├── configs/
    └── run.py
```

* Pipeline usage: [`clinical_el_builder/README.md`](./clinical_el_builder/README.md)
* Design rationale: [`docs/design_decisions.md`](./docs/design_decisions.md)

---

## Final Artifact

`clinical_el_builder`는 이 연구에서 개발한 이벤트 로그 구축 방법론을 YAML 설정 기반 Python CLI로 정리한 최종 산출물이다. MIMIC-IV와 eICU-CRD에 모두 적용할 수 있도록 구성되어 있으며, 코호트 필터링, L1/L2 매핑, 품질 검증 단계를 재사용 가능한 형태로 묶고 있다.

→ **[`clinical_el_builder/`](./clinical_el_builder/README.md)**

---

## Data Access

이 Repository는 코드와 문서만 포함하며, 원본 임상 데이터나 환자 식별 정보(PHI)는 포함하지 않는다. 코드를 실행하려면 [PhysioNet](https://physionet.org/)의 CITI 교육 이수와 데이터 접근 승인이 필요하다.

* MIMIC-IV v3.1: [physionet.org/content/mimiciv/](https://physionet.org/content/mimiciv/)
* eICU-CRD: [physionet.org/content/eicu-crd/](https://physionet.org/content/eicu-crd/)

---

## Author

**Jun Sik Kim**
Undergraduate Researcher, Data Analytics Lab (2025.07 – 2026.02)
Software Engineering, Kwangwoon University
[JunSik.io](https://u-nsiq.github.io/) · [wnstlr0830@gmail.com](mailto:wnstlr0830@gmail.com)
