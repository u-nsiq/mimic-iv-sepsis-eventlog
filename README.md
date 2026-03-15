# 🏥 MIMIC-IV Sepsis Event Log: Research Archive & Pipeline

<div align="center">
  <img src="https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=Python&logoColor=white"/>
  <img src="https://img.shields.io/badge/Google%20BigQuery-4285F4?style=flat-square&logo=Google%20BigQuery&logoColor=white"/>
  <img src="https://img.shields.io/badge/Process%20Mining-FF6F00?style=flat-square"/>
</div>

> **Kwangwoon University Data Analytics Lab 학부연구생 연구 활동 종합 기록 (2025.07 ~ 2026.02)**

## 📖 About This Archive

본 리포지토리는 약 7개월간 진행된 헬스케어 프로세스 마이닝(Process Mining) 연구 활동의 **포트폴리오이자 최종 산출물 아카이브**입니다. 

미국 최대 임상 DB인 **MIMIC-IV**와 다기관 데이터 **eICU-CRD**를 활용하여, 패혈증(Sepsis) 환자의 임상 경로를 분석하기 위한 대규모 이벤트 로그(Event Log)를 설계하고 구축했습니다. 

이 공간은 파편화된 임상 기록을 어떻게 분석 가능한 데이터로 추상화하고 결함을 통제했는지에 대한 **'연구 기록(`notebooks/`)'**과, 그 6개월간의 과정을 후임자를 위해 재현 가능한 파이프라인으로 묶어낸 **'엔지니어링 결과물(`clinical_el_builder/`)'**을 모두 담고 있습니다.

---

## 🔬 Part 1. Research Journey (연구 및 데이터 엔지니어링)

단순한 데이터 추출을 넘어, 1.1억 건 이상의 임상 데이터를 신뢰할 수 있는 이벤트 로그로 정제하기 위한 고민의 과정입니다.

### 💡 Core Highlights
* **Cohort & Event Log Design**
  * 응급실부터 퇴원까지 환자의 전체 여정을 포착하기 위해 `hadm_id`를 Case ID로 채택하고 Sepsis-3 코호트(13,471건)를 구축했습니다.
  * 복잡도를 낮추기 위해 L0(원본) $\rightarrow$ L1(임상 의미 단위) $\rightarrow$ L2(최상위 대분류)의 계층적 추상화를 설계했습니다.
  * 임상 기록의 지연 입력 특성을 고려해 **5분 윈도우 기반 이벤트 병합**을 적용, 핵심 정보 손실 없이 이벤트 볼륨을 **86.2% 축소**했습니다.
* **Data Quality Assurance (Suriadi 11 Patterns)**
  * 헬스케어 데이터의 고질적 결함을 학술적 프레임워크 기반으로 검증했습니다. 배치 입력(Form-based Event Capture, 전체 97.5%), 시간 역전(Inadvertent Time Travel) 현상을 규명하고 정제 로직을 확립했습니다.

### 🚧 Key Challenges & Troubleshooting
> (추후 작성: 로컬 SQLite에서 BigQuery로 마이그레이션 한 이유, 타임스탬프 버그 해결 과정, NDC 결측치 148만 건 복구 전략 등 면접관이 흥미로워할 굵직한 문제 해결 사례 2~3개 요약)

### 📚 Detailed Logs (Blog Posts)
연구 과정에서의 깊은 고민과 설계 근거(Design Decisions)는 기술 블로그 **JunSik.io**와 단계별 Notebook에 연재되어 있습니다. 상세한 의사결정 기록은 [`docs/design_decisions.md`](./docs/design_decisions.md)를 참고하세요.

| Step | Topic | Blog Post | Notebook |
| :---: | :--- | :--- | :--- |
| 1 | **Cohort** | [MIMIC-IV 패혈증 코호트 만들기 - Sepsis-3와 hadm_id](#) | [`01_cohort.ipynb`](./notebooks/01_cohort.ipynb) |
| 2 | **Abstraction** | [임상 이벤트 로그의 추상화 - L0/L1/L2 계층 설계](#) | [`02_abstraction.ipynb`](./notebooks/02_abstraction.ipynb) |
| 3 | **Build** | [10개 테이블을 하나의 이벤트 로그로 - 구축 과정과 결정들](#) | [`03_baseline_build.ipynb`](./notebooks/03_baseline_build.ipynb) |
| 4 | **Quality** | [임상 데이터의 불완전함 - Suriadi 11패턴 검증하기](#) | [`04_quality.ipynb`](./notebooks/04_quality.ipynb) |
| 5 | **Expansion** | [SQLite에서 BigQuery로, eICU-CRD로의 확장](#) | [`05_eicu_bq.ipynb`](./notebooks/05_eicu_bq.ipynb) |
| - | **Review** | [Data Analytics Lab 7개월 회고 - 내가 남긴 것과 배운 것](#) | - |

---

## 🛠 Part 2. Final Artifact (`clinical_el_builder`)

연구 활동의 최종 산출물로, 6개월간 파편화되었던 노트북의 로직들을 모아 **YAML 설정 주도형 Python CLI 파이프라인**을 개발했습니다. 후임 연구자가 복잡한 쿼리 작성 없이 전체 이벤트 로그를 재구축하고 확장할 수 있습니다.

👉 **[clinical_el_builder 상세 사용 설명서 보러가기](./clinical_el_builder/README.md)** ---

## 🔒 Data Privacy & Access Policy
본 리포지토리의 소스 코드는 데이터 전처리 로직만 포함하며, 원본 데이터베이스나 환자 식별 정보(PHI)는 일절 포함하지 않습니다. 코드를 실행하려면 [PhysioNet](https://physionet.org/)의 규정에 따라 데이터 사용 교육(CITI) 이수 및 접근 권한 승인이 필요합니다.

---
**Jun Sik Kim** Software Engineering Dept., Kwangwoon Univ.  
[Tech Blog - JunSik.io](https://junsik.io) | wnstlr0830@gmail.com