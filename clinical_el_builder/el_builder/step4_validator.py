from __future__ import annotations

"""
파이프라인 스텝 4: Baseline Event Log 품질 검증.

Suriadi et al. (2013) 'A Method for Mining Meaningful Events from Process Logs'에서
제시한 Imperfection Patterns를 기반으로 Baseline EL의 데이터 품질을 자동 진단한다.

이 연구에서 직접 검증한 패턴:
  P1 Form-based Event Capture  : 동일 타임스탬프 배치 이벤트 — PASS (배치 비율 97.5%)
  P2 Inadvertent Time Travel   : 입원 범위 밖 이벤트 — PASS (사전 정제 규칙 적용)
  P3 Unanchored Event          : 타임스탬프 NULL 이벤트 — PASS
  P6 Scattered Case            : 10개 테이블 통합 확인 — PASS
  P7 Collateral Events         : chartevents↔labevents 644,760건 중복 → labevents 단일화

직접 검증하지 않은 패턴 (범용 validator 완성을 위해 추가 구현):
  P5 Elusive Case, P8 Polluted Label, P9 Distorted Label, P11 Homonymous Label

실행: python run.py validate --config configs/mimic_iv_complete.yaml
"""

from dataclasses import dataclass, field
from typing import List, Optional

from google.cloud import bigquery

from .config_parser import ELConfig


# ---------------------------------------------------------------------------
# 결과 데이터 클래스
# ---------------------------------------------------------------------------

@dataclass
class PatternResult:
    """개별 패턴 검증 결과."""
    pattern_id: str
    name: str
    status: str          # PASS | WARN | FAIL | SKIP
    message: str
    detail: Optional[str] = None


# ---------------------------------------------------------------------------
# 메인 검증 클래스
# ---------------------------------------------------------------------------

class EventLogValidator:
    """Baseline Event Log에 대한 Imperfection Patterns 검증기.

    Parameters
    ----------
    client : bigquery.Client
        BigQuery 클라이언트.
    config : ELConfig
        프로젝트 설정 (source / target / output / analysis 등).
    """

    def __init__(self, client: bigquery.Client, config: ELConfig) -> None:
        self.client = client
        self.config = config
        self.table_ref = (
            f"`{config.target.project}.{config.target.dataset}"
            f".{config.output.baseline_table}`"
        )
        self.case_id_col = config.analysis.case_id
        self.activity_level = config.analysis.activity_level
        self.activity_col = self._resolve_activity_col()

        self.columns: List[str] = []
        self.results: List[PatternResult] = []
        self.total_events: int = 0

    # ------------------------------------------------------------------
    # 내부 유틸리티
    # ------------------------------------------------------------------

    def _resolve_activity_col(self) -> str:
        return {"l0": "activity_l0", "l1": "activity_l1", "l2": "activity_l2"}.get(
            self.activity_level, "activity_l1"
        )

    def _has_column(self, col: str) -> bool:
        return col in self.columns

    def _detect_columns(self) -> None:
        sql = f"SELECT * FROM {self.table_ref} LIMIT 0"
        result = self.client.query(sql).result()
        self.columns = [f.name for f in result.schema]

    def _query_scalar(self, sql: str) -> int:
        rows = list(self.client.query(sql).result())
        if not rows:
            return 0
        val = rows[0][0]
        return int(val) if val is not None else 0

    def _query_df(self, sql: str):
        return (
            self.client.query(sql)
            .result()
            .to_dataframe(create_bqstorage_client=False)
        )

    def _add(
        self,
        pid: str,
        name: str,
        status: str,
        message: str,
        detail: Optional[str] = None,
    ) -> None:
        self.results.append(PatternResult(pid, name, status, message, detail))

    # ==================================================================
    # P1 / P4  —  Form-based Event Capture & Collateral Events
    # 동일 케이스 + 동일 타임스탬프에 여러 이벤트가 기록된 배치 입력 탐지
    # ==================================================================
    def check_p1_p4_collateral(self) -> None:
        if not (self._has_column(self.case_id_col) and self._has_column("event_timestamp")):
            self._add("P1/P4", "Form-based / Collateral", "SKIP",
                       f"필수 컬럼 누락 ({self.case_id_col}, event_timestamp)")
            return

        sql = f"""
        SELECT
          COUNT(*)        AS batch_count,
          SUM(dup_count)  AS affected_events,
          MAX(dup_count)  AS max_batch_size
        FROM (
          SELECT {self.case_id_col}, event_timestamp, COUNT(*) AS dup_count
          FROM {self.table_ref}
          GROUP BY {self.case_id_col}, event_timestamp
          HAVING COUNT(*) > 1
        )
        """
        df = self._query_df(sql)

        if df.empty or int(df.iloc[0]["batch_count"]) == 0:
            self._add("P1/P4", "Form-based / Collateral", "PASS",
                       "동시 타임스탬프 이벤트 없음")
            return

        batch_count = int(df.iloc[0]["batch_count"])
        affected = int(df.iloc[0]["affected_events"])
        max_size = int(df.iloc[0]["max_batch_size"])
        ratio = (affected / self.total_events * 100) if self.total_events else 0

        status = "WARN" if ratio > 5.0 else "PASS"
        self._add(
            "P1/P4", "Form-based / Collateral", status,
            f"{batch_count:,}개 배치, {affected:,}건 영향 ({ratio:.1f}%), "
            f"최대 배치 크기 {max_size}",
        )

    # ==================================================================
    # P2  —  Inadvertent Time Travel
    # 입퇴원 범위를 벗어나는 이벤트 탐지.
    #
    # 이 연구에서는 Baseline EL 구축 전에 admittime/dischtime 기준으로
    # 입원 범위 밖 이벤트를 사전 제거하여 P2를 해결했다 (별도 정제 스크립트).
    # 범용 validator에서는 admissions 테이블에 직접 접근이 필요하므로 SKIP.
    # ==================================================================
    def check_p2_time_travel(self) -> None:
        self._add("P2", "Inadvertent Time Travel", "SKIP",
                   "사전 정제 단계에서 처리 완료 — validator에서는 admissions 접근 필요")

    # ==================================================================
    # P3  —  Unanchored Event
    # 타임스탬프가 NULL인 이벤트 탐지
    # ==================================================================
    def check_p3_unanchored(self) -> None:
        if not self._has_column("event_timestamp"):
            self._add("P3", "Unanchored Event", "SKIP", "event_timestamp 컬럼 누락")
            return

        null_count = self._query_scalar(
            f"SELECT COUNT(*) FROM {self.table_ref} WHERE event_timestamp IS NULL"
        )

        if null_count == 0:
            self._add("P3", "Unanchored Event", "PASS", "타임스탬프 누락 없음")
        else:
            self._add("P3", "Unanchored Event", "FAIL",
                       f"{null_count:,}건의 이벤트에 타임스탬프 누락")

    # ==================================================================
    # P5  —  Elusive Case
    # Case ID(hadm_id 등)가 NULL인 이벤트 탐지
    # ==================================================================
    def check_p5_elusive_case(self) -> None:
        col = self.case_id_col
        if not self._has_column(col):
            self._add("P5", "Elusive Case", "SKIP", f"{col} 컬럼 누락")
            return

        null_count = self._query_scalar(
            f"SELECT COUNT(*) FROM {self.table_ref} WHERE {col} IS NULL"
        )

        if null_count == 0:
            self._add("P5", "Elusive Case", "PASS",
                       f"모든 이벤트에 {col} 부여됨")
        else:
            ratio = (null_count / self.total_events * 100) if self.total_events else 0
            self._add("P5", "Elusive Case", "WARN",
                       f"{null_count:,}건의 이벤트에 {col} 누락 ({ratio:.1f}%)")

    # ==================================================================
    # P6  —  Scattered Case (Remedy Verification)
    # 여러 소스 테이블이 하나의 Baseline으로 통합되었는지 확인
    # ==================================================================
    def check_p6_scattered_case(self) -> None:
        if not self._has_column("source_table"):
            self._add("P6", "Scattered Case", "SKIP", "source_table 컬럼 누락")
            return

        sql = f"""
        SELECT source_table, COUNT(*) AS event_count
        FROM {self.table_ref}
        GROUP BY source_table
        ORDER BY event_count DESC
        """
        df = self._query_df(sql)

        n_sources = len(df)
        items = df["source_table"].tolist()
        sources = ", ".join(items[:10])
        if n_sources > 10:
            sources += f" ... (+{n_sources - 10})"

        self._add("P6", "Scattered Case", "PASS",
                   f"{n_sources}개 소스 테이블 통합 완료",
                   f"소스: {sources}")

    # ==================================================================
    # P7  —  Duplicate Events
    # 핵심 컬럼(case_id, timestamp, activity, source_table) 기준 완전 중복 탐지.
    #
    # 참고: 실제 연구(260114)에서 수행한 P7 작업은 이것과 다르다.
    # 당시 P7은 chartevents ↔ labevents 간 cross-table label 중복(644,760건)을
    # 탐지하고 labevents를 master로 확정하는 작업이었다.
    # 여기서는 동일 키 기준 exact duplicate 여부를 범용적으로 검사한다.
    # ==================================================================
    def check_p7_duplicates(self) -> None:
        key_cols = [
            c for c in [self.case_id_col, "event_timestamp", "activity_l0", "source_table"]
            if self._has_column(c)
        ]
        if len(key_cols) < 2:
            self._add("P7", "Duplicate Events", "SKIP", "검증에 필요한 컬럼 부족")
            return

        cols_str = ", ".join(key_cols)
        sql = f"""
        SELECT IFNULL(SUM(dup_count - 1), 0) AS total_duplicates
        FROM (
          SELECT {cols_str}, COUNT(*) AS dup_count
          FROM {self.table_ref}
          GROUP BY {cols_str}
          HAVING COUNT(*) > 1
        )
        """
        dup_count = self._query_scalar(sql)

        if dup_count == 0:
            self._add("P7", "Duplicate Events", "PASS", "중복 이벤트 없음")
        else:
            ratio = (dup_count / self.total_events * 100) if self.total_events else 0
            status = "FAIL" if ratio > 1.0 else "WARN"
            self._add("P7", "Duplicate Events", status,
                       f"{dup_count:,}건의 중복 이벤트 ({ratio:.1f}%)")

    # ==================================================================
    # P8  —  Polluted Label
    # 레이블에 숫자 ID / UUID 등이 혼입되어 고유 레이블 수가 비정상적으로 많은 경우.
    # 참고: 실제 연구에서 직접 검증한 패턴은 아니다.
    # ==================================================================
    def check_p8_polluted_label(self) -> None:
        act = self.activity_col
        if not self._has_column(act):
            self._add("P8", "Polluted Label", "SKIP", f"{act} 컬럼 누락")
            return

        sql = f"""
        SELECT
          COUNT(DISTINCT {act}) AS n_original,
          COUNT(DISTINCT REGEXP_REPLACE({act}, r'[0-9a-fA-F]{{6,}}', '<ID>')) AS n_masked
        FROM {self.table_ref}
        WHERE {act} IS NOT NULL
        """
        df = self._query_df(sql)
        n_orig = int(df.iloc[0]["n_original"])
        n_masked = int(df.iloc[0]["n_masked"])

        ratio = (n_orig / n_masked) if n_masked > 0 else 1.0

        if ratio > 2.0:
            self._add("P8", "Polluted Label", "WARN",
                       f"ID 오염 의심 — 원본 {n_orig:,}개 → 마스킹 후 {n_masked:,}개 (비율 {ratio:.1f}x)")
        else:
            self._add("P8", "Polluted Label", "PASS",
                       f"고유 레이블 {n_orig:,}개 (마스킹 비율 {ratio:.2f}x)")

    # ==================================================================
    # P9  —  Distorted Label
    # 대소문자/공백만 다른 동일 의미 레이블 탐지.
    # 참고: 실제 연구에서 직접 검증한 패턴은 아니다.
    # ==================================================================
    def check_p9_distorted_label(self) -> None:
        act = self.activity_col
        if not self._has_column(act):
            self._add("P9", "Distorted Label", "SKIP", f"{act} 컬럼 누락")
            return

        sql = f"""
        SELECT
          UPPER(TRIM({act})) AS norm,
          COUNT(DISTINCT {act}) AS variation_count
        FROM {self.table_ref}
        WHERE {act} IS NOT NULL
        GROUP BY UPPER(TRIM({act}))
        HAVING COUNT(DISTINCT {act}) > 1
        ORDER BY variation_count DESC
        LIMIT 20
        """
        df = self._query_df(sql)
        distorted_count = len(df)

        if distorted_count == 0:
            self._add("P9", "Distorted Label", "PASS", "대소문자/공백 불일치 없음")
        else:
            examples = ", ".join(df["norm"].tolist()[:5])
            self._add("P9", "Distorted Label", "WARN",
                       f"{distorted_count}개 그룹에서 표기 변이 발견",
                       f"예: {examples}")

    # ==================================================================
    # P11  —  Homonymous Label
    # 동일 레이블이 서로 다른 소스 테이블에서 유의미한 비율(≥20%)로 발생하는 경우.
    # 참고: 실제 연구에서 직접 검증한 패턴은 아니다.
    # ==================================================================
    def check_p11_homonymous_label(self) -> None:
        act = self.activity_col
        if not (self._has_column(act) and self._has_column("source_table")):
            self._add("P11", "Homonymous Label", "SKIP",
                       f"필수 컬럼 누락 ({act}, source_table)")
            return

        sql = f"""
        WITH activity_source AS (
          SELECT {act} AS activity, source_table, COUNT(*) AS n
          FROM {self.table_ref}
          WHERE {act} IS NOT NULL
          GROUP BY {act}, source_table
        ),
        activity_total AS (
          SELECT activity, SUM(n) AS total
          FROM activity_source
          GROUP BY activity
          HAVING SUM(n) >= 50
        )
        SELECT COUNT(*) AS homonymous_count
        FROM (
          SELECT a.activity
          FROM activity_source a
          JOIN activity_total t ON a.activity = t.activity
          WHERE SAFE_DIVIDE(a.n, t.total) >= 0.2
          GROUP BY a.activity
          HAVING COUNT(DISTINCT a.source_table) >= 2
        )
        """
        count = self._query_scalar(sql)

        if count == 0:
            self._add("P11", "Homonymous Label", "PASS", "동음이의 레이블 없음")
        else:
            self._add("P11", "Homonymous Label", "WARN",
                       f"{count}개 레이블이 여러 소스에서 유의미한 비율(≥20%)로 발생")

    # ==================================================================
    # 실행 & 보고서 출력
    # ==================================================================

    def run(self) -> List[PatternResult]:
        """전체 Imperfection Pattern 검증을 실행하고 보고서를 출력한다."""
        print("이벤트 로그 품질 검증을 시작합니다...")
        print(f"  대상 테이블: {self.table_ref}")
        print(f"  Case ID    : {self.case_id_col}")
        print(f"  Activity   : {self.activity_col} ({self.activity_level})")
        print()

        # 스키마 탐지 & 총 이벤트 수
        self._detect_columns()
        self.total_events = self._query_scalar(
            f"SELECT COUNT(*) FROM {self.table_ref}"
        )
        print(f"  총 이벤트 수: {self.total_events:,}")
        print(f"  감지된 컬럼 : {', '.join(self.columns)}")
        print()

        # 개별 패턴 실행
        checks = [
            self.check_p1_p4_collateral,
            self.check_p2_time_travel,
            self.check_p3_unanchored,
            self.check_p5_elusive_case,
            self.check_p6_scattered_case,
            self.check_p7_duplicates,
            self.check_p8_polluted_label,
            self.check_p9_distorted_label,
            self.check_p11_homonymous_label,
        ]
        for fn in checks:
            label = fn.__name__.replace("check_", "").upper()
            print(f"  검증 중: {label} ...")
            fn()

        self._print_report()
        return self.results

    def _print_report(self) -> None:
        icons = {"PASS": "\u2705", "WARN": "\u26a0\ufe0f", "FAIL": "\u274c", "SKIP": "\u23ed\ufe0f"}

        print()
        print("=" * 72)
        print("# Event Log Quality Report")
        print(f"  Table  : {self.table_ref}")
        print(f"  Events : {self.total_events:,}")
        print("=" * 72)
        print()

        for r in self.results:
            icon = icons.get(r.status, "")
            print(f"- **{r.pattern_id}** ({r.name}): {icon} {r.status} — {r.message}")

        # PASS 이외 상세 정보
        details = [r for r in self.results if r.detail and r.status != "PASS"]
        if details:
            print()
            print("## Details")
            for r in details:
                print(f"  - {r.pattern_id} ({r.name}): {r.detail}")

        # 요약 카운터
        from collections import Counter
        cnt = Counter(r.status for r in self.results)
        print()
        print(
            f"Summary: {cnt.get('PASS', 0)} passed, "
            f"{cnt.get('WARN', 0)} warnings, "
            f"{cnt.get('FAIL', 0)} failed, "
            f"{cnt.get('SKIP', 0)} skipped"
        )
        print("=" * 72)
