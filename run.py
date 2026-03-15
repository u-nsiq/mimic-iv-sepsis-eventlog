from __future__ import annotations

"""
Clinical Event Log Builder CLI.

실행 순서:
  1) python run.py build-mappings --config configs/mimic_iv_complete.yaml --mappings-config configs/mappings_config.yaml
  2) python run.py build-baseline --config configs/mimic_iv_complete.yaml
  3) python run.py build-analysis --config configs/mimic_iv_complete.yaml
  4) python run.py validate       --config configs/mimic_iv_complete.yaml

SQL 미리보기 (실행 없이 생성 SQL만 출력):
  python run.py build-baseline --config configs/mimic_iv_complete.yaml --dry-run
"""

import argparse

from el_builder.step2_baseline import build_baseline_sql
from el_builder.step3_analysis import build_analysis_sql
from el_builder.step1_mappings import build_l1_mapping_sql, build_l2_mapping_sql
from el_builder.step4_validator import EventLogValidator
from el_builder.config_parser import load_config, load_mappings_config
from bigquery_utils import get_client, run_query


def cmd_build_baseline(args):
    config = load_config(args.config)
    sql = build_baseline_sql(config)

    if args.dry_run:
        print("=== Baseline Event Log SQL ===")
        print(sql)
        return

    client = get_client(config.target.project)
    destination_table = f"{config.target.project}.{config.target.dataset}.{config.output.baseline_table}"
    print(f"Building baseline event log → {destination_table} ...")
    client.query(sql).result()
    print(f"[OK] {destination_table}")


def cmd_build_analysis(args: argparse.Namespace) -> None:
    cfg = load_config(args.config)
    sql = build_analysis_sql(cfg)

    if args.dry_run:
        print("=== Analysis Event Log SQL ===")
        print(sql)
        return

    client = get_client(cfg.target.project)
    print("Building analysis view ...")
    run_query(client, sql)
    print("[OK] Analysis view created.")


def cmd_build_mappings(args: argparse.Namespace) -> None:
    config = load_config(args.config)
    mappings_rules = load_mappings_config(args.mappings_config)

    # L1 먼저 빌드 (L2 conditional 규칙이 L1 테이블에 의존)
    l1_sql = build_l1_mapping_sql(config, mappings_rules)
    if l1_sql:
        if args.dry_run:
            print("=== L1 Mapping Table SQL ===")
            print(l1_sql)
            print()
        else:
            dest = f"{config.target.project}.{config.target.dataset}.{mappings_rules.l1.output_table}"
            print(f"Building L1 mapping table → {dest} ...")
            get_client(config.target.project).query(l1_sql).result()
            print(f"[OK] {dest}")

    l2_sql = build_l2_mapping_sql(config, mappings_rules)
    if l2_sql:
        if args.dry_run:
            print("=== L2 Mapping Table SQL ===")
            print(l2_sql)
        else:
            dest = f"{config.target.project}.{config.target.dataset}.{mappings_rules.l2.output_table}"
            print(f"Building L2 mapping table → {dest} ...")
            get_client(config.target.project).query(l2_sql).result()
            print(f"[OK] {dest}")


def cmd_validate(args: argparse.Namespace) -> None:
    config = load_config(args.config)
    client = get_client(config.target.project)
    EventLogValidator(client, config).run()


def cmd_build_all(args: argparse.Namespace) -> None:
    print("=== Step 1/2: build-baseline ===")
    cmd_build_baseline(args)
    print("\n=== Step 2/2: build-analysis ===")
    cmd_build_analysis(args)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Clinical Event Log Builder (BigQuery)")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # build-mappings
    p_mappings = subparsers.add_parser("build-mappings", help="L1/L2 추상화 매핑 테이블 생성 (build-baseline 전에 실행)")
    p_mappings.add_argument("--config", required=True, help="메인 YAML 설정 파일 경로")
    p_mappings.add_argument("--mappings-config", required=True, help="매핑 규칙 YAML 파일 경로")
    p_mappings.add_argument("--dry-run", action="store_true", help="SQL만 출력하고 실행하지 않음")
    p_mappings.set_defaults(func=cmd_build_mappings)

    # build-baseline
    p_baseline = subparsers.add_parser("build-baseline", help="Baseline Event Log 테이블 구축")
    p_baseline.add_argument("--config", required=True, help="YAML 설정 파일 경로")
    p_baseline.add_argument("--dry-run", action="store_true", help="SQL만 출력하고 실행하지 않음")
    p_baseline.set_defaults(func=cmd_build_baseline)

    # build-analysis
    p_analysis = subparsers.add_parser("build-analysis", help="Analysis Event Log 뷰 생성")
    p_analysis.add_argument("--config", required=True, help="YAML 설정 파일 경로")
    p_analysis.add_argument("--dry-run", action="store_true", help="SQL만 출력하고 실행하지 않음")
    p_analysis.set_defaults(func=cmd_build_analysis)

    # build-all
    p_all = subparsers.add_parser("build-all", help="build-baseline + build-analysis 순서 실행")
    p_all.add_argument("--config", required=True, help="YAML 설정 파일 경로")
    p_all.add_argument("--dry-run", action="store_true", help="SQL만 출력하고 실행하지 않음")
    p_all.set_defaults(func=cmd_build_all)

    # validate
    p_validate = subparsers.add_parser(
        "validate",
        help="Baseline EL 품질 검증 (Suriadi Imperfection Patterns)",
    )
    p_validate.add_argument("--config", required=True, help="YAML 설정 파일 경로")
    p_validate.set_defaults(func=cmd_validate)

    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
