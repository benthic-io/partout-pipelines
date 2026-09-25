from __future__ import annotations

import argparse
import sys
import uuid
from collections.abc import Sequence
from typing import Any

import psycopg2

from .config import ConfigError, load_config
from .ledger import Ledger
from .log import setup_logging
from .nhtsa import (
    DATASETS,
    ApiUnavailableError,
    CircuitBreakerOpen,
    ImporterError,
    NhtsaClient,
    NhtsaImporter,
)
from .stages import STAGE_NAMES
from pipelines import nhtsa as nhtsa_pipeline


_DATASET_CHOICES = ("nhtsa", "vpic")


def _add_config_option(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--config",
        default=argparse.SUPPRESS,
        help="Path to partout.toml",
    )


def _add_rate_option(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--rate",
        type=float,
        help="Minimum seconds between requests; must be at least 1.0.",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="partout",
        description="Run the NHTSA/vPIC reference-data pipeline.",
    )
    parser.add_argument("--version", action="version", version="%(prog)s 0.1.0")
    parser.add_argument("--config", help="Path to partout.toml")
    commands = parser.add_subparsers(dest="command", required=True)

    run = commands.add_parser("run", help="Run or resume the NHTSA pipeline.")
    run.add_argument("dataset", nargs="?", choices=_DATASET_CHOICES, default="nhtsa")
    _add_config_option(run)
    run.add_argument("--only", action="append", choices=STAGE_NAMES)
    run.add_argument("--start-at", choices=STAGE_NAMES)
    run.add_argument("--force", action="store_true")
    run.add_argument("--dry-run", action="store_true")
    run.add_argument("--limit", type=int)
    run.add_argument("--variant")
    run.add_argument("--dbname", help="Validation database override.")
    run.add_argument("--run-id")
    run.add_argument(
        "--datasets",
        "--dataset",
        action="append",
        choices=DATASETS,
        dest="import_datasets",
        help="Importer dataset; repeat for multiple datasets.",
    )
    run.add_argument("--no-reconcile", action="store_true")

    status = commands.add_parser("status", help="Show pipeline or importer status.")
    status.add_argument("dataset", nargs="?", choices=_DATASET_CHOICES)
    _add_config_option(status)
    status.add_argument("--dbname")
    status.add_argument("--run-id")
    status.add_argument("--dataset", dest="import_dataset", choices=DATASETS)

    reset = commands.add_parser("reset", help="Clear the file stage ledger.")
    reset.add_argument("dataset", choices=_DATASET_CHOICES)
    _add_config_option(reset)
    reset.add_argument("--dbname")
    reset.add_argument("--stage", choices=STAGE_NAMES)

    stages = commands.add_parser("stages", help="List the canonical stage sequence.")
    stages.add_argument("dataset", nargs="?", choices=_DATASET_CHOICES, default="nhtsa")

    config = commands.add_parser("config", help="Show the resolved configuration path.")
    _add_config_option(config)

    sync = commands.add_parser(
        "nhtsa-reference",
        help="Run or resume the legacy direct importer command.",
    )
    _add_config_option(sync)
    sync.add_argument("--run-id")
    sync.add_argument("--dataset", action="append", choices=DATASETS, dest="import_datasets")
    _add_rate_option(sync)
    sync.add_argument("--dry-run", action="store_true")
    sync.add_argument("--no-reconcile", action="store_true")

    probe = commands.add_parser("probe", help="Make one low-rate API request.")
    _add_config_option(probe)
    _add_rate_option(probe)
    return parser


def _selected_datasets(values: Sequence[str] | None) -> tuple[str, ...]:
    selected = set(values or DATASETS)
    return tuple(dataset for dataset in DATASETS if dataset in selected)


def _print_dry_run(
    run_id: str,
    datasets: Sequence[str],
    reconcile: bool,
    config_path: str,
) -> None:
    print(f"config: {config_path}")
    print(f"run_id: {run_id}")
    print(f"datasets: {', '.join(datasets)}")
    print(f"reconcile: {'enabled' if reconcile else 'disabled'}")
    print("database connections: 0")
    print("API requests: 0")
    print("database writes: 0")


def _run_direct_sync(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    if args.rate is not None:
        config = config.with_request_interval(args.rate)
    datasets = _selected_datasets(args.import_datasets)
    run_id = args.run_id or str(uuid.uuid4())
    if args.dry_run:
        _print_dry_run(
            run_id,
            datasets,
            not args.no_reconcile,
            str(config.path),
        )
        return 0
    with NhtsaImporter(config) as importer:
        actual_run_id = importer.run(
            run_id=run_id,
            datasets=datasets,
            reconcile=not args.no_reconcile,
        )
    print(f"completed run {actual_run_id}")
    return 0


def _render_status(status: dict[str, list[dict[str, Any]]]) -> None:
    if not status["runs"] and not status["jobs"] and not status["progress"]:
        print("no NHTSA import runs found")
        return
    if status["runs"]:
        print("runs")
        for row in status["runs"]:
            datasets = ",".join(row["datasets"])
            print(
                f"  {row['run_id']}  {row['status']}  jobs={row['completed_jobs']}/"
                f"{row['total_jobs']}  rows={row['row_count']}  datasets={datasets}"
            )
            if row["error"]:
                print(f"    error: {row['error']}")
    if status["jobs"]:
        print("jobs")
        for row in status["jobs"]:
            print(
                f"  {row['run_id']}  {row['dataset']}  {row['status']}  "
                f"jobs={row['job_count']}  attempts={row['attempts']}  rows={row['row_count']}"
            )
    if status["progress"]:
        print("progress")
        for row in status["progress"]:
            print(
                f"  {row['importer_name']}  {row['status']}  "
                f"jobs={row['last_offset']}  rows={row['total_records']}"
            )


def _run_importer_status(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    with NhtsaImporter(config) as importer:
        result = importer.status(run_id=args.run_id, dataset=args.import_dataset)
    _render_status(result)
    return 0


def _run_pipeline_status(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    pipeline = nhtsa_pipeline.build()
    target = pipeline.resolve_dbname(config, args.dbname)
    print(f"{args.dataset or 'nhtsa'}  ->  {target}")
    for stage, stage_status in pipeline.status(config, args.dbname):
        print(f"  {stage:<12} {stage_status}")
    return 0


def _run_pipeline(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    if args.dry_run:
        setup_logging(None)
        return nhtsa_pipeline.build().run(
            config,
            only=args.only,
            start_at=args.start_at,
            force=args.force,
            dry_run=True,
            limit=args.limit,
            variant=args.variant,
            dbname=args.dbname,
            run_id=args.run_id,
            reconcile=not args.no_reconcile,
            datasets=_selected_datasets(args.import_datasets),
        )
    setup_logging(config.path_for("logs"))
    return nhtsa_pipeline.build().run(
        config,
        only=args.only,
        start_at=args.start_at,
        force=args.force,
        limit=args.limit,
        variant=args.variant,
        dbname=args.dbname,
        run_id=args.run_id,
        reconcile=not args.no_reconcile,
        datasets=_selected_datasets(args.import_datasets),
    )


def _run_reset(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    pipeline = nhtsa_pipeline.build()
    target = pipeline.resolve_dbname(config, args.dbname)
    target_cfg = config if target == config.dbname("nhtsa") else config.with_database_name(target)
    ledger = Ledger(target_cfg, "nhtsa", target if args.dbname else None)
    ledger.ensure()
    removed = ledger.reset(args.stage)
    print(f"reset {args.stage or 'all stages'} for nhtsa ({removed} ledger records removed)")
    return 0


def _run_probe(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    if args.rate is not None:
        config = config.with_request_interval(args.rate)
    url = f"{config.api.base_url.rstrip('/')}/vehicles/GetAllMakes?format=json"
    with NhtsaClient(config.api) as client:
        result = client.probe(url)
    status_text = result.http_status if result.http_status is not None else "unreachable"
    latency_text = (
        f"{result.latency_seconds * 1000:.0f} ms"
        if result.latency_seconds is not None
        else "unknown"
    )
    print(f"status: {status_text}")
    print(f"latency: {latency_text}")
    print(f"bytes: {result.response_bytes}")
    if result.count is not None:
        print(f"count: {result.count}")
    if result.message:
        print(f"message: {result.message}")
    if result.error:
        print(f"error: {result.error}", file=sys.stderr)
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "stages":
            for name in STAGE_NAMES:
                print(name)
            return 0
        if args.command == "run":
            return _run_pipeline(args)
        if args.command == "status":
            if args.dataset:
                return _run_pipeline_status(args)
            return _run_importer_status(args)
        if args.command == "reset":
            return _run_reset(args)
        if args.command == "config":
            print(load_config(args.config).path)
            return 0
        if args.command == "nhtsa-reference":
            return _run_direct_sync(args)
        if args.command == "probe":
            return _run_probe(args)
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 2
    except CircuitBreakerOpen as exc:
        print(f"NHTSA circuit breaker: {exc}", file=sys.stderr)
        return 4
    except ApiUnavailableError as exc:
        print(f"NHTSA unavailable: {exc}", file=sys.stderr)
        return 5
    except (ImporterError, psycopg2.Error, OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("interrupted; rerun to resume", file=sys.stderr)
        return 130
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
