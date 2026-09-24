from __future__ import annotations

import argparse
import sys
import uuid
from collections.abc import Sequence
from typing import Any

import psycopg2

from .config import ConfigError, load_config
from .nhtsa import (
    DATASETS,
    ApiUnavailableError,
    CircuitBreakerOpen,
    ImporterError,
    NhtsaClient,
    NhtsaImporter,
)


def _add_config_option(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", help="Path to parts.toml")


def _add_rate_option(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--rate",
        type=float,
        help="Minimum seconds between requests; must be at least 1.0.",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="parts-pipelines",
        description="Import and inspect NHTSA/vPIC reference data.",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    sync = commands.add_parser(
        "nhtsa-reference",
        help="Run or resume a full reference import.",
    )
    _add_config_option(sync)
    sync.add_argument("--run-id", help="Existing run ID to resume.")
    sync.add_argument(
        "--dataset",
        action="append",
        choices=DATASETS,
        help="Dataset to sync; repeat for multiple datasets. Default: all.",
    )
    _add_rate_option(sync)
    sync.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the plan without connecting, requesting, or writing.",
    )
    sync.add_argument(
        "--no-reconcile",
        action="store_true",
        help="Do not remove stale rows after a complete dataset.",
    )

    status = commands.add_parser("status", help="Show importer run and job status.")
    _add_config_option(status)
    status.add_argument("--run-id", help="Limit output to one run ID.")
    status.add_argument("--dataset", choices=DATASETS, help="Limit output to one dataset.")

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


def _run_sync(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    if args.rate is not None:
        config = config.with_request_interval(args.rate)
    datasets = _selected_datasets(args.dataset)
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


def _run_status(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    with NhtsaImporter(config) as importer:
        result = importer.status(run_id=args.run_id, dataset=args.dataset)
    _render_status(result)
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
        if args.command == "nhtsa-reference":
            return _run_sync(args)
        if args.command == "status":
            return _run_status(args)
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
    except (ImporterError, psycopg2.Error, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("interrupted; rerun with the same --run-id to resume", file=sys.stderr)
        return 130
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
