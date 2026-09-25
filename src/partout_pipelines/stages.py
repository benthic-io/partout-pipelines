from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

from .config import Config
from .ledger import STAGE_NAMES, Ledger
from .log import get


__all__ = ["Context", "Outcome", "Pipeline", "STAGE_NAMES", "StageFn"]


class Outcome(Enum):
    COMPLETED = "completed"
    SKIPPED = "skipped"


@dataclass
class Context:
    cfg: Config
    dataset: str
    ledger: Ledger
    dbname: str | None = None
    force: bool = False
    dry_run: bool = False
    limit: int | None = None
    variant: str | None = None
    run_id: str | None = None
    reconcile: bool = True
    datasets: tuple[str, ...] | None = None
    validation_database: bool = False
    serving_database: str | None = None
    shared: dict[str, object] = field(default_factory=dict)

    @property
    def archives(self) -> Path:
        return self.cfg.dataset_dir("archives", self.dataset)

    @property
    def work(self) -> Path:
        return self.cfg.dataset_dir("work", self.dataset)

    @property
    def state(self) -> Path:
        return self.cfg.dataset_dir("state", self.dataset)

    @property
    def log(self) -> logging.Logger:
        return get(self.dataset)

    @property
    def target_database(self) -> str:
        return self.dbname or self.cfg.dbname(self.dataset)

    def close(self) -> None:
        importer = self.shared.pop("importer", None)
        close = getattr(importer, "close", None)
        if callable(close):
            close()


StageFn = Callable[[Context], Outcome | None]


@dataclass
class Pipeline:
    dataset: str
    stages: dict[str, StageFn]
    dbname: str | None = None
    description: str = ""

    def resolve_dbname(self, cfg: Config, override: str | None = None) -> str:
        return override or self.dbname or cfg.dbname(self.dataset)

    def stage_order(self) -> tuple[str, ...]:
        unknown = set(self.stages) - set(STAGE_NAMES)
        if unknown:
            raise ValueError(f"{self.dataset}: unknown stages {sorted(unknown)}")
        return tuple(name for name in STAGE_NAMES if name in self.stages)

    def run(
        self,
        cfg: Config,
        *,
        only: Sequence[str] | None = None,
        start_at: str | None = None,
        force: bool = False,
        dry_run: bool = False,
        limit: int | None = None,
        variant: str | None = None,
        dbname: str | None = None,
        run_id: str | None = None,
        reconcile: bool = True,
        datasets: tuple[str, ...] | None = None,
    ) -> int:
        logger = get(self.dataset)
        target = self.resolve_dbname(cfg, dbname)
        order = list(self.stage_order())
        if start_at:
            if start_at not in order:
                logger.error("unknown start stage %s", start_at)
                return 2
            order = order[order.index(start_at) :]
        if only:
            selected = set(only)
            missing = selected - set(self.stages)
            if missing:
                logger.error("unknown stage(s): %s", ", ".join(sorted(missing)))
                return 2
            order = [name for name in order if name in selected]
        if dry_run:
            logger.info("dry run: %s -> %s", target, ", ".join(order) or "no stages")
            return 0
        target_cfg = cfg if target == cfg.dbname(self.dataset) else cfg.with_database_name(target)
        ledger = Ledger(target_cfg, self.dataset, target if dbname else None)
        ledger.ensure()
        stale = ledger.clear_stale()
        if stale:
            logger.warning("%d interrupted stage(s) marked failed", stale)
        target_cfg.ensure_dirs(self.dataset)
        ctx = Context(
            cfg=target_cfg,
            dataset=self.dataset,
            ledger=ledger,
            dbname=target,
            force=force,
            limit=limit,
            variant=variant,
            run_id=run_id,
            reconcile=reconcile,
            datasets=datasets,
            validation_database=bool(dbname and target != cfg.dbname(self.dataset)),
            serving_database=cfg.dbname(self.dataset),
        )
        try:
            for name in order:
                if not force and ledger.is_completed(name):
                    logger.info("%s already completed, skipping", name)
                    continue
                logger.info("--- %s ---", name)
                with ledger.start(name) as timer:
                    ctx.shared["timer"] = timer
                    result = self.stages[name](ctx)
                    if result is Outcome.SKIPPED:
                        timer.note(skipped=True)
            logger.info("pipeline %s finished", self.dataset)
        except Exception as exc:
            logger.error("pipeline halted: %s", exc)
            return 1
        finally:
            ctx.close()
        return 0

    def status(self, cfg: Config, dbname: str | None = None) -> list[tuple[str, str]]:
        target = self.resolve_dbname(cfg, dbname)
        target_cfg = cfg if target == cfg.dbname(self.dataset) else cfg.with_database_name(target)
        ledger = Ledger(target_cfg, self.dataset, target if dbname else None)
        rows: list[tuple[str, str]] = []
        for name in self.stage_order():
            rows.append((name, ledger.last_status(name) or "-"))
        return rows
