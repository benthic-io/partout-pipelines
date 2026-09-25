from __future__ import annotations

import json
from typing import cast

from partout_pipelines import db
from partout_pipelines.nhtsa import NhtsaImporter
from partout_pipelines.stages import Context, Outcome, Pipeline as BasePipeline, StageFn
from partout_pipelines.stages import STAGE_NAMES


DATASET = "nhtsa"


def _importer(ctx: Context) -> NhtsaImporter:
    importer = ctx.shared.get("importer")
    if importer is not None:
        return cast(NhtsaImporter, importer)
    created = NhtsaImporter(ctx.cfg)
    ctx.shared["importer"] = created
    return created


def _has_source(ctx: Context) -> bool:
    source = ctx.cfg.source("vpic")
    return bool(source.get("base_url") or source.get("url"))


def acquire(ctx: Context) -> Outcome:
    if not _has_source(ctx):
        raise RuntimeError("sources.vpic.base_url is required")
    ctx.shared["source_checked"] = True
    return Outcome.COMPLETED


def verify(ctx: Context) -> Outcome:
    if not ctx.shared.get("source_checked"):
        acquire(ctx)
    return Outcome.COMPLETED


def restore(ctx: Context) -> Outcome:
    if ctx.dry_run:
        return Outcome.COMPLETED
    is_validation = ctx.validation_database or (
        ctx.serving_database is not None and ctx.target_database != ctx.serving_database
    )
    if is_validation:
        db.create_database(ctx.cfg, ctx.target_database)
        ctx.shared["validation_database_ready"] = True
    importer = _importer(ctx)
    actual_run_id = importer.run(
        run_id=ctx.run_id,
        datasets=ctx.datasets,
        reconcile=ctx.reconcile,
    )
    ctx.run_id = actual_run_id
    ctx.shared["restore_run_id"] = actual_run_id
    return Outcome.COMPLETED


def schema(ctx: Context) -> Outcome:
    importer = _importer(ctx)
    importer.ensure_metadata()
    importer.ensure_reference_schema()
    return Outcome.COMPLETED


def index(ctx: Context) -> Outcome:
    importer = _importer(ctx)
    importer.ensure_model_stable_key_index()
    return Outcome.COMPLETED


def geocode(ctx: Context) -> Outcome:
    ctx.log.info("geocoding is not applicable to NHTSA reference data")
    return Outcome.SKIPPED


def derive(ctx: Context) -> Outcome:
    ctx.log.info("derived relations are not part of this importer baseline")
    return Outcome.SKIPPED


def analyze(ctx: Context) -> Outcome:
    db.analyze(ctx.cfg, ctx.target_database)
    return Outcome.COMPLETED


def expose(ctx: Context) -> Outcome:
    roles = (ctx.cfg.role("api_role"), ctx.cfg.role("anon_role"))
    try:
        schemas = db.grant_read_access(
            ctx.cfg,
            ctx.target_database,
            roles=roles,
            schemas=ctx.cfg.postgrest.schemas,
        )
    except Exception as exc:
        ctx.log.warning("could not apply PostgREST grants: %s", exc)
        schemas = []
    output = {
        "database": ctx.target_database,
        "roles": list(roles),
        "schemas": schemas,
        "api_url": ctx.cfg.postgrest_url(ctx.dataset),
    }
    ledger_dir = ctx.ledger.path.parent
    ledger_dir.mkdir(parents=True, exist_ok=True)
    (ledger_dir / "exposed.json").write_text(json.dumps(output, indent=2) + "\n")
    return Outcome.COMPLETED


STAGES: dict[str, StageFn] = {
    "00_acquire": acquire,
    "01_verify": verify,
    "02_restore": restore,
    "03_schema": schema,
    "04_index": index,
    "05_geocode": geocode,
    "06_derive": derive,
    "07_analyze": analyze,
    "08_expose": expose,
}


class Pipeline(BasePipeline):
    def __init__(self) -> None:
        super().__init__(
            dataset=DATASET,
            stages=STAGES,
            description="NHTSA/vPIC reference data importer",
        )
        if self.stage_order() != STAGE_NAMES:
            raise ValueError("NHTSA pipeline must expose the canonical stage sequence")

    def stage_00_acquire(self, ctx: Context) -> Outcome:
        return acquire(ctx)

    def stage_01_verify(self, ctx: Context) -> Outcome:
        return verify(ctx)

    def stage_02_restore(self, ctx: Context) -> Outcome:
        return restore(ctx)

    def stage_03_schema(self, ctx: Context) -> Outcome:
        return schema(ctx)

    def stage_04_index(self, ctx: Context) -> Outcome:
        return index(ctx)

    def stage_05_geocode(self, ctx: Context) -> Outcome:
        return geocode(ctx)

    def stage_06_derive(self, ctx: Context) -> Outcome:
        return derive(ctx)

    def stage_07_analyze(self, ctx: Context) -> Outcome:
        return analyze(ctx)

    def stage_08_expose(self, ctx: Context) -> Outcome:
        return expose(ctx)


def build() -> Pipeline:
    return Pipeline()


__all__ = [
    "DATASET",
    "STAGES",
    "STAGE_NAMES",
    "Outcome",
    "Pipeline",
    "acquire",
    "analyze",
    "build",
    "derive",
    "expose",
    "geocode",
    "index",
    "restore",
    "schema",
    "verify",
]
