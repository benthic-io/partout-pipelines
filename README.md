# Partout Pipelines

Partout Pipelines builds and maintains the NHTSA/vPIC dataset published at
`https://benthic.io/parts/NHTSA/`. It coordinates the official vPIC decoder
sources, imports reference data into the `partout_nhtsa` PostgreSQL database,
tracks resumable stage state, prepares PostgREST access, and produces the
operational handoff used by the benthic.io BDP publication tooling.

The repository is organized around a nine-stage pipeline contract. Stages are
idempotent, recorded in a durable ledger, and safe to resume after interruption.

## Quick start

```bash
python3 -m venv .venv
.venv/bin/pip install -e .

.venv/bin/partout config
.venv/bin/partout stages
.venv/bin/partout run nhtsa --dry-run
.venv/bin/partout run nhtsa
```

The `partout` command runs the staged pipeline and provides lower-level
commands for operators who need direct access to reference imports and import
state:

```bash
.venv/bin/partout nhtsa-reference
.venv/bin/partout probe
.venv/bin/partout status --run-id <id>
```

## Configuration

The tracked configuration file is `partout.toml`. It contains database, API,
filesystem, PostgREST, BDP, and vPIC source settings, but no passwords, tokens,
private keys, or connection strings. Supply credentials through the environment
or PostgreSQL client configuration.

Configuration is resolved in this order:

1. `--config <path>`
2. `$PARTOUT_CONFIG`
3. `./partout.toml`
4. the repository root
5. `~/.config/partout-pipelines/partout.toml`

The serving database is `partout_nhtsa`, the PostgREST endpoint is
`https://benthic.io/parts/NHTSA/`, and the published repository is
`https://github.com/benthic-io/partout-pipelines`.

## Stage contract

Every run exposes the same ordered sequence. A stage that does not apply is
recorded as skipped rather than omitted.

| Stage        | NHTSA behavior                                                                             |
| ------------ | ------------------------------------------------------------------------------------------ |
| `00_acquire` | Validate the configured vPIC source and official decoder metadata.                         |
| `01_verify`  | Verify the source contract before database work begins.                                    |
| `02_restore` | Run or resume the reference import. The stage upserts into the selected database.          |
| `03_schema`  | Ensure NHTSA import metadata and reference-schema objects.                                 |
| `04_index`   | Ensure the importer's stable-key index.                                                    |
| `05_geocode` | Skipped; NHTSA reference rows do not use Photon geocoding.                                 |
| `06_derive`  | Skipped; the published NHTSA dataset has no derived relations.                             |
| `07_analyze` | Refresh PostgreSQL statistics.                                                             |
| `08_expose`  | Apply read grants where roles and schemas exist and record the PostgREST exposure handoff. |

The file ledger under `[paths].state` makes completed stages resumable. Use
`--force` to rerun a completed stage and `--only` or `--start-at` to select
stages.

## Production database safety

A normal `partout run nhtsa` uses the configured `partout_nhtsa` database and
performs resumable, idempotent importer work. It does not issue
`DROP DATABASE`, `CREATE DATABASE`, or restore over the serving database.

Use `--dbname` for an isolated validation database:

```bash
.venv/bin/partout run nhtsa \
  --only 02_restore \
  --dbname partout_nhtsa_validate \
  --force
```

The restore stage may create an explicitly selected validation database, but it
refuses to treat the configured serving database as a replacement target. Never
point a validation run at `partout_nhtsa`.

## BDP publication boundary

This repository prepares the database, reference tables, read grants, and
PostgREST exposure state. The `benthic-site` BDP tooling owns schema
introspection, manifest and collection signing, summaries, and publication to
`benthic.io/bdp/`.

## Development

```bash
ruff check .
ruff format --check .
python -m unittest discover -s tests -p 'test_*.py' -v
```

The test suite covers configuration validation, NHTSA response handling,
retry and circuit-breaker behavior, normalization, stage mapping, resumable
import behavior, and production database safety.
