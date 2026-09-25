# Partout Pipelines

`partout-pipelines` is the canonical repository for the resumable NHTSA/vPIC
reference-data importer used by benthic.io. It owns the Python importer and
the nine-stage pipeline contract; it does not replace the benthic-site BDP
signing tooling.

The existing public API remains at `/parts/NHTSA/`, and the serving database
remains `partout_nhtsa`.

## Quick start

```bash
python3 -m venv .venv
.venv/bin/pip install -e .
.venv/bin/partout config
.venv/bin/partout stages
.venv/bin/partout run nhtsa --dry-run
.venv/bin/partout run nhtsa
```

`partout` is the canonical command. `parts-pipelines` remains as a
compatibility console script and points at the same entry point.

The direct importer commands remain available for operators who need the
original behavior:

```bash
.venv/bin/partout nhtsa-reference
.venv/bin/partout probe
.venv/bin/partout status --run-id <id>
```

## Configuration

The tracked configuration is `partout.toml`. It is safe to commit because it
contains no passwords, tokens, private keys, or connection strings. Supply
credentials through the environment or the PostgreSQL client configuration.

Configuration resolution is:

1. `--config <path>`
2. `$PARTOUT_CONFIG`
3. `$PARTS_PIPELINES_CONFIG` (legacy)
4. `./partout.toml`
5. `./parts.toml` (legacy)
6. the repository and user configuration directories

The `[database]`, `[api]`, and `[sources.vpic]` values retain the public NHTSA
API settings and the `partout_nhtsa` database name. `[paths]`, `[postgrest]`,
and `[bdp]` provide the shared NGOpen-style locations and publication
metadata. The BDP repository URL is
`https://github.com/benthic-io/partout-pipelines`.

## Stage contract

Every run exposes the same ordered sequence. A stage that does not apply is
recorded as skipped rather than omitted.

| Stage        | NHTSA behavior                                                                                                                |
| ------------ | ----------------------------------------------------------------------------------------------------------------------------- |
| `00_acquire` | Validate the configured vPIC source.                                                                                          |
| `01_verify`  | Recheck the source contract; response verification remains in the importer.                                                   |
| `02_restore` | Run or resume the existing importer. It upserts into an existing serving database; it never drops or recreates that database. |
| `03_schema`  | Ensure NHTSA metadata and reference-schema objects.                                                                           |
| `04_index`   | Ensure the importer's stable-key index.                                                                                       |
| `05_geocode` | Explicitly `SKIPPED`; NHTSA reference rows do not use Photon geocoding.                                                       |
| `06_derive`  | Explicitly `SKIPPED`; no derived relations are part of the v1 baseline.                                                       |
| `07_analyze` | Refresh PostgreSQL statistics.                                                                                                |
| `08_expose`  | Apply read grants where roles and schemas exist and record the exposure handoff. It does not sign a BDP manifest.             |

The file ledger under `[paths].state` makes completed stages resumable. Use
`--force` to intentionally rerun a completed stage and `--only` or
`--start-at` to select stages. A disposable validation run looks like:

```bash
.venv/bin/partout run nhtsa --only 02_restore --dbname partout_nhtsa_validate --force
```

## Production database safety

A normal `partout run nhtsa` uses the configured `partout_nhtsa` database and
performs the existing resumable, idempotent importer work. It does not issue
`DROP DATABASE`, `CREATE DATABASE`, or a restore over the serving database.

A separate database can be supplied with `--dbname` for validation. The
`02_restore` stage may create that validation database when it is explicitly
selected; it still refuses to treat the configured serving name as a
replacement target. Do not point a validation run at `partout_nhtsa`.

The benthic-site BDP tooling remains responsible for manifest signing and
publication. This repository only prepares the database, read grants, and
`exposed.json` handoff.

## Development

```bash
ruff check .
ruff format --check .
python -m unittest discover -s tests -p 'test_*.py' -v
```

The importer tests continue to cover NHTSA response handling, retry behavior,
normalization, and the original database behavior.

## Migration

See [MIGRATION.md](MIGRATION.md) for the package/config rename, stage
contract, compatibility notes, and the relationship to the legacy
`partout-vin` work.
