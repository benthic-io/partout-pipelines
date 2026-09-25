# Migration

This is the first standardization pass for `benthic-io/partout-pipelines`.
The public NHTSA importer remains the behavioral baseline; this pass changes
its repository-facing names and gives it a small, explicit stage contract.

## Package and command names

| Before                                      | Canonical now                                     | Compatibility                                                              |
| ------------------------------------------- | ------------------------------------------------- | -------------------------------------------------------------------------- |
| `src/parts_pipelines`                       | `src/partout_pipelines`                           | Python imports use the new package.                                        |
| `parts-pipelines`                           | `partout`                                         | The old console script remains available.                                  |
| `parts.toml`                                | `partout.toml`                                    | `PARTS_PIPELINES_CONFIG` and `parts.toml` are still searched as fallbacks. |
| `https://github.com/benthic-io/partout-vin` | `https://github.com/benthic-io/partout-pipelines` | The new repository is the source of the importer.                          |

The public endpoint is unchanged. The nginx route remains
`/parts/NHTSA/`, and the database name remains `partout_nhtsa`.

## Configuration migration

Move a controlled copy of `parts.toml` to `partout.toml`, then add the
standardized sections as needed:

```toml
[paths]
root = "/raid_0/partout"

[postgrest]
base_url = "https://benthic.io/parts/NHTSA"

[bdp]
repository_url = "https://github.com/benthic-io/partout-pipelines"

[sources.vpic]
base_url = "https://vpic.nhtsa.dot.gov/api"
```

`PARTOUT_CONFIG` is now the preferred environment variable. Existing jobs can
continue to use `PARTS_PIPELINES_CONFIG` or a local `parts.toml` during the
transition. Passwords, API keys, DSNs, and private signing material must not
be added to either file; use environment or PostgreSQL client authentication.

## Stage contract

`pipelines/nhtsa` now exposes all nine canonical stage names:

1. `00_acquire`
2. `01_verify`
3. `02_restore`
4. `03_schema`
5. `04_index`
6. `05_geocode`
7. `06_derive`
8. `07_analyze`
9. `08_expose`

The existing `NhtsaImporter` still owns API requests, retry/backoff behavior,
response parsing, resumable jobs, reconciliation, and upserts. The wrapper
does not reimplement those behaviors. Schema/metadata setup, stable-key
indexing, analysis, and safe read grants are exposed as stages. Geocoding and
v1 derived relations are explicitly skipped.

The shared `stages`, `ledger`, `db`, and `log` modules are intentionally small.
The ledger is file-backed under `[paths].state`, which keeps stage inspection
and dry runs independent of a database connection. A future pass can move the
ledger into PostgreSQL if the deployment needs the ledger to travel with a
database dump.

## Production safety

The legacy vPIC pipeline could drop and recreate its target database. That
behavior is not carried into this repository. A normal run never drops or
creates `partout_nhtsa`; it uses the existing importer's idempotent upsert path.
Only an explicitly selected validation database can be created by the restore
stage. Operators should use a name such as `partout_nhtsa_validate` and should
never use the serving name for a replacement run.

The stage ledger records skipped stages, so an audit can distinguish “not
applicable” from a missing stage. `--force` is required to rerun a completed
stage.

## Relationship to `partout-vin`

The older `partout-vin` repository remains the historical home of the
full-database vPIC/BDP experiment and its publication assets. This repository
owns the current public NHTSA API importer and presents it through the shared
nine-stage contract. It does not copy the old framework wholesale.

The BDP manifest signer remains in the benthic-site tooling. This pass does
not implement manifest signing, does not add `cryptography`, and does not
publish signed artifacts. `08_expose` only prepares the database grants and
an `exposed.json` handoff for the external signer.

## Follow-up gaps

- Add a production-specific validation database workflow and a documented
  cleanup policy once operators have exercised it against a real PostgreSQL
  cluster.
- Decide whether the stage ledger should be migrated from the state directory
  into PostgreSQL for backup and multi-host coordination.
- Add BDP manifest preparation/signing only in the benthic-site tooling, with
  key handling kept outside this repository.
- Add integration tests for PostgREST grants and a disposable PostgreSQL
  database; the focused tests in this pass are intentionally driver-free.
