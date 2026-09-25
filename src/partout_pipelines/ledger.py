from __future__ import annotations

import json
import os
import re
import socket
import time
from dataclasses import dataclass, field
from typing import Any

from .config import Config


STAGE_NAMES = (
    "00_acquire",
    "01_verify",
    "02_restore",
    "03_schema",
    "04_index",
    "05_geocode",
    "06_derive",
    "07_analyze",
    "08_expose",
)
_COMPLETED = {"completed", "skipped"}


def _safe_name(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", value)
    return cleaned or "default"


@dataclass
class StageTimer:
    ledger: Ledger
    name: str
    started_at: float = field(default_factory=time.time)
    detail: dict[str, Any] = field(default_factory=dict)
    skipped: bool = False
    _finished: bool = False

    @property
    def elapsed(self) -> float:
        return max(0.0, time.time() - self.started_at)

    def note(self, *, skipped: bool = False, **detail: Any) -> None:
        self.skipped = skipped
        self.detail.update(detail)

    def __enter__(self) -> StageTimer:
        return self

    def __exit__(self, exc_type, exc, traceback) -> bool:
        if self._finished:
            return False
        self._finished = True
        if exc is not None:
            self.ledger.finish(
                self,
                "failed",
                error=f"{exc_type.__name__}: {exc}",
            )
        else:
            self.ledger.finish(self, "skipped" if self.skipped else "completed")
        return False


class Ledger:
    def __init__(self, cfg: Config, dataset: str, dbname: str | None = None) -> None:
        known_datasets = {"nhtsa", "vpic", *cfg.sources}
        if dbname in known_datasets and dataset not in known_datasets:
            dataset, dbname = dbname, dataset
        self.cfg = cfg
        self.dataset = dataset
        self.dbname = dbname
        state = cfg.paths.resolve()["state"] / _safe_name(dataset)
        if dbname and dbname != cfg.dbname(dataset):
            state /= _safe_name(dbname)
        self.path = state / "ledger.json"

    def ensure(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self._write({"stages": {}})

    def _read(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"stages": {}}
        try:
            with self.path.open(encoding="utf-8") as handle:
                value = json.load(handle)
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"cannot read pipeline ledger {self.path}: {exc}") from exc
        if not isinstance(value, dict) or not isinstance(value.get("stages"), dict):
            raise RuntimeError(f"pipeline ledger {self.path} has an invalid shape")
        return value

    def _write(self, data: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(f".{self.path.name}.{os.getpid()}.tmp")
        temporary.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
        os.replace(temporary, self.path)

    def clear_state(self) -> int:
        return self.clear_stale()

    def clear_stale(self) -> int:
        data = self._read()
        changed = 0
        for _stage, record in data["stages"].items():
            if isinstance(record, dict) and record.get("status") == "running":
                record["status"] = "failed"
                record["error"] = "process died without recording an outcome"
                record["finished_at"] = time.time()
                changed += 1
        if changed:
            self._write(data)
        return changed

    def last_status(self, stage: str) -> str | None:
        record = self._read()["stages"].get(stage)
        if not isinstance(record, dict):
            return None
        value = record.get("status")
        return value if isinstance(value, str) else None

    def is_completed(self, stage: str) -> bool:
        return self.last_status(stage) in _COMPLETED

    def is_complete(self, stage: str) -> bool:
        return self.is_completed(stage)

    def start(self, stage: str) -> StageTimer:
        data = self._read()
        existing = data["stages"].get(stage)
        started = existing.get("started_at") if isinstance(existing, dict) else None
        data["stages"][stage] = {
            "status": "running",
            "started_at": started if isinstance(started, (int, float)) else time.time(),
            "host": socket.gethostname(),
            "pid": os.getpid(),
        }
        self._write(data)
        return StageTimer(self, stage, started_at=data["stages"][stage]["started_at"])

    def finish(
        self,
        timer: StageTimer,
        status: str,
        *,
        detail: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        if status not in {"completed", "skipped", "failed"}:
            raise ValueError(f"invalid stage status: {status}")
        data = self._read()
        existing = data["stages"].get(timer.name)
        started = existing.get("started_at") if isinstance(existing, dict) else timer.started_at
        record: dict[str, Any] = {
            "status": status,
            "started_at": started,
            "finished_at": time.time(),
            "duration_s": round(timer.elapsed, 3),
            "detail": {**timer.detail, **(detail or {})},
        }
        if error:
            record["error"] = error
        data["stages"][timer.name] = record
        self._write(data)

    def reset(self, stage: str | None = None) -> int:
        data = self._read()
        if stage is None:
            removed = len(data["stages"])
            data["stages"] = {}
        else:
            removed = int(stage in data["stages"])
            data["stages"].pop(stage, None)
        if removed:
            self._write(data)
        return removed

    def history(self) -> list[dict[str, Any]]:
        records = self._read()["stages"]
        return [dict(record) for record in records.values() if isinstance(record, dict)]
