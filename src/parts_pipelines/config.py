from __future__ import annotations

import math
import os
import tomllib
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

CONFIG_ENV = "PARTS_PIPELINES_CONFIG"
CONFIG_NAME = "parts.toml"
MINIMUM_REQUEST_INTERVAL = 1.0


class ConfigError(RuntimeError):
    pass


@dataclass(frozen=True)
class DatabaseConfig:
    host: str
    port: int
    name: str
    user: str

    def connect_kwargs(self) -> dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.name,
            "user": self.user,
            "connect_timeout": 10,
            "application_name": "parts-pipelines-nhtsa",
        }


@dataclass(frozen=True)
class ApiConfig:
    base_url: str
    request_interval_seconds: float
    timeout_seconds: float
    max_attempts: int
    backoff_seconds: float
    max_backoff_seconds: float
    max_retry_after_seconds: float
    user_agent: str
    all_models_csv_url: str
    equipment_year: int
    equipment_types: tuple[int, ...]


@dataclass(frozen=True)
class Config:
    path: Path
    database: DatabaseConfig
    api: ApiConfig

    def with_request_interval(self, interval: float) -> Config:
        if not math.isfinite(interval) or interval < MINIMUM_REQUEST_INTERVAL:
            raise ConfigError(
                f"request interval must be at least {MINIMUM_REQUEST_INTERVAL:.1f} seconds"
            )
        return replace(self, api=replace(self.api, request_interval_seconds=interval))


def _candidates(explicit: str | Path | None) -> list[Path]:
    paths: list[Path] = []
    if explicit:
        paths.append(Path(explicit).expanduser())
    env_value = os.environ.get(CONFIG_ENV)
    if env_value:
        paths.append(Path(env_value).expanduser())
    paths.append(Path.cwd() / CONFIG_NAME)
    paths.append(Path(__file__).resolve().parents[2] / CONFIG_NAME)
    paths.append(Path.home() / ".config" / "parts-pipelines" / CONFIG_NAME)
    return paths


def _table(data: dict[str, Any], name: str, path: Path) -> dict[str, Any]:
    value = data.get(name)
    if not isinstance(value, dict):
        raise ConfigError(f"{path}: section [{name}] must be a table")
    return value


def _required_string(table: dict[str, Any], key: str, section: str, path: Path) -> str:
    value = table.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{path}: {section}.{key} must be a non-empty string")
    return value.strip()


def _number(
    table: dict[str, Any],
    key: str,
    section: str,
    path: Path,
    *,
    minimum: float,
) -> float:
    value = table.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError(f"{path}: {section}.{key} must be numeric")
    result = float(value)
    if not math.isfinite(result) or result < minimum:
        raise ConfigError(f"{path}: {section}.{key} must be at least {minimum:g}")
    return result


def _integer(
    table: dict[str, Any],
    key: str,
    section: str,
    path: Path,
    *,
    minimum: int,
    maximum: int | None = None,
) -> int:
    value = table.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"{path}: {section}.{key} must be an integer")
    if value < minimum or (maximum is not None and value > maximum):
        bound = f"between {minimum} and {maximum}" if maximum is not None else f"at least {minimum}"
        raise ConfigError(f"{path}: {section}.{key} must be {bound}")
    return value


def _validate_public_https_url(
    value: str,
    key: str,
    path: Path,
    *,
    allow_query: bool = False,
) -> str:
    parsed = urlsplit(value)
    if parsed.scheme != "https" or not parsed.netloc:
        raise ConfigError(f"{path}: api.{key} must be an absolute HTTPS URL")
    if parsed.username or parsed.password:
        raise ConfigError(f"{path}: api.{key} must not contain credentials")
    if not allow_query and (parsed.query or parsed.fragment):
        raise ConfigError(f"{path}: api.{key} must not contain a query or fragment")
    return value.rstrip("/")


def load_config(explicit: str | Path | None = None) -> Config:
    tried = _candidates(explicit)
    selected: Path | None = None
    for candidate in tried:
        if candidate.is_file():
            selected = candidate.resolve()
            break
    if selected is None:
        listing = "\n  ".join(str(candidate) for candidate in tried)
        raise ConfigError(f"no {CONFIG_NAME} found. Looked in:\n  {listing}")
    try:
        with selected.open("rb") as handle:
            data = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise ConfigError(f"cannot read {selected}: {exc}") from exc
    database_data = _table(data, "database", selected)
    api_data = _table(data, "api", selected)
    forbidden_names = {"password", "passfile", "dsn", "connection_string"}
    forbidden = forbidden_names.intersection(
        key.lower() for key in database_data if isinstance(key, str)
    )
    if forbidden:
        names = ", ".join(sorted(forbidden))
        raise ConfigError(f"{selected}: secrets are not allowed in [database]: {names}")
    port = _integer(database_data, "port", "database", selected, minimum=1, maximum=65535)
    request_interval = _number(
        api_data,
        "request_interval_seconds",
        "api",
        selected,
        minimum=MINIMUM_REQUEST_INTERVAL,
    )
    timeout = _number(api_data, "timeout_seconds", "api", selected, minimum=0.1)
    max_attempts = _integer(api_data, "max_attempts", "api", selected, minimum=1, maximum=20)
    backoff = _number(api_data, "backoff_seconds", "api", selected, minimum=0.0)
    max_backoff = _number(
        api_data,
        "max_backoff_seconds",
        "api",
        selected,
        minimum=0.0,
    )
    max_retry_after = _number(
        api_data,
        "max_retry_after_seconds",
        "api",
        selected,
        minimum=0.0,
    )
    if max_backoff < backoff:
        raise ConfigError(
            f"{selected}: api.max_backoff_seconds must be at least api.backoff_seconds"
        )
    base_url = _validate_public_https_url(
        _required_string(api_data, "base_url", "api", selected),
        "base_url",
        selected,
    )
    all_models_url = _validate_public_https_url(
        _required_string(api_data, "all_models_csv_url", "api", selected),
        "all_models_csv_url",
        selected,
        allow_query=True,
    )
    user_agent = _required_string(api_data, "user_agent", "api", selected)
    if "\r" in user_agent or "\n" in user_agent:
        raise ConfigError(f"{selected}: api.user_agent must be a single line")
    equipment_year = _integer(
        api_data,
        "equipment_year",
        "api",
        selected,
        minimum=1980,
        maximum=2200,
    )
    equipment_types_value = api_data.get("equipment_types")
    if not isinstance(equipment_types_value, list):
        raise ConfigError(f"{selected}: api.equipment_types must be an array")
    if any(isinstance(item, bool) or not isinstance(item, int) for item in equipment_types_value):
        raise ConfigError(f"{selected}: api.equipment_types must contain integers")
    equipment_types = tuple(equipment_types_value)
    if equipment_types != (1, 3, 13, 16):
        raise ConfigError(f"{selected}: api.equipment_types must equal [1, 3, 13, 16]")
    database = DatabaseConfig(
        host=_required_string(database_data, "host", "database", selected),
        port=port,
        name=_required_string(database_data, "name", "database", selected),
        user=_required_string(database_data, "user", "database", selected),
    )
    api = ApiConfig(
        base_url=base_url,
        request_interval_seconds=request_interval,
        timeout_seconds=timeout,
        max_attempts=max_attempts,
        backoff_seconds=backoff,
        max_backoff_seconds=max_backoff,
        max_retry_after_seconds=max_retry_after,
        user_agent=user_agent,
        all_models_csv_url=all_models_url,
        equipment_year=equipment_year,
        equipment_types=equipment_types,
    )
    return Config(path=selected, database=database, api=api)
