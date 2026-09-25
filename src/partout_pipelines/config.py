from __future__ import annotations

import math
import os
import re
import tomllib
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

CONFIG_ENV = "PARTOUT_CONFIG"
LEGACY_CONFIG_ENV = "PARTS_PIPELINES_CONFIG"
CONFIG_NAME = "partout.toml"
LEGACY_CONFIG_NAME = "parts.toml"
CONFIG_DIRECTORY_NAME = "partout-pipelines"
MINIMUM_REQUEST_INTERVAL = 1.0
DEFAULT_REPOSITORY_URL = "https://github.com/benthic-io/partout-pipelines"
_MISSING = object()
_SECRET_KEYS = {
    "api_key",
    "apikey",
    "access_token",
    "auth_token",
    "connection_string",
    "credential",
    "credentials",
    "database_url",
    "db_uri",
    "dsn",
    "passfile",
    "passwd",
    "password",
    "private_key",
    "secret",
    "token",
}


class ConfigError(RuntimeError):
    pass


@dataclass(frozen=True)
class DatabaseConfig:
    host: str
    port: int
    name: str
    user: str
    maintenance_db: str = "postgres"
    targets: dict[str, str] = field(default_factory=dict)

    @property
    def superuser(self) -> str:
        return self.user

    @property
    def db_host(self) -> str:
        return self.host

    @property
    def db_port(self) -> int:
        return self.port

    @property
    def db_superuser(self) -> str:
        return self.user

    def connect_kwargs(self, dbname: str | None = None) -> dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": dbname or self.name,
            "user": self.user,
            "connect_timeout": 10,
            "application_name": "partout-pipelines-nhtsa",
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
class PathsConfig:
    root: Path = Path(".partout")
    archives: str = "archives"
    work: str = "work"
    logs: str = "logs"
    state: str = "state"
    bdp_output: str = "bdp"

    def resolve(self) -> dict[str, Path]:
        root = Path(self.root).expanduser()
        values = {
            "root": root,
            "archives": self.archives,
            "work": self.work,
            "logs": self.logs,
            "state": self.state,
            "bdp_output": self.bdp_output,
        }
        return {
            key: value if isinstance(value, Path) else _path_under(root, value)
            for key, value in values.items()
        }

    def ensure_dirs(self, dataset: str | None = None) -> None:
        resolved = self.resolve()
        for name in ("root", "archives", "work", "logs", "state", "bdp_output"):
            resolved[name].mkdir(parents=True, exist_ok=True)
        if dataset:
            for name in ("archives", "work", "logs", "state"):
                (resolved[name] / dataset).mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class PostgrestConfig:
    base_url: str = "https://benthic.io/parts/NHTSA"
    port: int = 3005
    api_role: str = "api_user"
    anon_role: str = "web_anon"
    schemas: tuple[str, ...] = ("vpic", "api_reference")


@dataclass(frozen=True)
class BdpConfig:
    author_identity: str = "brian@benthic.io"
    collection: str = "parts"
    repository_url: str = DEFAULT_REPOSITORY_URL
    signing_key: str | None = None


@dataclass(frozen=True)
class Config:
    path: Path
    database: DatabaseConfig
    api: ApiConfig
    paths: PathsConfig = field(default_factory=PathsConfig)
    postgrest: PostgrestConfig = field(default_factory=PostgrestConfig)
    bdp: BdpConfig = field(default_factory=BdpConfig)
    sources: dict[str, dict[str, Any]] = field(default_factory=dict)
    data: dict[str, Any] = field(default_factory=dict, repr=False, compare=False)

    def with_request_interval(self, interval: float) -> Config:
        if not math.isfinite(interval) or interval < MINIMUM_REQUEST_INTERVAL:
            raise ConfigError(
                f"request interval must be at least {MINIMUM_REQUEST_INTERVAL:.1f} seconds"
            )
        return replace(self, api=replace(self.api, request_interval_seconds=interval))

    def with_database_name(self, name: str) -> Config:
        return replace(self, database=replace(self.database, name=name))

    def get(self, dotted: str, default: Any = _MISSING) -> Any:
        value: Any = self.data
        for part in dotted.split("."):
            if not isinstance(value, dict) or part not in value:
                if default is _MISSING:
                    raise ConfigError(f"{self.path}: missing key '{dotted}'")
                return default
            value = value[part]
        return value

    def path_for(self, name: str) -> Path:
        return self.paths.resolve()[name]

    def dataset_dir(self, kind: str, dataset: str) -> Path:
        return self.path_for(kind) / dataset

    def ensure_dirs(self, dataset: str | None = None) -> None:
        self.paths.ensure_dirs(dataset)

    def dbname(self, dataset: str = "nhtsa") -> str:
        return self.database.targets.get(dataset, self.database.name)

    def role(self, name: str) -> str:
        roles = self.get("database.roles", {})
        if isinstance(roles, dict) and isinstance(roles.get(name), str):
            return roles[name]
        if name == "api_role":
            return self.postgrest.api_role
        if name == "anon_role":
            return self.postgrest.anon_role
        raise ConfigError(f"{self.path}: missing database role '{name}'")

    @property
    def db_host(self) -> str:
        return self.database.host

    @property
    def db_port(self) -> int:
        return self.database.port

    @property
    def db_superuser(self) -> str:
        return self.database.user

    @property
    def maintenance_db(self) -> str:
        return self.database.maintenance_db

    def postgrest_port(self, dataset: str = "nhtsa") -> int:
        return self.postgrest.port

    def postgrest_url(self, dataset: str = "nhtsa") -> str:
        base = self.postgrest.base_url.rstrip("/")
        if base.rsplit("/", 1)[-1].lower() == dataset.lower():
            return f"{base}/"
        return f"{base}/{dataset}/"

    def source(self, dataset: str) -> dict[str, Any]:
        return dict(self.sources.get(dataset, {}))

    def secret(self, env_name: str) -> str | None:
        return os.environ.get(env_name) or None


def _path_under(root: Path, value: str) -> Path:
    path = Path(value).expanduser()
    return path if path.is_absolute() else root / path


def _candidates(explicit: str | Path | None) -> list[Path]:
    paths: list[Path] = []

    def add(value: str | Path) -> None:
        path = Path(value).expanduser()
        if path not in paths:
            paths.append(path)

    if explicit:
        add(explicit)
    for env_name in (CONFIG_ENV, LEGACY_CONFIG_ENV):
        value = os.environ.get(env_name)
        if value:
            add(value)
    for directory in (
        Path.cwd(),
        Path(__file__).resolve().parents[2],
        Path.home() / ".config" / CONFIG_DIRECTORY_NAME,
    ):
        add(directory / CONFIG_NAME)
        add(directory / LEGACY_CONFIG_NAME)
    add(Path.home() / ".config" / "parts-pipelines" / LEGACY_CONFIG_NAME)
    return paths


def _table(
    data: dict[str, Any],
    name: str,
    path: Path,
    *,
    required: bool = True,
) -> dict[str, Any]:
    value = data.get(name)
    if value is None and not required:
        return {}
    if not isinstance(value, dict):
        raise ConfigError(f"{path}: section [{name}] must be a table")
    return value


def _required_string(table: dict[str, Any], key: str, section: str, path: Path) -> str:
    value = table.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{path}: {section}.{key} must be a non-empty string")
    return value.strip()


def _optional_string(
    table: dict[str, Any],
    key: str,
    default: str,
    section: str,
    path: Path,
) -> str:
    if key not in table:
        return default
    return _required_string(table, key, section, path)


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
    section: str,
    *,
    allow_query: bool = False,
) -> str:
    parsed = urlsplit(value)
    if parsed.scheme != "https" or not parsed.netloc:
        raise ConfigError(f"{path}: {section}.{key} must be an absolute HTTPS URL")
    if parsed.username or parsed.password:
        raise ConfigError(f"{path}: {section}.{key} must not contain credentials")
    if not allow_query and (parsed.query or parsed.fragment):
        raise ConfigError(f"{path}: {section}.{key} must not contain a query or fragment")
    return value.rstrip("/")


def _secret_keys(value: Any) -> list[str]:
    found: list[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            normalized = re.sub(r"[^a-z0-9]+", "_", str(key).lower()).strip("_")
            if normalized in _SECRET_KEYS:
                found.append(str(key))
            found.extend(_secret_keys(child))
    elif isinstance(value, list):
        for child in value:
            found.extend(_secret_keys(child))
    return found


def _parse_paths(data: dict[str, Any], path: Path) -> PathsConfig:
    table = _table(data, "paths", path, required=False)
    root = _optional_string(table, "root", ".partout", "paths", path)
    return PathsConfig(
        root=Path(root).expanduser(),
        archives=_optional_string(table, "archives", "archives", "paths", path),
        work=_optional_string(table, "work", "work", "paths", path),
        logs=_optional_string(table, "logs", "logs", "paths", path),
        state=_optional_string(table, "state", "state", "paths", path),
        bdp_output=_optional_string(table, "bdp_output", "bdp", "paths", path),
    )


def _parse_database(data: dict[str, Any], path: Path) -> DatabaseConfig:
    table = _table(data, "database", path)
    port = _integer(table, "port", "database", path, minimum=1, maximum=65535)
    user = table.get("user", table.get("superuser"))
    if not isinstance(user, str) or not user.strip():
        raise ConfigError(f"{path}: database.user or database.superuser is required")
    targets_data = table.get("targets", {})
    if not isinstance(targets_data, dict):
        raise ConfigError(f"{path}: section [database.targets] must be a table")
    targets = {
        str(key): str(value)
        for key, value in targets_data.items()
        if isinstance(value, str) and value.strip()
    }
    return DatabaseConfig(
        host=_required_string(table, "host", "database", path),
        port=port,
        name=_required_string(table, "name", "database", path),
        user=user.strip(),
        maintenance_db=_optional_string(table, "maintenance_db", "postgres", "database", path),
        targets=targets,
    )


def _source_api_table(data: dict[str, Any], path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    api = _table(data, "api", path, required=False)
    sources = _table(data, "sources", path, required=False)
    source = sources.get("vpic", {})
    if not isinstance(source, dict):
        raise ConfigError(f"{path}: section [sources.vpic] must be a table")
    return api, dict(source)


def _api_value(
    api: dict[str, Any],
    source: dict[str, Any],
    key: str,
    *,
    aliases: tuple[str, ...] = (),
    default: Any = _MISSING,
) -> Any:
    for table in (api, source):
        for candidate in (key, *aliases):
            if candidate in table:
                return table[candidate]
    if default is not _MISSING:
        return default
    raise ConfigError(f"api.{key} or sources.vpic.{key} is required")


def _parse_api(data: dict[str, Any], path: Path) -> tuple[ApiConfig, dict[str, Any]]:
    api, source = _source_api_table(data, path)
    merged = dict(api)
    merged.update(source)
    if "url" in source:
        merged["base_url"] = source["url"]
    elif "base_url" not in merged and "url" in merged:
        merged["base_url"] = merged["url"]
    request_interval = _number(
        merged,
        "request_interval_seconds",
        "api",
        path,
        minimum=MINIMUM_REQUEST_INTERVAL,
    )
    timeout = _number(merged, "timeout_seconds", "api", path, minimum=0.1)
    max_attempts = _integer(merged, "max_attempts", "api", path, minimum=1, maximum=20)
    backoff = _number(merged, "backoff_seconds", "api", path, minimum=0.0)
    max_backoff = _number(merged, "max_backoff_seconds", "api", path, minimum=0.0)
    max_retry_after = _number(merged, "max_retry_after_seconds", "api", path, minimum=0.0)
    if max_backoff < backoff:
        raise ConfigError(f"{path}: api.max_backoff_seconds must be at least api.backoff_seconds")
    base_value = _api_value(source, api, "base_url", aliases=("url",))
    all_models_value = _api_value(source, api, "all_models_csv_url")
    if not isinstance(base_value, str) or not isinstance(all_models_value, str):
        raise ConfigError(f"{path}: API URLs must be strings")
    base_url = _validate_public_https_url(base_value, "base_url", path, "api")
    all_models_url = _validate_public_https_url(
        all_models_value,
        "all_models_csv_url",
        path,
        "api",
        allow_query=True,
    )
    user_agent = _api_value(source, api, "user_agent")
    if not isinstance(user_agent, str) or not user_agent.strip():
        raise ConfigError(f"{path}: api.user_agent must be a non-empty string")
    user_agent = user_agent.strip()
    if "\r" in user_agent or "\n" in user_agent:
        raise ConfigError(f"{path}: api.user_agent must be a single line")
    equipment_year = _integer(merged, "equipment_year", "api", path, minimum=1980, maximum=2200)
    equipment_types_value = _api_value(source, api, "equipment_types")
    if not isinstance(equipment_types_value, list):
        raise ConfigError(f"{path}: api.equipment_types must be an array")
    if any(isinstance(item, bool) or not isinstance(item, int) for item in equipment_types_value):
        raise ConfigError(f"{path}: api.equipment_types must contain integers")
    equipment_types = tuple(equipment_types_value)
    if equipment_types != (1, 3, 13, 16):
        raise ConfigError(f"{path}: api.equipment_types must equal [1, 3, 13, 16]")
    parsed = ApiConfig(
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
    normalized = dict(source)
    normalized.update(
        {
            "url": base_url,
            "base_url": base_url,
            "request_interval_seconds": request_interval,
            "timeout_seconds": timeout,
            "max_attempts": max_attempts,
            "backoff_seconds": backoff,
            "max_backoff_seconds": max_backoff,
            "max_retry_after_seconds": max_retry_after,
            "user_agent": user_agent,
            "all_models_csv_url": all_models_url,
            "equipment_year": equipment_year,
            "equipment_types": list(equipment_types),
        }
    )
    return parsed, normalized


def _parse_postgrest(data: dict[str, Any], path: Path) -> PostgrestConfig:
    table = _table(data, "postgrest", path, required=False)
    database_table = data.get("database", {})
    role_table = database_table.get("roles", {}) if isinstance(database_table, dict) else {}
    if not isinstance(role_table, dict):
        role_table = {}
    api_role_default = role_table.get("api_role", "api_user")
    anon_role_default = role_table.get("anon_role", "web_anon")
    if not isinstance(api_role_default, str) or not api_role_default.strip():
        api_role_default = "api_user"
    if not isinstance(anon_role_default, str) or not anon_role_default.strip():
        anon_role_default = "web_anon"
    base_url = _optional_string(
        table, "base_url", "https://benthic.io/parts/NHTSA", "postgrest", path
    )
    base_url = _validate_public_https_url(base_url, "base_url", path, "postgrest")
    port_value = table.get("port")
    ports = table.get("ports", {})
    if port_value is None and isinstance(ports, dict):
        port_value = ports.get("nhtsa")
    port = (
        _integer({"port": port_value}, "port", "postgrest", path, minimum=1, maximum=65535)
        if port_value is not None
        else 3005
    )
    schemas_value = table.get("schemas", ["vpic", "api_reference"])
    if not isinstance(schemas_value, list) or not schemas_value:
        raise ConfigError(f"{path}: postgrest.schemas must be a non-empty array")
    if any(not isinstance(item, str) or not item.strip() for item in schemas_value):
        raise ConfigError(f"{path}: postgrest.schemas must contain non-empty strings")
    return PostgrestConfig(
        base_url=base_url,
        port=port,
        api_role=_optional_string(
            table,
            "api_role",
            api_role_default,
            "postgrest",
            path,
        ),
        anon_role=_optional_string(
            table,
            "anon_role",
            anon_role_default,
            "postgrest",
            path,
        ),
        schemas=tuple(item.strip() for item in schemas_value),
    )


def _parse_bdp(data: dict[str, Any], path: Path) -> BdpConfig:
    table = _table(data, "bdp", path, required=False)
    repository_url = _optional_string(table, "repository_url", DEFAULT_REPOSITORY_URL, "bdp", path)
    repository_url = _validate_public_https_url(repository_url, "repository_url", path, "bdp")
    signing_key = table.get("signing_key")
    if signing_key is not None and (not isinstance(signing_key, str) or not signing_key.strip()):
        raise ConfigError(f"{path}: bdp.signing_key must be a path when provided")
    if isinstance(signing_key, str) and ("\n" in signing_key or "BEGIN" in signing_key):
        raise ConfigError(f"{path}: bdp.signing_key must not contain key material")
    return BdpConfig(
        author_identity=_optional_string(table, "author_identity", "brian@benthic.io", "bdp", path),
        collection=_optional_string(table, "collection", "parts", "bdp", path),
        repository_url=repository_url,
        signing_key=signing_key.strip() if isinstance(signing_key, str) else None,
    )


def load_config(explicit: str | Path | None = None) -> Config:
    tried = _candidates(explicit)
    selected: Path | None = None
    for candidate in tried:
        if candidate.is_file():
            selected = candidate.resolve()
            break
    if selected is None:
        listing = "\n  ".join(str(candidate) for candidate in tried)
        raise ConfigError(
            f"no {CONFIG_NAME} or {LEGACY_CONFIG_NAME} found. Looked in:\n  {listing}"
        )
    try:
        with selected.open("rb") as handle:
            data = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise ConfigError(f"cannot read {selected}: {exc}") from exc
    forbidden = sorted(set(_secret_keys(data)))
    if forbidden:
        names = ", ".join(forbidden)
        raise ConfigError(f"{selected}: secrets are not allowed in configuration: {names}")
    database = _parse_database(data, selected)
    api, source = _parse_api(data, selected)
    paths = _parse_paths(data, selected)
    postgrest = _parse_postgrest(data, selected)
    bdp = _parse_bdp(data, selected)
    sources = {"vpic": source}
    return Config(
        path=selected,
        database=database,
        api=api,
        paths=paths,
        postgrest=postgrest,
        bdp=bdp,
        sources=sources,
        data=data,
    )
