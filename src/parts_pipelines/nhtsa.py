from __future__ import annotations

import csv
import hashlib
import io
import json
import random
import re
import time
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, date, datetime
from email.utils import parsedate_to_datetime
from types import TracebackType
from typing import Any, Self
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

import psycopg2
import requests
from psycopg2.extras import Json, execute_values

from .config import ApiConfig, Config

DATASETS = (
    \"manufacturers\",
    \"vehicle_variables\",
    \"variable_values\",
    \"wmi_codes\",
    \"models_historical\",
    \"equipment_plants\",
)
PROGRESS_PREFIX = \"nhtsa_\"

MANUFACTURER_SQL = \"\"\"
INSERT INTO api_reference.manufacturers (
    manufacturer_id,
    manufacturer_name,
    manufacturer_common_name,
    primary_product,
    vehicle_types,
    last_updated,
    created_at
) VALUES %s
ON CONFLICT (manufacturer_id) DO UPDATE SET
    manufacturer_name = EXCLUDED.manufacturer_name,
    manufacturer_common_name = EXCLUDED.manufacturer_common_name,
    primary_product = EXCLUDED.primary_product,
    vehicle_types = EXCLUDED.vehicle_types,
    last_updated = EXCLUDED.last_updated
\"\"\"

VEHICLE_VARIABLE_SQL = \"\"\"
INSERT INTO api_reference.vehicle_variables (
    variable_id,
    variable_name,
    variable_group_name,
    data_type,
    description,
    last_updated,
    created_at
) VALUES %s
ON CONFLICT (variable_id) DO UPDATE SET
    variable_name = EXCLUDED.variable_name,
    variable_group_name = EXCLUDED.variable_group_name,
    data_type = EXCLUDED.data_type,
    description = EXCLUDED.description,
    last_updated = EXCLUDED.last_updated
\"\"\"

VARIABLE_VALUE_SQL = \"\"\"
INSERT INTO api_reference.variable_values (
    variable_id,
    value_id,
    value,
    description,
    last_updated,
    created_at
) VALUES %s
ON CONFLICT (variable_id, value_id) DO UPDATE SET
    value = EXCLUDED.value,
    description = EXCLUDED.description,
    last_updated = EXCLUDED.last_updated
\"\"\"

WMI_SQL = \"\"\"
INSERT INTO api_reference.wmi_codes (
    wmi,
    manufacturer_id,
    brand_name,
    vehicle_type_id,
    vehicle_type_name,
    country,
    date_available_to_public,
    last_updated,
    created_at
) VALUES %s
ON CONFLICT (wmi) DO UPDATE SET
    manufacturer_id = EXCLUDED.manufacturer_id,
    brand_name = COALESCE(EXCLUDED.brand_name, api_reference.wmi_codes.brand_name),
    vehicle_type_id = COALESCE(
        EXCLUDED.vehicle_type_id,
        api_reference.wmi_codes.vehicle_type_id
    ),
    vehicle_type_name = COALESCE(
        EXCLUDED.vehicle_type_name,
        api_reference.wmi_codes.vehicle_type_name
    ),
    country = COALESCE(EXCLUDED.country, api_reference.wmi_codes.country),
    date_available_to_public = COALESCE(
        EXCLUDED.date_available_to_public,
        api_reference.wmi_codes.date_available_to_public
    ),
    last_updated = EXCLUDED.last_updated
\"\"\"

MODEL_SQL = \"\"\"
INSERT INTO api_reference.models_historical (
    make_id,
    make_name,
    model_id,
    model_name,
    year,
    last_updated,
    created_at
) VALUES %s
ON CONFLICT (make_id, model_id, (COALESCE(year, -1))) DO UPDATE SET
    make_name = EXCLUDED.make_name,
    model_name = EXCLUDED.model_name,
    last_updated = EXCLUDED.last_updated
\"\"\"

EQUIPMENT_SQL = \"\"\"
INSERT INTO api_reference.equipment_plants (
    equipment_type,
    equipment_type_name,
    plant_year,
    dot_code,
    old_dot_code,
    plant_name,
    plant_address,
    plant_city,
    plant_state,
    plant_postal_code,
    plant_country,
    plant_status,
    plant_phone,
    source_updated_at,
    created_at,
    last_updated
) VALUES %s
ON CONFLICT (equipment_type, plant_year, dot_code) DO UPDATE SET
    equipment_type_name = EXCLUDED.equipment_type_name,
    old_dot_code = EXCLUDED.old_dot_code,
    plant_name = EXCLUDED.plant_name,
    plant_address = EXCLUDED.plant_address,
    plant_city = EXCLUDED.plant_city,
    plant_state = EXCLUDED.plant_state,
    plant_postal_code = EXCLUDED.plant_postal_code,
    plant_country = EXCLUDED.plant_country,
    plant_status = EXCLUDED.plant_status,
    plant_phone = EXCLUDED.plant_phone,
    source_updated_at = EXCLUDED.source_updated_at,
    last_updated = EXCLUDED.last_updated
\"\"\"

METADATA_SQL = (
    \"CREATE SCHEMA IF NOT EXISTS nhtsa_import\",
    \"\"\"
    CREATE TABLE IF NOT EXISTS nhtsa_import.sync_runs (
        run_id TEXT PRIMARY KEY,
        datasets TEXT[] NOT NULL,
        status TEXT NOT NULL,
        reconcile_enabled BOOLEAN NOT NULL,
        requested_rate DOUBLE PRECISION NOT NULL,
        total_jobs INTEGER NOT NULL DEFAULT 0,
        completed_jobs INTEGER NOT NULL DEFAULT 0,
        row_count BIGINT NOT NULL DEFAULT 0,
        error TEXT,
        started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        finished_at TIMESTAMPTZ
    )
    \"\"\",
    \"\"\"
    CREATE TABLE IF NOT EXISTS nhtsa_import.sync_jobs (
        run_id TEXT NOT NULL REFERENCES nhtsa_import.sync_runs(run_id) ON DELETE CASCADE,
        job_key TEXT NOT NULL,
        dataset TEXT NOT NULL,
        request_url TEXT NOT NULL,
        request_options JSONB NOT NULL DEFAULT '{}'::jsonb,
        status TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        http_status INTEGER,
        row_count INTEGER,
        response_sha256 CHAR(64),
        response_body TEXT,
        response_json JSONB,
        error TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        started_at TIMESTAMPTZ,
        completed_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        PRIMARY KEY (run_id, job_key)
    )
    \"\"\",
    \"\"\"
    CREATE TABLE IF NOT EXISTS nhtsa_import.reconciliation_keys (
        run_id TEXT NOT NULL REFERENCES nhtsa_import.sync_runs(run_id) ON DELETE CASCADE,
        dataset TEXT NOT NULL,
        stable_key JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        PRIMARY KEY (run_id, dataset, stable_key)
    )
    \"\"\",
    \"CREATE INDEX IF NOT EXISTS sync_jobs_dataset_status_idx ON nhtsa_import.sync_jobs(dataset, status)\",
    \"CREATE INDEX IF NOT EXISTS sync_jobs_run_url_idx ON nhtsa_import.sync_jobs(run_id, request_url)\",
    \"CREATE INDEX IF NOT EXISTS reconciliation_keys_dataset_idx ON nhtsa_import.reconciliation_keys(run_id, dataset)\",
)

EQUIPMENT_CREATE_SQL = \"\"\"
CREATE TABLE api_reference.equipment_plants (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    equipment_type INTEGER NOT NULL CHECK (equipment_type > 0),
    equipment_type_name TEXT,
    plant_year INTEGER NOT NULL CHECK (plant_year BETWEEN 1980 AND 2200),
    dot_code TEXT NOT NULL CHECK (btrim(dot_code) <> ''),
    old_dot_code TEXT,
    plant_name TEXT,
    plant_address TEXT,
    plant_city TEXT,
    plant_state TEXT,
    plant_postal_code TEXT,
    plant_country TEXT,
    plant_status TEXT,
    plant_phone TEXT,
    source_updated_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    last_updated TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (equipment_type, plant_year, dot_code)
)
\"\"\"

DATASET_TABLES = {
    \"manufacturers\": \"manufacturers\",
    \"vehicle_variables\": \"vehicle_variables\",
    \"variable_values\": \"variable_values\",
    \"wmi_codes\": \"wmi_codes\",
    \"models_historical\": \"models_historical\",
    \"equipment_plants\": \"equipment_plants\",
}

DATASET_SQL = {
    \"manufacturers\": MANUFACTURER_SQL,
    \"vehicle_variables\": VEHICLE_VARIABLE_SQL,
    \"variable_values\": VARIABLE_VALUE_SQL,
    \"wmi_codes\": WMI_SQL,
    \"models_historical\": MODEL_SQL,
    \"equipment_plants\": EQUIPMENT_SQL,
}

DATASET_COLUMNS = {
    \"manufacturers\": (
        \"manufacturer_id\",
        \"manufacturer_name\",
        \"manufacturer_common_name\",
        \"primary_product\",
        \"vehicle_types\",
        \"last_updated\",
        \"created_at\",
    ),
    \"vehicle_variables\": (
        \"variable_id\",
        \"variable_name\",
        \"variable_group_name\",
        \"data_type\",
        \"description\",
        \"last_updated\",
        \"created_at\",
    ),
    \"variable_values\": (
        \"variable_id\",
        \"value_id\",
        \"value\",
        \"description\",
        \"last_updated\",
        \"created_at\",
    ),
    \"wmi_codes\": (
        \"wmi\",
        \"manufacturer_id\",
        \"brand_name\",
        \"vehicle_type_id\",
        \"vehicle_type_name\",
        \"country\",
        \"date_available_to_public\",
        \"last_updated\",
        \"created_at\",
    ),
    \"models_historical\": (
        \"make_id\",
        \"make_name\",
        \"model_id\",
        \"model_name\",
        \"year\",
        \"last_updated\",
        \"created_at\",
    ),
    \"equipment_plants\": (
        \"equipment_type\",
        \"equipment_type_name\",
        \"plant_year\",
        \"dot_code\",
        \"old_dot_code\",
        \"plant_name\",
        \"plant_address\",
        \"plant_city\",
        \"plant_state\",
        \"plant_postal_code\",
        \"plant_country\",
        \"plant_status\",
        \"plant_phone\",
        \"source_updated_at\",
        \"created_at\",
        \"last_updated\",
    ),
}

EQUIPMENT_REQUIRED_COLUMNS = {
    column
    for column in DATASET_COLUMNS[\"equipment_plants\"]
    if column not in {\"created_at\", \"last_updated\"}
}


class ImporterError(RuntimeError):
    pass


class DataError(ImporterError):
    pass


@dataclass(frozen=True)
class FetchFailure:
    url: str
    attempts: int
    http_status: int | None
    response_body: str
    response_sha256: str
    error: str


class ApiUnavailableError(ImporterError):
    def __init__(self, failure: FetchFailure):
        super().__init__(failure.error)
        self.failure = failure


class CircuitBreakerOpen(ImporterError):
    def __init__(self, failure: FetchFailure):
        super().__init__(failure.error)
        self.failure = failure


@dataclass(frozen=True)
class FetchResult:
    url: str
    attempts: int
    http_status: int
    response_body: str
    response_sha256: str
    payload: Any | None
    latency_seconds: float


@dataclass(frozen=True)
class JobOutcome:
    row_count: int
    response_sha256: str | None


@dataclass(frozen=True)
class ProbeResult:
    url: str
    http_status: int | None
    latency_seconds: float | None
    response_bytes: int
    count: int | None
    message: str | None
    error: str | None


def _text(value: Any) -> str | None:
    if value is None:
        return None
    result = str(value).strip()
    return result or None


def _integer(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    cleaned = _text(value)
    if cleaned is None:
        return None
    try:
        return int(cleaned)
    except ValueError:
        return None


def _first(record: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in record and record[key] not in (None, \"\"):
            return record[key]
    return None


def parse_microsoft_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    match = re.fullmatch(r\"/Date\\((-?\\d+)\\)/?\", value.strip())
    if match is None:
        return None
    try:
        milliseconds = int(match.group(1))
        return datetime.fromtimestamp(milliseconds / 1000, tz=UTC)
    except (OverflowError, OSError, ValueError):
        return None


def extract_results(payload: Any, context: str) -> list[dict[str, Any]]:
    if not isinstance(payload, dict) or not payload:
        raise DataError(f\"{context}: empty or invalid API envelope\")
    errors = payload.get(\"Errors\")
    if isinstance(errors, list) and errors:
        raise DataError(f\"{context}: API errors: {json.dumps(errors, default=str)}\")
    error = payload.get(\"Error\")
    if error not in (None, \"\", [], {}):
        raise DataError(f\"{context}: API error: {error}\")
    results = payload.get(\"Results\")
    if results is None:
        count = payload.get(\"Count\")
        if count == 0:
            return []
        raise DataError(f\"{context}: API envelope has no Results array\")
    if not isinstance(results, list):
        raise DataError(f\"{context}: API Results must be an array\")
    if any(not isinstance(item, dict) for item in results):
        raise DataError(f\"{context}: API Results contains a non-object row\")
    return results


def should_stop_manufacturer_pagination(results: Sequence[Mapping[str, Any]]) -> bool:
    return len(results) == 0


def normalize_equipment_plant(
    record: Mapping[str, Any],
    equipment_type: int,
    equipment_year: int,
) -> dict[str, Any]:
    if not isinstance(record, Mapping):
        raise DataError(\"equipment plant row must be an object\")
    dot_code = _text(_first(record, \"DOTCode\", \"DotCode\", \"PlantCode\"))
    if dot_code is None:
        raise DataError(\"equipment plant row is missing DOTCode\")
    row_type = _integer(_first(record, \"EquipmentType\", \"EquipmentTypeCode\")) or equipment_type
    row_year = _integer(_first(record, \"Year\", \"ModelYear\", \"PlantYear\")) or equipment_year
    if row_type <= 0:
        raise DataError(\"equipment type must be positive\")
    if not 1980 <= row_year <= 2200:
        raise DataError(f\"invalid equipment plant year: {row_year}\")
    return {
        \"equipment_type\": row_type,
        \"equipment_type_name\": _text(
            _first(record, \"EquipmentTypeDescription\", \"EquipmentTypeName\")
        ),
        \"plant_year\": row_year,
        \"dot_code\": dot_code,
        \"old_dot_code\": _text(_first(record, \"OldDotCode\", \"OldDOTCode\")),
        \"plant_name\": _text(_first(record, \"Name\", \"PlantName\")),
        \"plant_address\": _text(_first(record, \"Address\", \"PlantAddress\")),
        \"plant_city\": _text(_first(record, \"City\", \"PlantCity\")),
        \"plant_state\": _text(_first(record, \"StateProvince\", \"State\", \"PlantState\")),
        \"plant_postal_code\": _text(_first(record, \"PostalCode\", \"PlantPostalCode\")),
        \"plant_country\": _text(_first(record, \"Country\", \"PlantCountry\")),
        \"plant_status\": _text(_first(record, \"Status\", \"PlantStatus\")),
        \"plant_phone\": _text(_first(record, \"Phone\", \"PlantPhone\", \"Telephone\")),
        \"source_updated_at\": parse_microsoft_timestamp(
            _first(record, \"LastUpdated\", \"Updated\", \"DateUpdated\")
        ),
    }


def _normalize_manufacturer(record: Mapping[str, Any]) -> dict[str, Any]:
    manufacturer_id = _integer(_first(record, \"Mfr_ID\", \"MfrId\", \"Id\", \"ManufacturerId\"))
    if manufacturer_id is None:
        raise DataError(\"manufacturer row is missing Mfr_ID\")
    vehicle_types = record.get(\"VehicleTypes\")
    if vehicle_types is None:
        vehicle_types = []
    if not isinstance(vehicle_types, list):
        vehicle_types = [vehicle_types]
    return {
        \"manufacturer_id\": manufacturer_id,
        \"manufacturer_name\": _text(_first(record, \"Mfr_Name\", \"ManufacturerName\")),
        \"manufacturer_common_name\": _text(
            _first(record, \"Mfr_CommonName\", \"ManufacturerCommonName\")
        ),
        \"primary_product\": _text(_first(record, \"PrimaryProduct\", \"Mfr_PrimaryProduct\")),
        \"vehicle_types\": vehicle_types,
    }


def _normalize_vehicle_variable(record: Mapping[str, Any]) -> dict[str, Any]:
    variable_id = _integer(_first(record, \"Id\", \"ID\", \"VariableId\", \"Variable_ID\"))
    if variable_id is None:
        raise DataError(\"vehicle variable row is missing Id\")
    return {
        \"variable_id\": variable_id,
        \"variable_name\": _text(_first(record, \"Name\", \"VariableName\")),
        \"variable_group_name\": _text(_first(record, \"GroupName\", \"VariableGroupName\")),
        \"data_type\": _text(_first(record, \"DataType\", \"VariableDataType\")),
        \"description\": _text(_first(record, \"Description\", \"VariableDescription\")),
    }


def _normalize_variable_value(record: Mapping[str, Any], variable_id: int) -> dict[str, Any]:
    value_id = _integer(_first(record, \"Id\", \"ID\", \"ValueId\", \"Value_ID\"))
    if value_id is None:
        raise DataError(f\"variable value row for {variable_id} is missing Id\")
    value = _text(_first(record, \"Value\", \"Name\"))
    return {
        \"variable_id\": variable_id,
        \"value_id\": value_id,
        \"value\": value,
        \"description\": _text(_first(record, \"Description\", \"ValueDescription\")),
    }


def _normalize_wmi(record: Mapping[str, Any], manufacturer_id: int | None = None) -> dict[str, Any]:
    wmi = _text(_first(record, \"WMI\", \"Wmi\", \"WmiCode\"))
    if wmi is None:
        raise DataError(\"WMI row is missing WMI\")
    resolved_manufacturer_id = _integer(
        _first(record, \"ManufacturerId\", \"ManufacturerID\", \"Manufacturer_Id\")
    )
    if resolved_manufacturer_id is None:
        resolved_manufacturer_id = manufacturer_id
    available = _first(
        record,
        \"DateAvailableToPublic\",
        \"PublicAvailabilityDate\",
        \"AvailableDate\",
    )
    parsed_available: datetime | date | None = None
    if isinstance(available, (datetime, date)):
        parsed_available = available
    else:
        parsed_available = parse_microsoft_timestamp(available)
    if isinstance(parsed_available, datetime):
        available_value = parsed_available.date()
    else:
        available_value = parsed_available
    return {
        \"wmi\": wmi.upper(),
        \"manufacturer_id\": resolved_manufacturer_id,
        \"brand_name\": _text(_first(record, \"BrandName\", \"MakeName\", \"Make\")),
        \"vehicle_type_id\": _integer(_first(record, \"VehicleTypeId\", \"VehicleTypeID\")),
        \"vehicle_type_name\": _text(_first(record, \"VehicleTypeName\", \"VehicleType\")),
        \"country\": _text(_first(record, \"Country\", \"CountryName\")),
        \"date_available_to_public\": available_value,
    }


def _normalized_header(value: str) -> str:
    return re.sub(r\"[^a-z0-9]\", \"\", value.lower())


def parse_models_csv(body: str) -> list[dict[str, Any]]:
    if not body.strip():
        raise DataError(\"models CSV response is empty\")
    reader = csv.DictReader(io.StringIO(body.lstrip(\"\\ufeff\")))
    if reader.fieldnames is None:
        raise DataError(\"models CSV has no header\")
    aliases = {
        \"makeid\": (\"makeid\", \"make_id\"),
        \"makename\": (\"make\", \"makename\", \"make_name\"),
        \"modelid\": (\"modelid\", \"model_id\"),
        \"modelname\": (\"model\", \"modelname\", \"model_name\"),
        \"year\": (\"year\", \"modelyear\", \"model_year\"),
    }
    columns: dict[str, str] = {}
    for field in reader.fieldnames:
        normalized = _normalized_header(field)
        for target, names in aliases.items():
            if normalized in names and target not in columns:
                columns[target] = field
    if \"modelname\" not in columns or not ({\"makeid\", \"modelid\"} <= set(columns)):
        raise DataError(\"models CSV must contain make/model identifiers and a model name column\")
    rows: list[dict[str, Any]] = []
    for line_number, record in enumerate(reader, start=2):
        make_id = _integer(record.get(columns[\"makeid\"]))
        model_id = _integer(record.get(columns[\"modelid\"]))
        model_name = _text(record.get(columns[\"modelname\"]))
        if make_id is None or model_id is None or model_name is None:
            raise DataError(f\"models CSV row {line_number} has an incomplete stable key\")
        make_name = _text(record.get(columns.get(\"makename\", \"\")))
        year = _integer(record.get(columns.get(\"year\", \"\")))
        rows.append(
            {
                \"make_id\": make_id,
                \"make_name\": make_name,
                \"model_id\": model_id,
                \"model_name\": model_name,
                \"year\": year,
            }
        )
    if not rows:
        raise DataError(\"models CSV contains no data rows\")
    return rows


def parse_retry_after(value: str | None, now: datetime | None = None) -> float | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    try:
        return max(0.0, float(stripped))
    except ValueError:
        pass
    try:
        parsed = parsedate_to_datetime(stripped)
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    current = now or datetime.now(UTC)
    if current.tzinfo is None:
        current = current.replace(tzinfo=UTC)
    return max(0.0, (parsed - current).total_seconds())


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(\",\", \":\"),
        sort_keys=True,
        default=str,
    )


def _stable_key(dataset: str, row: Mapping[str, Any]) -> str:
    if dataset == \"manufacturers\":
        values: tuple[Any, ...] = (row[\"manufacturer_id\"],)
    elif dataset == \"vehicle_variables\":
        values = (row[\"variable_id\"],)
    elif dataset == \"variable_values\":
        values = (row[\"variable_id\"], row[\"value_id\"])
    elif dataset == \"wmi_codes\":
        values = (row[\"wmi\"],)
    elif dataset == \"models_historical\":
        values = (row[\"make_id\"], row[\"model_id\"], row[\"year\"])
    elif dataset == \"equipment_plants\":
        values = (row[\"equipment_type\"], row[\"plant_year\"], row[\"dot_code\"])
    else:
        raise DataError(f\"unknown dataset: {dataset}\")
    return _canonical_json(values)


def _dedupe_rows(dataset: str, rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    deduplicated: dict[str, dict[str, Any]] = {}
    for source in rows:
        row = dict(source)
        key = _stable_key(dataset, row)
        deduplicated[key] = row
    return list(deduplicated.values())


def _database_row(
    dataset: str,
    row: Mapping[str, Any],
    now: datetime,
) -> tuple[Any, ...]:
    values = dict(row)
    values.setdefault(\"last_updated\", now)
    values.setdefault(\"created_at\", now)
    if dataset == \"manufacturers\":
        values[\"vehicle_types\"] = Json(values[\"vehicle_types\"])
    return tuple(values[column] for column in DATASET_COLUMNS[dataset])


def _failure_from_body(
    url: str,
    attempts: int,
    status: int,
    body: bytes,
    error: str,
) -> FetchFailure:
    text = body.decode(\"utf-8\", errors=\"replace\")
    return FetchFailure(
        url=url,
        attempts=attempts,
        http_status=status,
        response_body=text,
        response_sha256=hashlib.sha256(body).hexdigest(),
        error=error,
    )


class NhtsaClient:
    def __init__(
        self,
        config: ApiConfig,
        *,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
        random_value: Callable[[float, float], float] = random.uniform,
    ) -> None:
        self.config = config
        self.session = session or requests.Session()
        self.session.headers.update(
            {
                \"User-Agent\": config.user_agent,
                \"Accept\": \"application/json, text/csv;q=0.9\",
                \"Accept-Encoding\": \"gzip, deflate\",
            }
        )
        self._sleep = sleep
        self._random_value = random_value
        self._last_request_at: float | None = None

    def close(self) -> None:
        self.session.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def _wait_for_turn(self) -> None:
        if self._last_request_at is None:
            return
        remaining = self.config.request_interval_seconds - (
            time.monotonic() - self._last_request_at
        )
        if remaining > 0:
            self._sleep(remaining)

    def _backoff(self, attempt: int, retry_after: str | None = None) -> float:
        header_delay = parse_retry_after(retry_after)
        if header_delay is not None:
            return min(header_delay, self.config.max_retry_after_seconds)
        exponential = min(
            self.config.max_backoff_seconds,
            self.config.backoff_seconds * (2 ** (attempt - 1)),
        )
        jitter = min(exponential * 0.25, 1.0)
        return exponential + self._random_value(0.0, jitter)

    def request(
        self,
        url: str,
        *,
        response_format: str,
        params: Mapping[str, Any] | None = None,
        before_attempt: Callable[[int], None] | None = None,
    ) -> FetchResult:
        request_params = dict(params or {})
        request_params[\"format\"] = response_format
        last_error = \"request failed\"
        last_status: int | None = None
        last_body = b\"\"
        for attempt in range(1, self.config.max_attempts + 1):
            if before_attempt is not None:
                before_attempt(attempt)
            self._wait_for_turn()
            started = time.monotonic()
            try:
                response = self.session.get(
                    url,
                    params=request_params,
                    timeout=self.config.timeout_seconds,
                    allow_redirects=True,
                )
            except requests.RequestException as exc:
                last_error = f\"network error: {exc}\"
                if attempt == self.config.max_attempts:
                    failure = _failure_from_body(url, attempt, 0, b\"\", last_error)
                    raise ApiUnavailableError(failure) from exc
                self._sleep(self._backoff(attempt))
                continue
            finally:
                self._last_request_at = time.monotonic()
            elapsed = time.monotonic() - started
            last_status = response.status_code
            last_body = response.content
            if response.status_code == 403:
                failure = _failure_from_body(
                    response.url,
                    attempt,
                    response.status_code,
                    last_body,
                    \"NHTSA circuit breaker opened on HTTP 403; stopping immediately\",
                )
                raise CircuitBreakerOpen(failure)
            retryable = response.status_code == 429 or 500 <= response.status_code <= 599
            if retryable:
                last_error = f\"HTTP {response.status_code} from NHTSA\"
                if attempt == self.config.max_attempts:
                    failure = _failure_from_body(
                        response.url,
                        attempt,
                        response.status_code,
                        last_body,
                        last_error,
                    )
                    raise ApiUnavailableError(failure)
                self._sleep(self._backoff(attempt, response.headers.get(\"Retry-After\")))
                continue
            if not 200 <= response.status_code <= 299:
                failure = _failure_from_body(
                    response.url,
                    attempt,
                    response.status_code,
                    last_body,
                    f\"HTTP {response.status_code} from NHTSA\",
                )
                raise ApiUnavailableError(failure)
            try:
                body = last_body.decode(\"utf-8-sig\")
            except UnicodeDecodeError as exc:
                failure = _failure_from_body(
                    response.url,
                    attempt,
                    response.status_code,
                    last_body,
                    \"NHTSA response was not valid UTF-8\",
                )
                raise ApiUnavailableError(failure) from exc
            if response_format == \"json\":
                try:
                    payload = json.loads(body)
                except json.JSONDecodeError as exc:
                    failure = _failure_from_body(
                        response.url,
                        attempt,
                        response.status_code,
                        last_body,
                        f\"NHTSA returned invalid JSON: {exc}\",
                    )
                    raise ApiUnavailableError(failure) from exc
            else:
                payload = None
            return FetchResult(
                url=response.url,
                attempts=attempt,
                http_status=response.status_code,
                response_body=body,
                response_sha256=hashlib.sha256(last_body).hexdigest(),
                payload=payload,
                latency_seconds=elapsed,
            )
        failure = FetchFailure(
            url=url,
            attempts=self.config.max_attempts,
            http_status=last_status,
            response_body=last_body.decode(\"utf-8\", errors=\"replace\"),
            response_sha256=hashlib.sha256(last_body).hexdigest(),
            error=last_error,
        )
        raise ApiUnavailableError(failure)

    def probe(self, url: str) -> ProbeResult:
        self._wait_for_turn()
        started = time.monotonic()
        try:
            response = self.session.get(
                url,
                params={\"format\": \"json\"},
                timeout=self.config.timeout_seconds,
                allow_redirects=True,
            )
        except requests.RequestException as exc:
            return ProbeResult(
                url=url,
                http_status=None,
                latency_seconds=time.monotonic() - started,
                response_bytes=0,
                count=None,
                message=None,
                error=str(exc),
            )
        finally:
            self._last_request_at = time.monotonic()
        elapsed = time.monotonic() - started
        count: int | None = None
        message: str | None = None
        error: str | None = None
        try:
            payload = response.json()
            if isinstance(payload, dict):
                raw_count = payload.get(\"Count\")
                count = raw_count if isinstance(raw_count, int) else None
                message = _text(payload.get(\"Message\"))
                errors = payload.get(\"Errors\")
                if errors:
                    error = _canonical_json(errors)
        except ValueError:
            error = \"response was not JSON\"
        if not 200 <= response.status_code <= 299:
            error = error or f\"HTTP {response.status_code}\"
        return ProbeResult(
            url=response.url,
            http_status=response.status_code,
            latency_seconds=elapsed,
            response_bytes=len(response.content),
            count=count,
            message=message,
            error=error,
        )


class NhtsaImporter:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.connection = psycopg2.connect(**config.database.connect_kwargs())
        self.connection.autocommit = False
        self.client = NhtsaClient(config.api)
        self.run_id: str | None = None
        self.reconcile = False
        self.datasets: tuple[str, ...] = ()

    def close(self) -> None:
        self.client.close()
        self.connection.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    @staticmethod
    def _api_url(
        base_url: str,
        path: str,
        params: Mapping[str, Any] | None = None,
        response_format: str = \"json\",
    ) -> str:
        query = dict(params or {})
        query[\"format\"] = response_format
        return f\"{base_url.rstrip('/')}/vehicles/{path.lstrip('/')}?{urlencode(query)}\"

    @staticmethod
    def _csv_url(url: str) -> str:
        parsed = urlsplit(url)
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        query[\"format\"] = \"csv\"
        return urlunsplit(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                urlencode(query),
                parsed.fragment,
            )
        )

    def ensure_metadata(self) -> None:
        with self.connection:
            with self.connection.cursor() as cursor:
                for statement in METADATA_SQL:
                    cursor.execute(statement)

    def ensure_reference_schema(self) -> None:
        with self.connection:
            with self.connection.cursor() as cursor:
                statements = (
                    \"ALTER TABLE api_reference.manufacturers ALTER COLUMN manufacturer_name TYPE text, ALTER COLUMN manufacturer_common_name TYPE text, ALTER COLUMN primary_product TYPE text\",
                    \"ALTER TABLE api_reference.vehicle_variables ALTER COLUMN variable_name TYPE text, ALTER COLUMN variable_group_name TYPE text, ALTER COLUMN data_type TYPE text\",
                    \"ALTER TABLE api_reference.variable_values ALTER COLUMN value TYPE text\",
                    \"ALTER TABLE api_reference.wmi_codes ALTER COLUMN wmi TYPE text, ALTER COLUMN brand_name TYPE text, ALTER COLUMN vehicle_type_name TYPE text, ALTER COLUMN country TYPE text\",
                    \"ALTER TABLE api_reference.models_historical ALTER COLUMN make_name TYPE text, ALTER COLUMN model_name TYPE text\",
                )
                for statement in statements:
                    cursor.execute(statement)

    def ensure_equipment_schema(self) -> None:
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(\"SELECT to_regclass('api_reference.equipment_plants')\")
                exists = cursor.fetchone()[0] is not None
                if exists:
                    cursor.execute(
                        \"LOCK TABLE api_reference.equipment_plants IN ACCESS EXCLUSIVE MODE\"
                    )
                    cursor.execute(\"SELECT EXISTS (SELECT 1 FROM api_reference.equipment_plants)\")
                    if cursor.fetchone()[0]:
                        cursor.execute(
                            \"\"\"
                            SELECT column_name
                            FROM information_schema.columns
                            WHERE table_schema = 'api_reference'
                              AND table_name = 'equipment_plants'
                            \"\"\"
                        )
                        actual = {row[0] for row in cursor.fetchall()}
                        missing = sorted(EQUIPMENT_REQUIRED_COLUMNS - actual)
                        if missing:
                            raise DataError(
                                \"api_reference.equipment_plants is non-empty and lacks required columns: \"
                                + \", \".join(missing)
                            )
                        cursor.execute(
                            \"\"\"
                            SELECT pg_get_constraintdef(oid)
                            FROM pg_constraint
                            WHERE conrelid = 'api_reference.equipment_plants'::regclass
                              AND contype IN ('p', 'u')
                            \"\"\"
                        )
                        key_definitions = [row[0] for row in cursor.fetchall()]
                        required_key = {\"equipment_type\", \"plant_year\", \"dot_code\"}
                        if not any(
                            all(column in definition for column in required_key)
                            for definition in key_definitions
                        ):
                            raise DataError(
                                \"api_reference.equipment_plants lacks the required \"
                                \"(equipment_type, plant_year, dot_code) unique key\"
                            )
                        return
                    cursor.execute(\"DROP TABLE api_reference.equipment_plants\")
                cursor.execute(EQUIPMENT_CREATE_SQL)

    def ensure_model_stable_key_index(self) -> None:
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    \"\"\"
                    CREATE UNIQUE INDEX IF NOT EXISTS models_historical_stable_key_idx
                    ON api_reference.models_historical (
                        make_id,
                        model_id,
                        (COALESCE(year, -1))
                    )
                    \"\"\"
                )

    def _create_or_resume_run(
        self,
        run_id: str,
        datasets: Sequence[str],
        reconcile: bool,
    ) -> None:
        if not run_id or len(run_id) > 128:
            raise ImporterError(\"run ID must contain between 1 and 128 characters\")
        ordered = tuple(dataset for dataset in DATASETS if dataset in set(datasets))
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    \"SELECT datasets, reconcile_enabled FROM nhtsa_import.sync_runs WHERE run_id = %s\",
                    (run_id,),
                )
                existing = cursor.fetchone()
                if existing is None:
                    cursor.execute(
                        \"\"\"
                        INSERT INTO nhtsa_import.sync_runs (
                            run_id,
                            datasets,
                            status,
                            reconcile_enabled,
                            requested_rate
                        ) VALUES (%s, %s, 'running', %s, %s)
                        \"\"\",
                        (
                            run_id,
                            list(ordered),
                            reconcile,
                            self.config.api.request_interval_seconds,
                        ),
                    )
                else:
                    previous_datasets = tuple(existing[0])
                    if bool(existing[1]) != reconcile:
                        raise ImporterError(
                            \"resume reconciliation setting differs from the persisted run setting\"
                        )
                    merged = tuple(
                        dataset
                        for dataset in DATASETS
                        if dataset in set(previous_datasets) | set(ordered)
                    )
                    cursor.execute(
                        \"\"\"
                        UPDATE nhtsa_import.sync_runs
                        SET datasets = %s,
                            status = 'running',
                            error = NULL,
                            finished_at = NULL,
                            updated_at = clock_timestamp()
                        WHERE run_id = %s
                        \"\"\",
                        (list(merged), run_id),
                    )
                    ordered = merged
        self.run_id = run_id
        self.reconcile = reconcile
        self.datasets = ordered

    def _job_state(self, dataset: str, job_key: str) -> Mapping[str, Any] | None:
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    \"\"\"
                    SELECT status, row_count, response_sha256
                    FROM nhtsa_import.sync_jobs
                    WHERE run_id = %s AND dataset = %s AND job_key = %s
                    \"\"\",
                    (self.run_id, dataset, job_key),
                )
                row = cursor.fetchone()
                if row is None:
                    return None
                return {
                    \"status\": row[0],
                    \"row_count\": row[1],
                    \"response_sha256\": row[2],
                }

    def _completed_jobs(self, dataset: str) -> set[str]:
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    \"\"\"
                    SELECT job_key
                    FROM nhtsa_import.sync_jobs
                    WHERE run_id = %s AND dataset = %s AND status = 'complete'
                    \"\"\",
                    (self.run_id, dataset),
                )
                return {row[0] for row in cursor.fetchall()}

    def _begin_attempt(
        self,
        dataset: str,
        job_key: str,
        url: str,
        options: Mapping[str, Any],
        attempt: int,
    ) -> None:
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    \"\"\"
                    INSERT INTO nhtsa_import.sync_jobs (
                        run_id,
                        job_key,
                        dataset,
                        request_url,
                        request_options,
                        status,
                        attempts,
                        started_at,
                        updated_at
                    ) VALUES (%s, %s, %s, %s, %s, 'running', %s, clock_timestamp(), clock_timestamp())
                    ON CONFLICT (run_id, job_key) DO UPDATE SET
                        request_url = EXCLUDED.request_url,
                        request_options = EXCLUDED.request_options,
                        status = 'running',
                        attempts = nhtsa_import.sync_jobs.attempts + EXCLUDED.attempts,
                        http_status = NULL,
                        row_count = NULL,
                        response_sha256 = NULL,
                        response_body = NULL,
                        response_json = NULL,
                        error = NULL,
                        started_at = COALESCE(nhtsa_import.sync_jobs.started_at, clock_timestamp()),
                        completed_at = NULL,
                        updated_at = clock_timestamp()
                    \"\"\",
                    (
                        self.run_id,
                        job_key,
                        dataset,
                        url,
                        Json(dict(options)),
                        attempt,
                    ),
                )
                self._refresh_run_counts(cursor)

    def _refresh_run_counts(self, cursor: Any) -> None:
        cursor.execute(
            \"\"\"
            UPDATE nhtsa_import.sync_runs AS runs
            SET total_jobs = counts.total,
                completed_jobs = counts.completed,
                row_count = counts.rows,
                updated_at = clock_timestamp()
            FROM (
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE status = 'complete') AS completed,
                    COALESCE(SUM(row_count) FILTER (WHERE status = 'complete'), 0) AS rows
                FROM nhtsa_import.sync_jobs
                WHERE run_id = %s
            ) AS counts
            WHERE runs.run_id = %s
            \"\"\",
            (self.run_id, self.run_id),
        )

    def _store_failure(
        self,
        dataset: str,
        job_key: str,
        options: Mapping[str, Any],
        failure: FetchFailure,
        *,
        status: str = \"failed\",
    ) -> None:
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    \"\"\"
                    INSERT INTO nhtsa_import.sync_jobs (
                        run_id,
                        job_key,
                        dataset,
                        request_url,
                        request_options,
                        status,
                        attempts,
                        http_status,
                        row_count,
                        response_sha256,
                        response_body,
                        response_json,
                        error,
                        started_at,
                        completed_at,
                        updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, 0, %s, %s, %s, %s,
                        clock_timestamp(), clock_timestamp(), clock_timestamp()
                    )
                    ON CONFLICT (run_id, job_key) DO UPDATE SET
                        request_url = EXCLUDED.request_url,
                        request_options = EXCLUDED.request_options,
                        status = EXCLUDED.status,
                        attempts = GREATEST(nhtsa_import.sync_jobs.attempts, EXCLUDED.attempts),
                        http_status = EXCLUDED.http_status,
                        row_count = 0,
                        response_sha256 = EXCLUDED.response_sha256,
                        response_body = EXCLUDED.response_body,
                        response_json = EXCLUDED.response_json,
                        error = EXCLUDED.error,
                        completed_at = EXCLUDED.completed_at,
                        updated_at = clock_timestamp()
                    \"\"\",
                    (
                        self.run_id,
                        job_key,
                        dataset,
                        failure.url,
                        Json(dict(options)),
                        status,
                        failure.attempts,
                        failure.http_status,
                        failure.response_sha256,
                        failure.response_body,
                        Json(None),
                        failure.error,
                    ),
                )
                self._refresh_run_counts(cursor)

    def _update_progress(
        self,
        cursor: Any,
        dataset: str,
        status: str,
        error: str | None = None,
    ) -> None:
        cursor.execute(
            \"\"\"
            SELECT
                COUNT(*) FILTER (WHERE job_status = 'complete'),
                COALESCE(SUM(row_count) FILTER (WHERE job_status = 'complete'), 0)
            FROM (
                SELECT status AS job_status, row_count
                FROM nhtsa_import.sync_jobs
                WHERE run_id = %s AND dataset = %s
            ) AS jobs
            \"\"\",
            (self.run_id, dataset),
        )
        completed, total_records = cursor.fetchone()
        cursor.execute(
            \"\"\"
            INSERT INTO api_reference.import_progress (
                importer_name,
                last_offset,
                total_records,
                status,
                error_message,
                created_at,
                last_updated
            ) VALUES (%s, %s, %s, %s, %s, clock_timestamp(), clock_timestamp())
            ON CONFLICT (importer_name) DO UPDATE SET
                last_offset = EXCLUDED.last_offset,
                total_records = EXCLUDED.total_records,
                status = EXCLUDED.status,
                error_message = EXCLUDED.error_message,
                last_updated = EXCLUDED.last_updated
            \"\"\",
            (PROGRESS_PREFIX + dataset, completed, total_records, status, error),
        )

    def _upsert_rows(self, cursor: Any, dataset: str, rows: Sequence[Mapping[str, Any]]) -> None:
        if not rows:
            return
        now = datetime.now(UTC)
        columns = DATASET_COLUMNS[dataset]
        template = \"(\" + \", \".join([\"%s\"] * len(columns)) + \")\"
        for start in range(0, len(rows), 1000):
            chunk = rows[start : start + 1000]
            execute_values(
                cursor,
                DATASET_SQL[dataset],
                [_database_row(dataset, row, now) for row in chunk],
                template=template,
                page_size=1000,
            )

    def _store_seen_keys(
        self,
        cursor: Any,
        dataset: str,
        rows: Sequence[Mapping[str, Any]],
    ) -> None:
        if not rows:
            return
        keys = [(self.run_id, dataset, Json(json.loads(_stable_key(dataset, row)))) for row in rows]
        execute_values(
            cursor,
            \"\"\"
            INSERT INTO nhtsa_import.reconciliation_keys (run_id, dataset, stable_key)
            VALUES %s
            ON CONFLICT DO NOTHING
            \"\"\",
            keys,
            template=\"(%s, %s, %s)\",
            page_size=1000,
        )

    def _complete_job(
        self,
        dataset: str,
        job_key: str,
        url: str,
        options: Mapping[str, Any],
        result: FetchResult,
        rows: Sequence[Mapping[str, Any]],
    ) -> None:
        prepared = _dedupe_rows(dataset, rows)
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    \"\"\"
                    INSERT INTO nhtsa_import.sync_jobs (
                        run_id,
                        job_key,
                        dataset,
                        request_url,
                        request_options,
                        status,
                        attempts,
                        http_status,
                        row_count,
                        response_sha256,
                        response_body,
                        response_json,
                        started_at,
                        completed_at,
                        updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, 'complete', %s, %s, %s, %s, %s, %s,
                        COALESCE(
                            (
                                SELECT started_at
                                FROM nhtsa_import.sync_jobs
                                WHERE run_id = %s AND job_key = %s
                            ),
                            clock_timestamp()
                        ),
                        clock_timestamp(),
                        clock_timestamp()
                    )
                    ON CONFLICT (run_id, job_key) DO UPDATE SET
                        request_url = EXCLUDED.request_url,
                        request_options = EXCLUDED.request_options,
                        status = 'complete',
                        attempts = GREATEST(nhtsa_import.sync_jobs.attempts, EXCLUDED.attempts),
                        http_status = EXCLUDED.http_status,
                        row_count = EXCLUDED.row_count,
                        response_sha256 = EXCLUDED.response_sha256,
                        response_body = EXCLUDED.response_body,
                        response_json = EXCLUDED.response_json,
                        error = NULL,
                        completed_at = clock_timestamp(),
                        updated_at = clock_timestamp()
                    \"\"\",
                    (
                        self.run_id,
                        job_key,
                        dataset,
                        url,
                        Json(dict(options)),
                        result.attempts,
                        result.http_status,
                        len(prepared),
                        result.response_sha256,
                        result.response_body,
                        Json(result.payload) if result.payload is not None else None,
                        self.run_id,
                        job_key,
                    ),
                )
                self._upsert_rows(cursor, dataset, prepared)
                self._store_seen_keys(cursor, dataset, prepared)
                self._update_progress(cursor, dataset, \"running\")
                self._refresh_run_counts(cursor)

    def _run_fetch_job(
        self,
        dataset: str,
        job_key: str,
        url: str,
        options: Mapping[str, Any],
        normalizer: Callable[[Any], list[dict[str, Any]]],
        *,
        response_format: str = \"json\",
    ) -> JobOutcome:
        state = self._job_state(dataset, job_key)
        if state is not None and state[\"status\"] == \"complete\":
            return JobOutcome(
                row_count=int(state[\"row_count\"] or 0),
                response_sha256=state[\"response_sha256\"],
            )
        result: FetchResult | None = None
        try:
            result = self.client.request(
                url,
                response_format=response_format,
                params=None,
                before_attempt=lambda attempt: self._begin_attempt(
                    dataset, job_key, url, options, attempt
                ),
            )
            rows = normalizer(result.payload if response_format == \"json\" else result.response_body)
        except CircuitBreakerOpen as exc:
            self._store_failure(dataset, job_key, options, exc.failure)
            raise
        except ApiUnavailableError as exc:
            self._store_failure(dataset, job_key, options, exc.failure)
            raise
        except DataError as exc:
            if result is None:
                failure = FetchFailure(
                    url=url,
                    attempts=1,
                    http_status=0,
                    response_body=\"\",
                    response_sha256=hashlib.sha256(b\"\").hexdigest(),
                    error=str(exc),
                )
            else:
                failure = FetchFailure(
                    url=result.url,
                    attempts=result.attempts,
                    http_status=result.http_status,
                    response_body=result.response_body,
                    response_sha256=result.response_sha256,
                    error=str(exc),
                )
            self._store_failure(dataset, job_key, options, failure)
            wrapped = ApiUnavailableError(failure)
            raise wrapped from exc
        if result is None:
            raise DataError(f\"{dataset}/{job_key}: request produced no result\")
        prepared_count = len(_dedupe_rows(dataset, rows))
        self._complete_job(dataset, job_key, url, options, result, rows)
        return JobOutcome(row_count=prepared_count, response_sha256=result.response_sha256)

    def _mark_progress(self, dataset: str, status: str, error: str | None = None) -> None:
        with self.connection:
            with self.connection.cursor() as cursor:
                self._update_progress(cursor, dataset, status, error)

    def _finish_dataset(self, dataset: str) -> None:
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    \"\"\"
                    SELECT job_key, status
                    FROM nhtsa_import.sync_jobs
                    WHERE run_id = %s AND dataset = %s AND status NOT IN ('complete', 'fallback')
                    \"\"\",
                    (self.run_id, dataset),
                )
                unfinished = cursor.fetchall()
                if unfinished:
                    keys = \", \".join(row[0] for row in unfinished[:5])
                    raise DataError(f\"{dataset} has unfinished jobs: {keys}\")
                if self.reconcile:
                    table = DATASET_TABLES[dataset]
                    stable_key_sql = {
                        \"manufacturers\": \"jsonb_build_array(target.manufacturer_id)\",
                        \"vehicle_variables\": \"jsonb_build_array(target.variable_id)\",
                        \"variable_values\": \"jsonb_build_array(target.variable_id, target.value_id)\",
                        \"wmi_codes\": \"jsonb_build_array(target.wmi)\",
                        \"models_historical\": \"jsonb_build_array(target.make_id, target.model_id, target.year)\",
                        \"equipment_plants\": \"jsonb_build_array(target.equipment_type, target.plant_year, target.dot_code)\",
                    }[dataset]
                    cursor.execute(
                        f\"\"\"
                        DELETE FROM api_reference.{table} AS target
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM nhtsa_import.reconciliation_keys AS seen
                            WHERE seen.run_id = %s
                              AND seen.dataset = %s
                              AND seen.stable_key = {stable_key_sql}
                        )
                        \"\"\",
                        (self.run_id, dataset),
                    )
                self._update_progress(cursor, dataset, \"complete\")

    def _finish_run(self, status: str, error: str | None = None) -> None:
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    \"\"\"
                    UPDATE nhtsa_import.sync_runs
                    SET status = %s,
                        error = %s,
                        finished_at = CASE WHEN %s IN ('complete', 'failed', 'interrupted')
                            THEN clock_timestamp() ELSE NULL END,
                        updated_at = clock_timestamp()
                    WHERE run_id = %s
                    \"\"\",
                    (status, error, status, self.run_id),
                )

    def _lookup_variable_ids(self) -> list[int]:
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    \"\"\"
                    SELECT variable_id
                    FROM api_reference.vehicle_variables
                    WHERE lower(data_type) = 'lookup'
                    ORDER BY variable_id
                    \"\"\"
                )
                variable_ids = [int(row[0]) for row in cursor.fetchall()]
                if variable_ids:
                    return variable_ids
                cursor.execute(
                    \"\"\"
                    SELECT response_json -> 'Results'
                    FROM nhtsa_import.sync_jobs
                    WHERE run_id = %s
                      AND dataset = 'vehicle_variables'
                      AND status = 'complete'
                      AND response_json IS NOT NULL
                    ORDER BY completed_at DESC
                    LIMIT 1
                    \"\"\",
                    (self.run_id,),
                )
                row = cursor.fetchone()
        if row is not None and isinstance(row[0], list):
            variable_ids = [
                variable_id
                for record in row[0]
                if isinstance(record, dict) and str(record.get(\"DataType\", \"\")).lower() == \"lookup\"
                for variable_id in [_integer(_first(record, \"Id\", \"ID\", \"VariableId\"))]
                if variable_id is not None
            ]
        if not variable_ids:
            raise DataError(\"no lookup vehicle variables are available\")
        return sorted(set(variable_ids))

    def _candidate_manufacturer_ids(self) -> list[int]:
        try:
            with self.connection:
                with self.connection.cursor() as cursor:
                    cursor.execute(
                        \"\"\"
                        SELECT DISTINCT manufacturerid
                        FROM vpic.wmi
                        WHERE manufacturerid IS NOT NULL
                        ORDER BY manufacturerid
                        \"\"\"
                    )
                    values = [int(row[0]) for row in cursor.fetchall()]
        except psycopg2.Error as exc:
            raise DataError(f\"cannot read current decoder table vpic.wmi: {exc}\") from exc
        if not values:
            raise DataError(\"current decoder table vpic.wmi has no manufacturer IDs\")
        return values

    def _decoder_wmi_rows(self, manufacturer_ids: Sequence[int]) -> list[dict[str, Any]]:
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    \"\"\"
                    SELECT
                        w.wmi,
                        w.manufacturerid,
                        make.name,
                        vehicle_type.name,
                        country.name,
                        w.publicavailabilitydate
                    FROM vpic.wmi AS w
                    LEFT JOIN vpic.wmi_make AS wm ON wm.wmiid = w.id
                    LEFT JOIN vpic.make AS make ON make.id = wm.makeid
                    LEFT JOIN vpic.vehiculetype AS vehicle_type
                        ON vehicle_type.id = w.vehicletypeid
                    LEFT JOIN vpic.country AS country ON country.id = w.countryid
                    WHERE w.manufacturerid = ANY(%s)
                    ORDER BY w.wmi, make.name
                    \"\"\",
                    (list(manufacturer_ids),),
                )
                source = cursor.fetchall()
        rows: list[dict[str, Any]] = []
        for wmi, manufacturer_id, make_name, vehicle_type, country, available in source:
            rows.append(
                _normalize_wmi(
                    {
                        \"WMI\": wmi,
                        \"ManufacturerId\": manufacturer_id,
                        \"MakeName\": make_name,
                        \"VehicleTypeName\": vehicle_type,
                        \"Country\": country,
                        \"PublicAvailabilityDate\": available,
                    }
                )
            )
        return _dedupe_rows(\"wmi_codes\", rows)

    def _store_decoder_fallback(
        self,
        manufacturer_ids: Sequence[int],
        api_error: str,
    ) -> JobOutcome:
        dataset = \"wmi_codes\"
        job_key = \"decoder-fallback\"
        state = self._job_state(dataset, job_key)
        if state is not None and state[\"status\"] == \"complete\":
            return JobOutcome(
                row_count=int(state[\"row_count\"] or 0),
                response_sha256=state[\"response_sha256\"],
            )
        rows = self._decoder_wmi_rows(manufacturer_ids)
        body = _canonical_json(
            {
                \"source\": \"vpic.wmi\",
                \"manufacturer_ids\": list(manufacturer_ids),
                \"results\": rows,
            }
        ).encode(\"utf-8\")
        result = FetchResult(
            url=\"postgresql://vpic.wmi\",
            attempts=0,
            http_status=0,
            response_body=body.decode(\"utf-8\"),
            response_sha256=hashlib.sha256(body).hexdigest(),
            payload=json.loads(body),
            latency_seconds=0.0,
        )
        options = {\"fallback\": True, \"reason\": api_error}
        self._complete_job(dataset, job_key, result.url, options, result, rows)
        return JobOutcome(row_count=len(rows), response_sha256=result.response_sha256)

    def _jobs_manufacturers(self) -> Iterable[JobOutcome]:
        completed = self._completed_jobs(\"manufacturers\")
        page = 1
        seen_hashes: set[str] = set()
        while True:
            job_key = f\"page-{page:06d}\"
            state = self._job_state(\"manufacturers\", job_key)
            if job_key in completed and state is not None:
                row_count = int(state[\"row_count\"] or 0)
                if row_count == 0:
                    break
                page += 1
                continue
            url = self._api_url(
                self.config.api.base_url,
                \"GetAllManufacturers\",
                {\"page\": page},
            )
            outcome = self._run_fetch_job(
                \"manufacturers\",
                job_key,
                url,
                {\"page\": page},
                lambda payload, page=page: [
                    _normalize_manufacturer(record)
                    for record in extract_results(payload, f\"GetAllManufacturers page {page}\")
                ],
            )
            yield outcome
            if outcome.response_sha256 is not None:
                if outcome.response_sha256 in seen_hashes:
                    raise DataError(
                        \"GetAllManufacturers repeated a page response; pagination is not advancing\"
                    )
                seen_hashes.add(outcome.response_sha256)
            if outcome.row_count == 0:
                break
            page += 1

    def _jobs_vehicle_variables(self) -> Iterable[JobOutcome]:
        job_key = \"all\"
        if self._job_state(\"vehicle_variables\", job_key) is not None:
            state = self._job_state(\"vehicle_variables\", job_key)
            if state is not None and state[\"status\"] == \"complete\":
                return
        url = self._api_url(self.config.api.base_url, \"GetVehicleVariableList\")
        yield self._run_fetch_job(
            \"vehicle_variables\",
            job_key,
            url,
            {},
            lambda payload: [
                _normalize_vehicle_variable(record)
                for record in extract_results(payload, \"GetVehicleVariableList\")
            ],
        )

    def _jobs_variable_values(self) -> Iterable[JobOutcome]:
        completed = self._completed_jobs(\"variable_values\")
        for variable_id in self._lookup_variable_ids():
            job_key = f\"variable-{variable_id:06d}\"
            if job_key in completed:
                continue
            url = self._api_url(
                self.config.api.base_url,
                f\"GetVehicleVariableValuesList/{quote(str(variable_id), safe='')}\",
            )
            yield self._run_fetch_job(
                \"variable_values\",
                job_key,
                url,
                {\"variable_id\": variable_id},
                lambda payload, variable_id=variable_id: [
                    _normalize_variable_value(record, variable_id)
                    for record in extract_results(
                        payload,
                        f\"GetVehicleVariableValuesList/{variable_id}\",
                    )
                ],
            )

    def _jobs_wmi_codes(self) -> Iterable[JobOutcome]:
        fallback_key = \"decoder-fallback\"
        fallback_state = self._job_state(\"wmi_codes\", fallback_key)
        if fallback_state is not None and fallback_state[\"status\"] == \"complete\":
            return
        manufacturer_ids = self._candidate_manufacturer_ids()
        completed = self._completed_jobs(\"wmi_codes\")
        for manufacturer_id in manufacturer_ids:
            job_key = f\"manufacturer-{manufacturer_id:06d}\"
            if job_key in completed:
                continue
            url = self._api_url(
                self.config.api.base_url,
                f\"GetWMIsForManufacturer/{quote(str(manufacturer_id), safe='')}\",
            )
            try:
                yield self._run_fetch_job(
                    \"wmi_codes\",
                    job_key,
                    url,
                    {\"manufacturer_id\": manufacturer_id},
                    lambda payload, manufacturer_id=manufacturer_id: [
                        _normalize_wmi(record, manufacturer_id)
                        for record in extract_results(
                            payload,
                            f\"GetWMIsForManufacturer/{manufacturer_id}\",
                        )
                    ],
                )
            except CircuitBreakerOpen:
                raise
            except ApiUnavailableError as exc:
                failure = exc.failure
                self._store_failure(
                    \"wmi_codes\",
                    job_key,
                    {\"manufacturer_id\": manufacturer_id},
                    failure,
                    status=\"fallback\",
                )
                yield self._store_decoder_fallback(manufacturer_ids, failure.error)
                return

    def _jobs_models_historical(self) -> Iterable[JobOutcome]:
        job_key = \"all-models\"
        state = self._job_state(\"models_historical\", job_key)
        if state is not None and state[\"status\"] == \"complete\":
            return
        self.ensure_model_stable_key_index()
        url = self._csv_url(self.config.api.all_models_csv_url)
        yield self._run_fetch_job(
            \"models_historical\",
            job_key,
            url,
            {\"serialized\": True},
            parse_models_csv,
            response_format=\"csv\",
        )

    def _jobs_equipment_plants(self) -> Iterable[JobOutcome]:
        self.ensure_equipment_schema()
        completed = self._completed_jobs(\"equipment_plants\")
        for equipment_type in self.config.api.equipment_types:
            job_key = f\"equipment-{equipment_type:02d}\"
            if job_key in completed:
                continue
            url = self._api_url(
                self.config.api.base_url,
                \"GetEquipmentPlantCodes\",
                {
                    \"year\": self.config.api.equipment_year,
                    \"equipmentType\": equipment_type,
                    \"reportType\": \"All\",
                },
            )
            yield self._run_fetch_job(
                \"equipment_plants\",
                job_key,
                url,
                {
                    \"year\": self.config.api.equipment_year,
                    \"equipmentType\": equipment_type,
                    \"reportType\": \"All\",
                },
                lambda payload, equipment_type=equipment_type: [
                    normalize_equipment_plant(
                        record,
                        equipment_type,
                        self.config.api.equipment_year,
                    )
                    for record in extract_results(
                        payload,
                        f\"GetEquipmentPlantCodes type {equipment_type}\",
                    )
                    if _text(_first(record, \"DOTCode\", \"DotCode\", \"PlantCode\")) is not None
                ],
            )

    def run(
        self,
        *,
        run_id: str | None = None,
        datasets: Sequence[str] | None = None,
        reconcile: bool = True,
    ) -> str:
        selected = tuple(dataset for dataset in DATASETS if dataset in set(datasets or DATASETS))
        if not selected:
            raise ImporterError(\"at least one dataset is required\")
        actual_run_id = run_id or str(uuid.uuid4())
        self.ensure_metadata()
        self.ensure_reference_schema()
        self._create_or_resume_run(actual_run_id, selected, reconcile)
        selected = self.datasets
        started: list[str] = []
        try:
            for dataset in selected:
                started.append(dataset)
                self._mark_progress(dataset, \"running\")
                generators = {
                    \"manufacturers\": self._jobs_manufacturers,
                    \"vehicle_variables\": self._jobs_vehicle_variables,
                    \"variable_values\": self._jobs_variable_values,
                    \"wmi_codes\": self._jobs_wmi_codes,
                    \"models_historical\": self._jobs_models_historical,
                    \"equipment_plants\": self._jobs_equipment_plants,
                }
                total = 0
                for outcome in generators[dataset]():
                    total += outcome.row_count
                    print(
                        f\"{actual_run_id} {dataset}: +{outcome.row_count} rows \"
                        f\"({total} this invocation)\",
                        flush=True,
                    )
                self._finish_dataset(dataset)
            self._finish_run(\"complete\")
        except KeyboardInterrupt as exc:
            if self.run_id is not None:
                for dataset in started[-1:]:
                    with suppress(psycopg2.Error):
                        self._mark_progress(dataset, \"interrupted\", str(exc) or \"interrupted\")
                self._finish_run(\"interrupted\", \"interrupted\")
            raise
        except Exception as exc:
            if self.run_id is not None:
                for dataset in started[-1:]:
                    with suppress(psycopg2.Error):
                        self._mark_progress(dataset, \"failed\", str(exc))
                self._finish_run(\"failed\", str(exc))
            raise
        return actual_run_id

    def status(
        self,
        *,
        run_id: str | None = None,
        dataset: str | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        with self.connection:
            with self.connection.cursor() as cursor:
                cursor.execute(\"SELECT to_regclass('nhtsa_import.sync_runs')\")
                if cursor.fetchone()[0] is None:
                    return {\"runs\": [], \"jobs\": [], \"progress\": []}
                run_sql = \"SELECT run_id, datasets, status, total_jobs, completed_jobs, row_count, error, started_at, finished_at FROM nhtsa_import.sync_runs\"
                run_params: list[Any] = []
                if run_id:
                    run_sql += \" WHERE run_id = %s\"
                    run_params.append(run_id)
                run_sql += \" ORDER BY started_at DESC\"
                cursor.execute(run_sql, run_params)
                columns = [description.name for description in cursor.description]
                runs = [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
                job_sql = \"\"\"
                    SELECT run_id,
                           dataset,
                           status,
                           COUNT(*) AS job_count,
                           COALESCE(SUM(attempts), 0) AS attempts,
                           COALESCE(SUM(row_count), 0) AS row_count,
                           MAX(updated_at) AS updated_at
                    FROM nhtsa_import.sync_jobs
                \"\"\"
                job_params: list[Any] = []
                clauses: list[str] = []
                if run_id:
                    clauses.append(\"run_id = %s\")
                    job_params.append(run_id)
                if dataset:
                    clauses.append(\"dataset = %s\")
                    job_params.append(dataset)
                if clauses:
                    job_sql += \" WHERE \" + \" AND \".join(clauses)
                job_sql += \" GROUP BY run_id, dataset, status ORDER BY run_id, dataset, status\"
                cursor.execute(job_sql, job_params)
                job_columns = [description.name for description in cursor.description]
                jobs = [dict(zip(job_columns, row, strict=True)) for row in cursor.fetchall()]
                progress_sql = \"\"\"
                    SELECT importer_name, last_offset, total_records, status,
                           error_message, last_updated
                    FROM api_reference.import_progress
                    WHERE importer_name LIKE 'nhtsa_%%'
                \"\"\"
                progress_params: list[Any] = []
                if dataset:
                    progress_sql += \" AND importer_name = %s\"
                    progress_params.append(PROGRESS_PREFIX + dataset)
                progress_sql += \" ORDER BY importer_name\"
                cursor.execute(progress_sql, progress_params)
                progress_columns = [description.name for description in cursor.description]
                progress = [
                    dict(zip(progress_columns, row, strict=True)) for row in cursor.fetchall()
                ]
        return {\"runs\": runs, \"jobs\": jobs, \"progress\": progress}

    def probe(self) -> ProbeResult:
        url = self._api_url(self.config.api.base_url, \"GetAllMakes\")
        return self.client.probe(url)
