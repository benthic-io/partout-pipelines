from __future__ import annotations

import sys
import unittest
from datetime import UTC, datetime
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from parts_pipelines.config import ApiConfig, ConfigError, load_config
from parts_pipelines.nhtsa import (
    CircuitBreakerOpen,
    DataError,
    NhtsaClient,
    extract_results,
    normalize_equipment_plant,
    parse_microsoft_timestamp,
    parse_models_csv,
    parse_retry_after,
    should_stop_manufacturer_pagination,
)


class FakeResponse(requests.Response):
    def __init__(self, status: int, body: bytes, retry_after: str | None = None) -> None:
        super().__init__()
        self.status_code = status
        self._content = body
        self.url = "https://vpic.nhtsa.dot.gov/api/vehicles/test?format=json"
        self.headers["Retry-After"] = retry_after or ""


class FakeSession(requests.Session):
    def __init__(self, responses: list[FakeResponse]) -> None:
        super().__init__()
        self.responses = responses
        self.calls = 0

    def get(self, *args: object, **kwargs: object) -> FakeResponse:
        response = self.responses[self.calls]
        self.calls += 1
        return response

    def close(self) -> None:
        return None


def api_config(max_attempts: int = 2) -> ApiConfig:
    return ApiConfig(
        base_url="https://vpic.nhtsa.dot.gov/api",
        request_interval_seconds=1.0,
        timeout_seconds=1.0,
        max_attempts=max_attempts,
        backoff_seconds=2.0,
        max_backoff_seconds=10.0,
        max_retry_after_seconds=30.0,
        user_agent="parts-pipelines-test/1.0",
        all_models_csv_url="https://vpic.nhtsa.dot.gov/api/vehicles/models?format=csv",
        equipment_year=2026,
        equipment_types=(1, 3, 13, 16),
    )


class HttpClientTests(unittest.TestCase):
    def test_403_opens_circuit_without_retry(self) -> None:
        session = FakeSession([FakeResponse(403, b"blocked")])
        sleeps: list[float] = []
        client = NhtsaClient(
            api_config(),
            session=session,
            sleep=sleeps.append,
            random_value=lambda low, high: low,
        )
        with self.assertRaises(CircuitBreakerOpen):
            client.request("https://vpic.nhtsa.dot.gov/api/vehicles/test", response_format="json")
        self.assertEqual(session.calls, 1)
        self.assertEqual(sleeps, [])

    def test_429_honors_retry_after_then_succeeds(self) -> None:
        session = FakeSession(
            [
                FakeResponse(429, b"slow down", retry_after="2"),
                FakeResponse(200, b'{"Count":0,"Results":[]}'),
            ]
        )
        sleeps: list[float] = []
        client = NhtsaClient(
            api_config(),
            session=session,
            sleep=sleeps.append,
            random_value=lambda low, high: low,
        )
        result = client.request(
            "https://vpic.nhtsa.dot.gov/api/vehicles/test",
            response_format="json",
        )
        self.assertEqual(result.http_status, 200)
        self.assertEqual(result.attempts, 2)
        self.assertEqual(session.calls, 2)
        self.assertIn(2.0, sleeps)


class ConfigTests(unittest.TestCase):
    def test_shipped_config_meets_rate_and_equipment_requirements(self) -> None:
        config = load_config(ROOT / "parts.toml")
        self.assertGreaterEqual(config.api.request_interval_seconds, 1.0)
        self.assertEqual(config.api.equipment_types, (1, 3, 13, 16))

    def test_rate_override_cannot_go_below_one_second(self) -> None:
        config = load_config(ROOT / "parts.toml")
        with self.assertRaises(ConfigError):
            config.with_request_interval(0.99)


class TimestampParsingTests(unittest.TestCase):
    def test_parses_microsoft_epoch_milliseconds(self) -> None:
        parsed = parse_microsoft_timestamp("/Date(0)/")
        self.assertEqual(parsed, datetime(1970, 1, 1, tzinfo=UTC))

    def test_parses_negative_and_fractional_dates(self) -> None:
        parsed = parse_microsoft_timestamp("/Date(-1)/")
        if parsed is None:
            self.fail("expected a parsed timestamp")
        self.assertEqual(parsed.isoformat(), "1969-12-31T23:59:59.999000+00:00")

    def test_rejects_malformed_and_out_of_range_values(self) -> None:
        self.assertIsNone(parse_microsoft_timestamp("2026-01-01"))
        self.assertIsNone(parse_microsoft_timestamp("/Date(not-a-number)/"))
        self.assertIsNone(parse_microsoft_timestamp("/Date(999999999999999999999999)/"))


class EquipmentNormalizationTests(unittest.TestCase):
    def test_normalizes_all_equipment_fields(self) -> None:
        row = normalize_equipment_plant(
            {
                "DOTCode": " 00T ",
                "OldDotCode": "",
                "Name": " Example Plant ",
                "Address": "1 Plant Road",
                "City": "Clarksville",
                "StateProvince": "TN",
                "PostalCode": 37040,
                "Country": "UNITED STATES (USA)",
                "Status": "Active",
                "LastUpdated": "/Date(0)/",
            },
            1,
            2026,
        )
        self.assertEqual(row["equipment_type"], 1)
        self.assertEqual(row["plant_year"], 2026)
        self.assertEqual(row["dot_code"], "00T")
        self.assertIsNone(row["old_dot_code"])
        self.assertEqual(row["plant_name"], "Example Plant")
        self.assertEqual(row["plant_address"], "1 Plant Road")
        self.assertEqual(row["plant_city"], "Clarksville")
        self.assertEqual(row["plant_state"], "TN")
        self.assertEqual(row["plant_postal_code"], "37040")
        self.assertEqual(row["plant_country"], "UNITED STATES (USA)")
        self.assertEqual(row["plant_status"], "Active")
        self.assertEqual(row["source_updated_at"], datetime(1970, 1, 1, tzinfo=UTC))

    def test_requires_stable_dot_code(self) -> None:
        with self.assertRaises(DataError):
            normalize_equipment_plant({"Name": "Missing"}, 1, 2026)


class ManufacturerPaginationTests(unittest.TestCase):
    def test_terminates_only_on_empty_page(self) -> None:
        self.assertFalse(should_stop_manufacturer_pagination([{"Mfr_ID": 1}]))
        self.assertTrue(should_stop_manufacturer_pagination([]))


class EnvelopeTests(unittest.TestCase):
    def test_accepts_explicit_empty_result(self) -> None:
        payload = {"Count": 0, "Message": "No results", "Results": []}
        self.assertEqual(extract_results(payload, "test"), [])

    def test_rejects_empty_and_error_envelopes(self) -> None:
        with self.assertRaises(DataError):
            extract_results({}, "test")
        with self.assertRaises(DataError):
            extract_results({"Error": "bad request"}, "test")


class ModelsCsvTests(unittest.TestCase):
    def test_preserves_year_when_available(self) -> None:
        rows = parse_models_csv(
            "Make_ID,Make_Name,Model_ID,Model_Name,ModelYear\n468,BUICK,1831,Roadmaster,1993\n"
        )
        self.assertEqual(
            rows,
            [
                {
                    "make_id": 468,
                    "make_name": "BUICK",
                    "model_id": 1831,
                    "model_name": "Roadmaster",
                    "year": 1993,
                }
            ],
        )


class RetryAfterTests(unittest.TestCase):
    def test_parses_seconds(self) -> None:
        self.assertEqual(parse_retry_after("12.5"), 12.5)

    def test_parses_http_date(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=UTC)
        self.assertEqual(
            parse_retry_after("Thu, 01 Jan 2026 00:00:30 GMT", now=now),
            30.0,
        )


if __name__ == "__main__":
    unittest.main()
