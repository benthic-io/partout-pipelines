from __future__ import annotations

import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

from pipelines.nhtsa.pipeline import build
from partout_pipelines import db
from partout_pipelines.config import CONFIG_ENV, LEGACY_CONFIG_ENV, load_config
from partout_pipelines.stages import Outcome, STAGE_NAMES


class StageMappingTests(unittest.TestCase):
    def test_nhtsa_pipeline_exposes_canonical_nine_stages(self) -> None:
        pipeline = build()
        self.assertEqual(pipeline.stage_order(), STAGE_NAMES)
        self.assertEqual(set(pipeline.stages), set(STAGE_NAMES))

    def test_geocode_is_explicitly_skipped(self) -> None:
        pipeline = build()
        context = Mock()
        self.assertIs(pipeline.stages["05_geocode"](context), Outcome.SKIPPED)


class ConfigResolutionTests(unittest.TestCase):
    def test_canonical_environment_precedes_legacy_environment(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            canonical = root / "partout.toml"
            legacy = root / "parts.toml"
            shutil.copy(ROOT / "partout.toml", canonical)
            shutil.copy(ROOT / "partout.toml", legacy)
            with patch.dict(
                os.environ,
                {CONFIG_ENV: str(canonical), LEGACY_CONFIG_ENV: str(legacy)},
                clear=False,
            ):
                config = load_config()
            self.assertEqual(config.path, canonical.resolve())
            with patch.dict(os.environ, {LEGACY_CONFIG_ENV: str(legacy)}, clear=True):
                config = load_config()
            self.assertEqual(config.path, legacy.resolve())

    def test_legacy_filename_is_a_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            legacy = root / "parts.toml"
            shutil.copy(ROOT / "partout.toml", legacy)
            with patch.dict(os.environ, {}, clear=True):
                with patch("partout_pipelines.config.Path.cwd", return_value=root):
                    config = load_config()
            self.assertEqual(config.path, legacy.resolve())
            self.assertEqual(config.database.name, "partout_nhtsa")
            self.assertEqual(
                config.bdp.repository_url,
                "https://github.com/benthic-io/partout-pipelines",
            )
            self.assertEqual(config.bdp.collection, "parts")
            self.assertEqual(config.bdp.author_identity, "brian@benthic.io")
            self.assertEqual(config.postgrest_url(), "https://benthic.io/parts/NHTSA/")


class DatabaseSafetyTests(unittest.TestCase):
    def test_serving_database_cannot_be_created_as_a_replacement(self) -> None:
        config = load_config(ROOT / "partout.toml")
        with self.assertRaises(db.DatabaseError):
            db.create_database(config, config.database.name)


if __name__ == "__main__":
    unittest.main()
