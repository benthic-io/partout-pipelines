from __future__ import annotations

import logging
from pathlib import Path

_LOGGER_NAME = "partout_pipelines"


def setup_logging(
    log_dir: Path | None = None,
    level: str = "INFO",
) -> logging.Logger:
    logger = logging.getLogger(_LOGGER_NAME)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    if logger.__dict__.get("_partout_configured", False):
        return logger
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    logger.addHandler(console)
    if log_dir is not None:
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_dir / "partout-pipelines.log")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    logger.propagate = False
    logger.__dict__["_partout_configured"] = True
    return logger


def get(dataset: str) -> logging.Logger:
    return logging.getLogger(f"{_LOGGER_NAME}.{dataset}")
