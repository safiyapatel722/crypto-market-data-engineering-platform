# src/utils/logger.py

import logging
import os
import sys


def get_logger(name: str) -> logging.Logger:
    """
    Return a named logger. Safe to call multiple times for the same name —
    the handler-guard prevents duplicate log lines.

    Log level is controlled by the LOG_LEVEL environment variable.
    Defaults to INFO if not set.

    Examples:
        LOG_LEVEL=DEBUG   → verbose, shows all debug messages
        LOG_LEVEL=WARNING → only warnings and errors (good for production)
        LOG_LEVEL=ERROR   → errors only
    """
    logger = logging.getLogger(name)

    # Guard: prevents adding duplicate handlers when get_logger() is called
    # multiple times for the same module name (very common in large projects).
    if logger.handlers:
        return logger

    # FIX: Read log level from environment so you can change verbosity
    # without touching code. Defaults to INFO.
    raw_level = os.getenv("LOG_LEVEL", "INFO").upper()
    level     = getattr(logging, raw_level, logging.INFO)
    logger.setLevel(level)

    # ── Handler ────────────────────────────────────────────────────────────────
    # Write to stdout so Cloud Run / Cloud Logging picks up all lines uniformly,
    # regardless of level. stderr is for unexpected crashes, not pipeline logs.
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)

    # ── Formatter ──────────────────────────────────────────────────────────────
    # Module name (e.g. "src.pipelines.backfill_pipeline") tells you instantly
    # which part of the code produced each line — critical when debugging.
    formatter = logging.Formatter(
        fmt     = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt = "%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # FIX: Prevents log messages bubbling up to the root logger and being
    # printed twice when root logging is also configured.
    logger.propagate = False

    return logger