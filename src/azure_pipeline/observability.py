"""Structured logging.

In a real Function App these lines land in Application Insights via the host's
built-in logger, so the job here is to emit structured records and let the host
ship them -- not to configure an exporter.

One Durable-specific hazard: an orchestrator's log statements re-execute on
every replay, so a line written naively appears once per replay rather than
once per event. `orchestration/orchestrators.py` guards every call with
`context.is_replaying`; activities have no such problem, because they run
exactly once per attempt.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog

_configured = False


def configure_logging(level: str = "INFO", json_output: bool = True) -> None:
    global _configured
    if _configured:
        return
    logging.basicConfig(
        format="%(message)s", stream=sys.stdout, level=getattr(logging, level.upper())
    )
    processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.format_exc_info,
    ]
    processors.append(
        structlog.processors.JSONRenderer() if json_output else structlog.dev.ConsoleRenderer()
    )
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, level.upper())),
        cache_logger_on_first_use=True,
    )
    _configured = True


def get_logger(name: str) -> Any:
    return structlog.get_logger(name)
