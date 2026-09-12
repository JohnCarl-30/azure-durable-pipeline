"""Persistence activities.

Split from `functions.py` because these are the only activities that touch
Azure Storage rather than the public internet, and they carry the idempotency
burden: the host runs an activity at least once, so a write that is not an
upsert will duplicate after any mid-activity failure.
"""

from __future__ import annotations

from typing import Any

from ..adapters.storage import ArtifactStore, RecordStore
from ..config import get_settings
from ..observability import get_logger

log = get_logger(__name__)


def persist_records(payload: dict[str, Any]) -> int:
    """Upsert records into Table Storage. Safe to run twice."""
    records = list(payload.get("records", []))
    if not records:
        return 0
    settings = get_settings()
    store = RecordStore(settings.azure_webjobs_storage)
    written = store.upsert_many(records)
    log.info("activity.persisted", records=len(records), written=written)
    return written


def write_run_artifact(payload: dict[str, Any]) -> str:
    """Write the run summary to Blob. Keyed by instance id, so a replay overwrites."""
    settings = get_settings()
    store = ArtifactStore(settings.azure_webjobs_storage, settings.artifacts_container)
    instance_id = payload.get("instance_id", "unknown")
    name = f"runs/{instance_id}.json"
    path = store.write_json(name, payload)
    log.info("activity.artifact_written", path=path)
    return path
