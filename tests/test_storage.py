"""Storage adapter against Azurite.

Skipped when the emulator is not running, so the suite stays runnable with
nothing installed. Start it with `npm run azurite`.

These cover the constraints that only a real Table Storage enforces: a
transaction cannot span partitions, batches cap at 100 entities, and an entity
cannot hold a nested object.
"""

from __future__ import annotations

import uuid

import httpx
import pytest

from azure_pipeline.adapters.storage import ArtifactStore, RecordStore, _to_entity
from azure_pipeline.config import AZURITE_CONNECTION_STRING


def _azurite_up() -> bool:
    try:
        # Unauthenticated, so a 400 still proves something is listening.
        httpx.get("http://127.0.0.1:10002/devstoreaccount1", timeout=2)
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _azurite_up(), reason="Azurite is not running (npm run azurite)"
)


def record(index: int, source: str = "demo") -> dict:
    return {
        "record_id": f"{uuid.uuid4().hex[:8]}-{index}",
        "source": source,
        "source_id": f"c{index}",
        "source_url": f"http://d/{index}",
        "name": f"Company {index}",
        "city": "Austin",
        "categories": ["Analytics", "Data"],
        "enriched": False,
        "schema_version": 1,
    }


@pytest.fixture
def store() -> RecordStore:
    s = RecordStore(AZURITE_CONNECTION_STRING, table_name=f"t{uuid.uuid4().hex[:12]}")
    s.ensure_table()
    yield s
    try:
        s._service.delete_table(s.table_name)
    except Exception:
        pass


def test_nested_values_are_serialised_for_a_flat_store():
    """Table Storage entities are flat: no lists, no dicts."""
    entity = _to_entity(record(1))
    assert isinstance(entity["categories"], str)
    assert entity["PartitionKey"] == "demo"
    assert entity["RowKey"]


def test_upsert_writes_and_is_idempotent(store):
    """Activities run at least once, so a second write must not duplicate."""
    records = [record(i) for i in range(5)]
    assert store.upsert_many(records) == 5
    assert store.count() == 5

    store.upsert_many(records)
    assert store.count() == 5, "re-running the pipeline must not duplicate rows"


def test_a_batch_spanning_partitions_is_split(store):
    """Table Storage rejects a transaction that mixes partition keys outright."""
    records = [record(i, source="alpha") for i in range(3)]
    records += [record(i, source="beta") for i in range(3)]

    assert store.upsert_many(records) == 6
    assert len(store.query_source("alpha")) == 3
    assert len(store.query_source("beta")) == 3


def test_batches_larger_than_the_limit_are_chunked(store):
    """The transaction cap is 100 entities; 150 must still write."""
    assert store.upsert_many([record(i) for i in range(150)]) == 150
    assert store.count() == 150


def test_single_partition_query_returns_only_that_source(store):
    store.upsert_many([record(i, source="alpha") for i in range(2)])
    store.upsert_many([record(i, source="beta") for i in range(3)])

    rows = store.query_source("alpha")
    assert len(rows) == 2
    assert {r["PartitionKey"] for r in rows} == {"alpha"}


def test_empty_input_is_a_no_op(store):
    assert store.upsert_many([]) == 0


def test_artifacts_round_trip():
    container = f"c{uuid.uuid4().hex[:12]}"
    artifacts = ArtifactStore(AZURITE_CONNECTION_STRING, container=container)
    artifacts.write_json("runs/instance-1.json", {"indexed": 12, "stage": "done"})

    assert artifacts.read_json("runs/instance-1.json")["indexed"] == 12
    assert "runs/instance-1.json" in artifacts.list_names()


def test_an_artifact_write_overwrites_rather_than_appending():
    """Keyed by instance id, so a replay must not accumulate versions."""
    container = f"c{uuid.uuid4().hex[:12]}"
    artifacts = ArtifactStore(AZURITE_CONNECTION_STRING, container=container)
    artifacts.write_json("runs/x.json", {"attempt": 1})
    artifacts.write_json("runs/x.json", {"attempt": 2})

    assert artifacts.read_json("runs/x.json")["attempt"] == 2
    assert len(artifacts.list_names()) == 1
