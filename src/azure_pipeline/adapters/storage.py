"""Azure Storage: Table for records, Blob for run artifacts.

**Partition key choice is the whole design.** Table Storage guarantees atomic
batches and efficient queries only *within* a partition, and it scales by
distributing partitions across nodes. So the key trades two things off:

  * Partition by `source` -- few, large partitions. Cheap "everything from this
    directory" queries, but one hot partition per source caps write throughput,
    and 10k records from one source all land on one node.
  * Partition by `record_id` -- one row per partition. Perfect write
    distribution, no cross-record query, no batching.

This uses `source` as the partition and `record_id` as the row key, because the
access pattern here is per-source reporting and the volumes are small. That is
a decision to revisit at scale, not a default, which is why it is named here
rather than buried.

Blob holds the per-run artifact: the full result, written once, not queried.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from azure.core.exceptions import ResourceExistsError
from azure.data.tables import TableServiceClient, UpdateMode
from azure.storage.blob import BlobServiceClient

from ..observability import get_logger

log = get_logger(__name__)

# Table Storage stores flat entities: no nested objects, no lists. Anything
# structured has to be serialized into a string column.
_JSON_COLUMNS = ("categories",)


def _to_entity(record: dict[str, Any]) -> dict[str, Any]:
    entity: dict[str, Any] = {
        "PartitionKey": record.get("source", "unknown"),
        # Deterministic row key: re-running the pipeline upserts the same row
        # rather than appending a duplicate.
        "RowKey": record["record_id"],
        "updated_at": datetime.now(UTC).isoformat(),
    }
    for key, value in record.items():
        if key in ("source", "record_id"):
            continue
        if key in _JSON_COLUMNS or isinstance(value, (list, dict)):
            entity[key] = json.dumps(value)
        elif value is None or isinstance(value, (str, int, float, bool)):
            entity[key] = value
        else:
            entity[key] = str(value)
    return entity


class RecordStore:
    def __init__(self, connection_string: str, table_name: str = "records") -> None:
        self._service = TableServiceClient.from_connection_string(connection_string)
        self.table_name = table_name

    def ensure_table(self) -> None:
        try:
            self._service.create_table(self.table_name)
        except ResourceExistsError:
            pass

    def upsert_many(self, records: list[dict[str, Any]]) -> int:
        """Upsert records, batched per partition.

        Table Storage batches are transactional but constrained: max 100
        entities, and **every entity must share a partition key**. Mixing
        partitions in one batch is rejected outright, so the grouping below is
        required rather than an optimisation.
        """
        if not records:
            return 0
        self.ensure_table()
        client = self._service.get_table_client(self.table_name)

        by_partition: dict[str, list[dict[str, Any]]] = {}
        for record in records:
            entity = _to_entity(record)
            by_partition.setdefault(entity["PartitionKey"], []).append(entity)

        written = 0
        for partition, entities in by_partition.items():
            for start in range(0, len(entities), 100):
                window = entities[start : start + 100]
                # MERGE rather than REPLACE: an upsert should not silently drop
                # columns a later schema version added but this payload lacks.
                client.submit_transaction(
                    [("upsert", e, {"mode": UpdateMode.MERGE}) for e in window]
                )
                written += len(window)
            log.info("storage.partition_written", partition=partition, rows=len(entities))
        return written

    def count(self) -> int:
        self.ensure_table()
        client = self._service.get_table_client(self.table_name)
        return sum(1 for _ in client.list_entities())

    def query_source(self, source: str) -> list[dict[str, Any]]:
        """Single-partition query -- the access pattern the key was chosen for."""
        self.ensure_table()
        client = self._service.get_table_client(self.table_name)
        return [
            dict(e)
            for e in client.query_entities("PartitionKey eq @source", parameters={"source": source})
        ]


class ArtifactStore:
    def __init__(self, connection_string: str, container: str = "artifacts") -> None:
        self._service = BlobServiceClient.from_connection_string(connection_string)
        self.container = container

    def ensure_container(self) -> None:
        try:
            self._service.create_container(self.container)
        except ResourceExistsError:
            pass

    def write_json(self, name: str, payload: dict[str, Any]) -> str:
        self.ensure_container()
        blob = self._service.get_blob_client(self.container, name)
        blob.upload_blob(json.dumps(payload, indent=2).encode(), overwrite=True)
        return f"{self.container}/{name}"

    def read_json(self, name: str) -> dict[str, Any]:
        blob = self._service.get_blob_client(self.container, name)
        return json.loads(blob.download_blob().readall())

    def list_names(self) -> list[str]:
        self.ensure_container()
        return [b.name for b in self._service.get_container_client(self.container).list_blobs()]
