"""The Service Bus path, tested without a broker or a host.

All of the behaviour lives in `ingest.handle`, so a fake client and a bytes
body are enough. The three cases that matter are redelivery, malformed input,
and a transient failure -- and they must be distinguishable, because the binding
settles the message differently for each.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import pytest

from azure_pipeline.domain.models import CrawlRequest
from azure_pipeline.ingest.message import (
    MalformedMessage,
    handle,
    instance_id_for,
    parse,
)


@dataclass
class FakeStatus:
    """Mimics the shape `client.get_status` returns."""

    class _Name:
        def __init__(self, name: str) -> None:
            self.name = name

    def __init__(self, name: str) -> None:
        self.runtime_status = self._Name(name)


class FakeClient:
    """Records what was started; can be armed with an existing instance."""

    def __init__(self, existing: dict[str, str] | None = None, fail: Exception | None = None):
        self.existing = existing or {}
        self.fail = fail
        self.started: list[tuple[str, str | None, Any]] = []

    async def get_status(self, instance_id: str) -> Any:
        name = self.existing.get(instance_id)
        return FakeStatus(name) if name else None

    async def start_new(self, name: str, instance_id: str | None, payload: Any) -> str:
        if self.fail:
            raise self.fail
        self.started.append((name, instance_id, payload))
        return instance_id or "generated"


def body(**overrides: Any) -> bytes:
    payload = {"categories": ["software"], "max_pages": 2, **overrides}
    return json.dumps(payload).encode()


# --- parsing -----------------------------------------------------------------


def test_a_valid_body_parses_to_a_request():
    request = parse(body(categories=["a", "b"]))
    assert isinstance(request, CrawlRequest)
    assert request.categories == ["a", "b"]


@pytest.mark.parametrize(
    "raw",
    [b"not json", b"", b"\xff\xfe invalid utf8", b"[1,2,3]", b'"a string"', b"null"],
)
def test_bad_bodies_raise_malformed_not_valueerror(raw):
    """One exception type, so the caller can branch on permanent vs transient."""
    with pytest.raises(MalformedMessage):
        parse(raw)


def test_unknown_fields_are_rejected():
    """CrawlRequest forbids extras, so a typo'd field fails loudly."""
    with pytest.raises(MalformedMessage):
        parse(json.dumps({"categoires": ["typo"]}).encode())


# --- instance ids ------------------------------------------------------------


def test_instance_id_is_derived_from_the_message_id():
    assert instance_id_for("abc-123", b"{}") == "ingest-abc-123"


def test_instance_id_falls_back_to_a_body_hash():
    """A message with no id would otherwise get a fresh id per delivery."""
    first = instance_id_for(None, b'{"categories":["a"]}')
    second = instance_id_for(None, b'{"categories":["a"]}')
    assert first == second
    assert first.startswith("ingest-body-")


def test_different_bodies_get_different_fallback_ids():
    assert instance_id_for(None, b'{"a":1}') != instance_id_for(None, b'{"a":2}')


# --- the handler -------------------------------------------------------------


async def test_a_new_message_starts_an_orchestration():
    client = FakeClient()
    outcome = await handle(client, body(), message_id="m1")

    assert outcome.action == "started"
    assert outcome.instance_id == "ingest-m1"
    assert len(client.started) == 1
    assert client.started[0][0] == "crawl_orchestrator"


@pytest.mark.parametrize("status", ["Running", "Pending", "ContinuedAsNew"])
async def test_redelivery_of_a_live_instance_is_deduplicated(status):
    """At-least-once delivery must not start the same crawl twice."""
    client = FakeClient(existing={"ingest-m1": status})
    outcome = await handle(client, body(), message_id="m1", delivery_count=2)

    assert outcome.action == "deduplicated"
    assert client.started == [], "a second orchestration must not be started"


@pytest.mark.parametrize("status", ["Completed", "Failed", "Terminated"])
async def test_a_finished_instance_does_not_block_a_restart(status):
    """Dedup covers work in flight, not work that already finished."""
    client = FakeClient(existing={"ingest-m1": status})
    outcome = await handle(client, body(), message_id="m1")

    assert outcome.action == "started"
    assert len(client.started) == 1


async def test_a_malformed_message_is_rejected_not_raised():
    """Permanent failure: raising would burn five deliveries to learn nothing."""
    client = FakeClient()
    outcome = await handle(client, b"not json at all", message_id="m1")

    assert outcome.action == "rejected"
    assert outcome.should_complete is True
    assert client.started == []


async def test_a_transient_failure_raises_so_the_broker_redelivers():
    """The counterpart: this is exactly when redelivery is the right answer."""
    client = FakeClient(fail=RuntimeError("durable client unavailable"))
    with pytest.raises(RuntimeError, match="unavailable"):
        await handle(client, body(), message_id="m1")


async def test_the_payload_reaches_the_orchestration_intact():
    client = FakeClient()
    await handle(client, body(categories=["energy"], max_pages=7), message_id="m1")

    _, _, payload = client.started[0]
    assert payload["categories"] == ["energy"]
    assert payload["max_pages"] == 7


async def test_two_distinct_messages_start_two_orchestrations():
    client = FakeClient()
    await handle(client, body(), message_id="m1")
    await handle(client, body(), message_id="m2")

    assert {i for _, i, _ in client.started} == {"ingest-m1", "ingest-m2"}
