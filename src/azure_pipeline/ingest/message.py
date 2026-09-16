"""Turning a Service Bus message into a started orchestration.

The trigger binding itself is three lines in `function_app.py`; everything that
can be got wrong lives here, so it can be tested without a host or a broker.

Three decisions worth stating, because none of them are the default:

**Delivery is at least once.** The broker redelivers on any consumer crash,
lock expiry or rebalance, so the same message will arrive again. The instance
id is derived from the message id rather than generated, which turns a
redelivery into a no-op: Durable Functions refuses to start a second instance
with an id that is already running, and that refusal is the deduplication.

**A malformed message must not be retried.** It will fail identically five
times and then dead-letter, having consumed five delivery attempts and five
lock durations to learn nothing. This mirrors the permanent/transient split the
activities already use: a parse failure is permanent, so the message is
completed and recorded, not thrown back.

**A transient failure must be retried.** If the Durable client itself is
unavailable, raising is correct -- the message returns to the queue and the
broker redelivers it. The distinction is the whole point; collapsing both into
one `except` loses it.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Protocol

from ..domain.models import CrawlRequest
from ..observability import get_logger

log = get_logger(__name__)


class MalformedMessage(ValueError):
    """Permanent. Retrying cannot help, so the message is completed, not thrown."""


@dataclass
class IngestOutcome:
    """What the trigger did, so the caller can log and tests can assert."""

    action: str  # started | deduplicated | rejected
    instance_id: str | None = None
    reason: str | None = None

    @property
    def should_complete(self) -> bool:
        """Every outcome here completes the message.

        A transient failure never reaches this type -- it raises, and the
        binding abandons the message for redelivery.
        """
        return True


class DurableClient(Protocol):
    """Only the surface this module uses, so tests need no real client."""

    async def start_new(self, name: str, instance_id: str | None, payload: Any) -> str: ...

    async def get_status(self, instance_id: str) -> Any: ...


def instance_id_for(message_id: str | None, body: bytes) -> str:
    """Deterministic id, so a redelivery addresses the same orchestration.

    Prefers the broker's message id. Falls back to a hash of the body, because
    a message published without one would otherwise get a fresh id per delivery
    and defeat the deduplication entirely.
    """
    if message_id:
        return f"ingest-{message_id}"
    digest = hashlib.sha256(body).hexdigest()[:32]
    return f"ingest-body-{digest}"


def parse(body: bytes) -> CrawlRequest:
    """Bytes to a validated request. Raises MalformedMessage, never ValueError."""
    try:
        payload = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise MalformedMessage(f"body is not valid JSON: {exc}") from exc

    if not isinstance(payload, dict):
        raise MalformedMessage(f"body must be a JSON object, got {type(payload).__name__}")

    try:
        return CrawlRequest.model_validate(payload)
    except Exception as exc:
        raise MalformedMessage(f"body is not a CrawlRequest: {exc}") from exc


# Statuses that mean an instance with this id is still doing the work.
_LIVE_STATUSES = {"Running", "Pending", "ContinuedAsNew"}


async def handle(
    client: DurableClient,
    body: bytes,
    *,
    message_id: str | None = None,
    delivery_count: int = 1,
) -> IngestOutcome:
    """Start an orchestration for this message, or explain why it didn't.

    Raises only for transient failures, which is the signal to the binding that
    the message should go back on the queue.
    """
    try:
        request = parse(body)
    except MalformedMessage as exc:
        # Completed rather than abandoned: five identical failures and a
        # dead-letter teaches nothing that this log line does not.
        log.error(
            "ingest.rejected",
            message_id=message_id,
            delivery_count=delivery_count,
            reason=str(exc),
            body=body[:400].decode("utf-8", errors="replace"),
        )
        return IngestOutcome(action="rejected", reason=str(exc))

    instance_id = instance_id_for(message_id, body)

    existing = await client.get_status(instance_id)
    status = getattr(getattr(existing, "runtime_status", None), "name", None)
    if status in _LIVE_STATUSES:
        # The dedup. A redelivery of a message already being worked on must not
        # start a second crawl over the same categories.
        log.info(
            "ingest.deduplicated",
            instance_id=instance_id,
            status=status,
            delivery_count=delivery_count,
        )
        return IngestOutcome(action="deduplicated", instance_id=instance_id, reason=status)

    started = await client.start_new("crawl_orchestrator", instance_id, request.to_json())
    log.info(
        "ingest.started",
        instance_id=started,
        categories=request.categories,
        delivery_count=delivery_count,
    )
    return IngestOutcome(action="started", instance_id=started)
