"""Orchestrator functions.

An orchestrator is **replayed from its history** every time it resumes. The
function body re-executes from the top on each replay, and Durable Functions
returns the recorded result for every task that already completed rather than
running it again. That imposes exactly the constraints a Temporal workflow has:

  * No I/O. Anything that touches the network or disk belongs in an activity.
  * No `datetime.now()`, no `random`, no `uuid4()`. Use
    `context.current_utc_datetime` and `context.new_guid()`, which replay to
    the same values.
  * No unordered iteration. A `set` iterates in a different order on a
    different process, which silently changes the sequence of scheduled tasks.

The one that surprises people is **logging**. A plain `log.info` in an
orchestrator fires again on every replay, so a workflow that resumes ten times
logs the same line ten times. `context.is_replaying` guards against that, and
this module routes every log through `_log` so it cannot be forgotten.
"""

from __future__ import annotations

from typing import Any

import azure.durable_functions as df

from ..domain.models import BatchRequest, BatchResult, CrawlRequest, CrawlResult, Stage
from ..observability import get_logger
from .retry import api_retry, scrape_retry, storage_retry

log = get_logger(__name__)


def _log(context: df.DurableOrchestrationContext, event: str, **fields: Any) -> None:
    """Log once per real execution, not once per replay."""
    if not context.is_replaying:
        log.info(event, instance_id=context.instance_id, **fields)


def crawl_orchestrator(context: df.DurableOrchestrationContext):
    """Top level: discover URLs, fan out to per-batch sub-orchestrations.

    Each batch is a **sub-orchestration** rather than a bare activity call, for
    the same reason it is a child workflow under Temporal: it gets its own
    history and its own failure boundary, so one poisoned batch of ten URLs
    fails alone instead of taking the whole crawl with it.
    """
    payload = context.get_input() or {}
    request = CrawlRequest.model_validate(payload)
    result = CrawlResult(instance_id=context.instance_id, stage=Stage.DISCOVERING)

    # Totals carried across a continue_as_new boundary.
    for field, value in (request.carried_totals or {}).items():
        if hasattr(result, field):
            setattr(result, field, getattr(result, field) + value)

    context.set_custom_status({"stage": Stage.DISCOVERING})

    if request.pending_urls:
        # Continuation run: this work was already discovered upstream.
        urls = list(request.pending_urls)
    else:
        # Discovery is parallel across categories and sequential within one,
        # because page N+1's URL is only known once page N is parsed.
        discoveries = [
            context.call_activity_with_retry(
                "discover_listings",
                scrape_retry(),
                {"category": category, "max_pages": request.max_pages},
            )
            for category in request.categories
        ]
        discovered: list[list[str]] = yield context.task_all(discoveries)

        # Order-preserving dedupe. A set would reorder between replays and
        # change which URL lands in which batch.
        seen: set[str] = set()
        urls = [url for group in discovered for url in group if not (url in seen or seen.add(url))]

    result.urls_discovered += len(urls)
    _log(context, "crawl.discovered", urls=len(urls))

    # Bound history growth: hand the overflow to a fresh instance.
    overflow: list[str] = []
    max_per_run = 500
    if len(urls) > max_per_run:
        urls, overflow = urls[:max_per_run], urls[max_per_run:]

    batches = [urls[i : i + request.batch_size] for i in range(0, len(urls), request.batch_size)]
    result.batches = len(batches)
    context.set_custom_status({"stage": Stage.FETCHING, "batches": len(batches)})

    # Fan out. Deterministic child instance ids -- derived from the parent's id
    # and the batch index, never from a random guid -- so a replay addresses the
    # same children instead of starting new ones.
    tasks = [
        context.call_sub_orchestrator(
            "batch_orchestrator",
            BatchRequest(
                urls=batch,
                source=request.source,
                enrich=request.enrich,
                batch_index=index,
            ).to_json(),
            f"{context.instance_id}:batch:{index}",
        )
        for index, batch in enumerate(batches)
    ]

    if tasks:
        # task_all is the fan-in. It raises if any child failed, so the results
        # are gathered with the failure boundary inside each child instead.
        batch_payloads: list[dict[str, Any]] = yield context.task_all(tasks)
        for payload_out in batch_payloads:
            result.merge_batch(BatchResult.model_validate(payload_out))

    result.stage = Stage.DONE

    # Write the run summary before reporting done, so a completed instance
    # always has a durable artifact to point at. Keyed by instance id, so a
    # replay overwrites rather than accumulating versions.
    yield context.call_activity_with_retry("write_run_artifact", storage_retry(), result.to_json())

    context.set_custom_status({"stage": Stage.DONE, "indexed": result.indexed})
    _log(context, "crawl.complete", indexed=result.indexed, failures=len(result.failures))

    if overflow:
        result.continued_as_new = True
        # continue_as_new restarts this orchestration with a clean history,
        # carrying the work still to do and the totals so far.
        context.continue_as_new(
            request.model_copy(
                update={
                    "categories": [],
                    "max_pages": 0,
                    "pending_urls": overflow,
                    "carried_totals": {
                        "urls_discovered": result.urls_discovered,
                        "extracted": result.extracted,
                        "enriched": result.enriched,
                        "indexed": result.indexed,
                    },
                }
            ).to_json()
        )
        return result.to_json()

    return result.to_json()


def batch_orchestrator(context: df.DurableOrchestrationContext):
    """One batch: fetch and extract, enrich, then index.

    Failures are caught and reported rather than raised, because this runs
    under `task_all` in the parent -- an exception here would abort the sibling
    batches too.
    """
    request = BatchRequest.model_validate(context.get_input() or {})
    result = BatchResult(batch_index=request.batch_index)

    try:
        records: list[dict[str, Any]] = yield context.call_activity_with_retry(
            "fetch_and_extract",
            scrape_retry(),
            {"urls": request.urls, "source": request.source},
        )
        result.extracted = len(records)

        if request.enrich and records:
            records = yield context.call_activity_with_retry(
                "enrich_records", api_retry(), {"records": records}
            )
            result.enriched = sum(1 for r in records if r.get("enriched"))

        if records:
            result.indexed = yield context.call_activity_with_retry(
                "persist_records", storage_retry(), {"records": records}
            )

    except Exception as exc:
        # Retries are already exhausted by the time this fires.
        result.failures.append(f"batch {request.batch_index}: {exc}")
        _log(context, "batch.failed", batch=request.batch_index, error=str(exc))

    return result.to_json()


def approval_orchestrator(context: df.DurableOrchestrationContext):
    """Human-in-the-loop: wait for an external approval, with a timeout.

    The pattern worth showing. `wait_for_external_event` suspends the
    orchestration with no process running and no polling -- it can wait for
    days at zero compute cost, which is the whole point of durable execution.

    The timer must be cancelled explicitly once the race is decided, or the
    orchestration stays alive until it fires.
    """
    payload = context.get_input() or {}
    timeout_hours = int(payload.get("timeout_hours", 24))

    deadline = context.current_utc_datetime + __import__("datetime").timedelta(hours=timeout_hours)
    timeout_task = context.create_timer(deadline)
    approval_task = context.wait_for_external_event("ApprovalReceived")

    winner = yield context.task_any([approval_task, timeout_task])

    if winner == approval_task:
        timeout_task.cancel()  # or the instance lingers until the timer fires
        decision = approval_task.result
        _log(context, "approval.received", approved=bool(decision))
        return {"approved": bool(decision), "timed_out": False}

    _log(context, "approval.timed_out", hours=timeout_hours)
    return {"approved": False, "timed_out": True}
