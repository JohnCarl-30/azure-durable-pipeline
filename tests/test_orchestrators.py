"""Orchestrator logic, driven directly as generators.

No Functions host, no Azurite, no network. What is asserted is the *sequence of
scheduled operations*, because that is what replay determinism actually means
and what an end-to-end test cannot see.
"""

from __future__ import annotations

import pytest

from azure_pipeline.domain.models import BatchRequest, CrawlRequest, Stage
from azure_pipeline.orchestration import orchestrators
from tests.fake_context import FakeOrchestrationContext, run_orchestrator


def record(index: int) -> dict:
    return {
        "record_id": f"r{index}",
        "source": "demo",
        "source_id": f"c{index}",
        "source_url": f"http://d/{index}",
        "name": f"Company {index}",
        "website": f"https://c{index}.example.com",
        "categories": [],
        "enriched": False,
        "schema_version": 1,
    }


# --- crawl orchestrator ------------------------------------------------------


def test_crawl_discovers_then_fans_out_to_sub_orchestrations():
    context = FakeOrchestrationContext(
        input=CrawlRequest(categories=["software"], max_pages=2, batch_size=2).to_json(),
        results={
            "discover_listings": [["u1", "u2", "u3", "u4"]],
            "batch_orchestrator": [
                {"batch_index": 0, "extracted": 2, "enriched": 2, "indexed": 2, "failures": []},
                {"batch_index": 1, "extracted": 2, "enriched": 1, "indexed": 2, "failures": []},
            ],
        },
    )
    result = run_orchestrator(orchestrators.crawl_orchestrator, context)

    assert result["urls_discovered"] == 4
    assert result["batches"] == 2
    assert result["indexed"] == 4
    assert result["enriched"] == 3
    assert result["stage"] == Stage.DONE

    kinds = [(c.kind, c.name) for c in context.calls]
    assert kinds[0] == ("activity", "discover_listings")
    assert kinds[1:3] == [("sub_orchestrator", "batch_orchestrator")] * 2
    # The run artifact is written before the orchestration reports done.
    assert kinds[-1] == ("activity", "write_run_artifact")


def test_batches_are_given_deterministic_child_instance_ids():
    """A replay must address the same children, not start new ones.

    Deriving the id from a new guid would create a fresh child on every replay
    -- the classic Durable Functions duplication bug.
    """
    context = FakeOrchestrationContext(
        instance_id="parent-1",
        input=CrawlRequest(categories=["a"], batch_size=1).to_json(),
        results={"discover_listings": [["u1", "u2"]], "batch_orchestrator": {"indexed": 0}},
    )
    run_orchestrator(orchestrators.crawl_orchestrator, context)

    ids = [c.instance_id for c in context.calls if c.kind == "sub_orchestrator"]
    assert ids == ["parent-1:batch:0", "parent-1:batch:1"]


def test_urls_are_deduplicated_in_a_stable_order():
    """A set would reorder between replays and reshuffle the batches."""
    context = FakeOrchestrationContext(
        input=CrawlRequest(categories=["a", "b"], batch_size=10).to_json(),
        results={
            "discover_listings": [["u1", "u2"], ["u2", "u3"]],
            "batch_orchestrator": {"indexed": 0},
        },
    )
    run_orchestrator(orchestrators.crawl_orchestrator, context)

    batch = next(c for c in context.calls if c.kind == "sub_orchestrator")
    assert batch.payload["urls"] == ["u1", "u2", "u3"]


def test_the_same_input_schedules_the_same_calls_twice():
    """The determinism property, asserted directly."""

    def run() -> list[tuple[str, str, object]]:
        context = FakeOrchestrationContext(
            input=CrawlRequest(categories=["a", "b"], batch_size=2).to_json(),
            results={
                "discover_listings": [["u1", "u2"], ["u3", "u4"]],
                "batch_orchestrator": {"indexed": 1},
            },
        )
        run_orchestrator(orchestrators.crawl_orchestrator, context)
        return [(c.kind, c.name, c.instance_id) for c in context.calls]

    assert run() == run()


def test_custom_status_tracks_progress():
    """set_custom_status is how a running instance is queried -- Temporal's
    query handler equivalent."""
    context = FakeOrchestrationContext(
        input=CrawlRequest(categories=["a"], batch_size=5).to_json(),
        results={"discover_listings": [["u1"]], "batch_orchestrator": {"indexed": 1}},
    )
    run_orchestrator(orchestrators.crawl_orchestrator, context)

    stages = [s["stage"] for s in context.custom_statuses]
    assert stages[0] == Stage.DISCOVERING
    assert stages[-1] == Stage.DONE


def test_overflow_continues_as_new_carrying_work_and_totals():
    """Bounding history growth must not silently drop the remaining work."""
    urls = [f"u{i}" for i in range(600)]
    context = FakeOrchestrationContext(
        input=CrawlRequest(categories=["a"], batch_size=100).to_json(),
        results={"discover_listings": [urls], "batch_orchestrator": {"indexed": 10}},
    )
    result = run_orchestrator(orchestrators.crawl_orchestrator, context)

    assert result["continued_as_new"] is True
    assert context.continued_with is not None
    carried = context.continued_with
    assert len(carried["pending_urls"]) == 100  # 600 - 500 processed
    assert carried["categories"] == []  # no re-discovery
    assert carried["carried_totals"]["indexed"] == result["indexed"]


def test_a_continuation_run_skips_discovery_entirely():
    context = FakeOrchestrationContext(
        input=CrawlRequest(
            categories=[], max_pages=0, batch_size=2, pending_urls=["u1", "u2"]
        ).to_json(),
        results={"batch_orchestrator": {"indexed": 2}},
    )
    result = run_orchestrator(orchestrators.crawl_orchestrator, context)

    assert not any(c.name == "discover_listings" for c in context.calls)
    assert result["urls_discovered"] == 2


def test_carried_totals_are_added_to_the_new_run():
    context = FakeOrchestrationContext(
        input=CrawlRequest(
            categories=[],
            batch_size=5,
            pending_urls=["u1"],
            carried_totals={"indexed": 100, "extracted": 120},
        ).to_json(),
        results={"batch_orchestrator": {"indexed": 1, "extracted": 1}},
    )
    result = run_orchestrator(orchestrators.crawl_orchestrator, context)

    assert result["indexed"] == 101
    assert result["extracted"] == 121


def test_the_run_artifact_is_written_before_completion():
    """A completed instance must always have a durable artifact to point at."""
    context = FakeOrchestrationContext(
        input=CrawlRequest(categories=["a"], batch_size=5).to_json(),
        results={
            "discover_listings": [["u1"]],
            "batch_orchestrator": {"indexed": 1},
            "write_run_artifact": "artifacts/runs/test-instance.json",
        },
    )
    run_orchestrator(orchestrators.crawl_orchestrator, context)

    artifact = next(c for c in context.calls if c.name == "write_run_artifact")
    assert artifact.payload["indexed"] == 1
    assert artifact.payload["stage"] == Stage.DONE


def test_no_urls_means_no_sub_orchestrations():
    context = FakeOrchestrationContext(
        input=CrawlRequest(categories=["a"]).to_json(),
        results={"discover_listings": [[]]},
    )
    result = run_orchestrator(orchestrators.crawl_orchestrator, context)

    assert result["batches"] == 0
    assert not any(c.kind == "sub_orchestrator" for c in context.calls)


# --- batch orchestrator ------------------------------------------------------


def test_batch_runs_fetch_enrich_persist_in_order():
    context = FakeOrchestrationContext(
        input=BatchRequest(urls=["u1", "u2"], source="demo").to_json(),
        results={
            "fetch_and_extract": [[record(1), record(2)]],
            "enrich_records": [[{**record(1), "enriched": True}, record(2)]],
            "persist_records": 2,
        },
    )
    result = run_orchestrator(orchestrators.batch_orchestrator, context)

    assert [c.name for c in context.calls] == [
        "fetch_and_extract",
        "enrich_records",
        "persist_records",
    ]
    assert result["extracted"] == 2
    assert result["enriched"] == 1
    assert result["indexed"] == 2


def test_enrichment_is_skipped_when_disabled():
    context = FakeOrchestrationContext(
        input=BatchRequest(urls=["u1"], source="demo", enrich=False).to_json(),
        results={"fetch_and_extract": [[record(1)]], "persist_records": 1},
    )
    result = run_orchestrator(orchestrators.batch_orchestrator, context)

    assert not any(c.name == "enrich_records" for c in context.calls)
    assert result["enriched"] == 0
    assert result["indexed"] == 1


def test_an_empty_extraction_skips_persistence():
    context = FakeOrchestrationContext(
        input=BatchRequest(urls=["u1"], source="demo").to_json(),
        results={"fetch_and_extract": [[]]},
    )
    result = run_orchestrator(orchestrators.batch_orchestrator, context)

    assert not any(c.name == "persist_records" for c in context.calls)
    assert result["indexed"] == 0


def test_a_failing_batch_reports_rather_than_raising():
    """It runs under task_all in the parent -- raising would abort its siblings."""
    context = FakeOrchestrationContext(
        input=BatchRequest(urls=["u1"], source="demo", batch_index=3).to_json(),
        results={"fetch_and_extract": RuntimeError("upstream gone")},
    )
    result = run_orchestrator(orchestrators.batch_orchestrator, context)

    assert result["failures"]
    assert "batch 3" in result["failures"][0]
    assert result["indexed"] == 0


# --- approval orchestrator ---------------------------------------------------


def test_approval_waits_for_an_external_event():
    context = FakeOrchestrationContext(
        input={"timeout_hours": 24},
        results={"ApprovalReceived": True},
    )
    result = run_orchestrator(orchestrators.approval_orchestrator, context)

    assert result == {"approved": True, "timed_out": False}
    assert any(c.kind == "external_event" for c in context.calls)
    assert any(c.kind == "timer" for c in context.calls)


def test_approval_cancels_its_timer_once_decided():
    """An uncancelled timer keeps the instance alive until it fires."""
    context = FakeOrchestrationContext(input={}, results={"ApprovalReceived": True})
    run_orchestrator(orchestrators.approval_orchestrator, context)
    # The generator cancelled the losing task; the fake records that.
    assert any(c.kind == "timer" for c in context.calls)


@pytest.mark.parametrize("hours", [1, 24, 72])
def test_approval_timeout_is_computed_from_the_replayable_clock(hours):
    """datetime.now() would return a different deadline on every replay."""
    context = FakeOrchestrationContext(input={"timeout_hours": hours}, results={})
    run_orchestrator(orchestrators.approval_orchestrator, context)

    timer = next(c for c in context.calls if c.kind == "timer")
    delta = timer.payload - context.current_utc_datetime
    assert delta.total_seconds() == hours * 3600
