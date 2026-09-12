"""A fake DurableOrchestrationContext for testing orchestrators.

Durable Functions orchestrators are **generator functions**: they yield tasks
and receive results back. That makes them testable without the Functions host,
without Azurite and without any I/O -- you drive the generator yourself and
decide what each yielded task returns.

This is the officially recommended way to test orchestrators, and it is much
better than an end-to-end test for the thing that actually matters: the
*sequence of scheduled operations*. Determinism bugs are invisible to an
integration test that happens to pass once, but obvious here, because the
recorded call sequence is the assertion.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


@dataclass
class Call:
    """One scheduled operation, as the orchestrator requested it."""

    kind: str  # activity | sub_orchestrator | timer | external_event
    name: str
    payload: Any = None
    instance_id: str | None = None


class FakeTask:
    """Stands in for a durable task. Carries its own pre-decided result."""

    def __init__(self, call: Call, result: Any = None, raises: Exception | None = None) -> None:
        self.call = call
        self._result = result
        self._raises = raises
        self.cancelled = False

    @property
    def result(self) -> Any:
        return self._result

    def cancel(self) -> None:
        self.cancelled = True


class WhenAll:
    """Marker for a fan-in, so the driver knows to gather several results."""

    def __init__(self, tasks: list[FakeTask]) -> None:
        self.tasks = tasks


class WhenAny:
    def __init__(self, tasks: list[FakeTask]) -> None:
        self.tasks = tasks


@dataclass
class FakeOrchestrationContext:
    """Enough of the real context for the orchestrators under test.

    `results` maps an activity or sub-orchestrator name to either a value, a
    list of values consumed in call order, or an Exception to raise.
    """

    instance_id: str = "test-instance"
    input: Any = None
    results: dict[str, Any] = field(default_factory=dict)
    is_replaying: bool = False
    current_utc_datetime: datetime = field(default_factory=lambda: datetime(2026, 1, 1, tzinfo=UTC))

    calls: list[Call] = field(default_factory=list)
    custom_statuses: list[Any] = field(default_factory=list)
    continued_with: Any = None
    _guid_counter: int = 0
    _call_counts: dict[str, int] = field(default_factory=dict)

    # --- the surface the orchestrators use ---------------------------------

    def get_input(self) -> Any:
        return self.input

    def set_custom_status(self, status: Any) -> None:
        self.custom_statuses.append(status)

    def new_guid(self) -> str:
        """Deterministic across replays, which is the whole point of the real one."""
        self._guid_counter += 1
        return f"00000000-0000-0000-0000-{self._guid_counter:012d}"

    def continue_as_new(self, payload: Any) -> None:
        self.continued_with = payload

    def call_activity(self, name: str, payload: Any = None) -> FakeTask:
        return self._schedule(Call("activity", name, payload))

    def call_activity_with_retry(
        self, name: str, retry_options: Any, payload: Any = None
    ) -> FakeTask:
        # Retry behaviour belongs to the host, not the orchestrator, so the
        # fake records the intent and returns the eventual outcome directly.
        return self._schedule(Call("activity", name, payload))

    def call_sub_orchestrator(
        self, name: str, payload: Any = None, instance_id: str | None = None
    ) -> FakeTask:
        return self._schedule(Call("sub_orchestrator", name, payload, instance_id))

    def call_sub_orchestrator_with_retry(
        self, name: str, retry_options: Any, payload: Any = None, instance_id: str | None = None
    ) -> FakeTask:
        return self._schedule(Call("sub_orchestrator", name, payload, instance_id))

    def create_timer(self, deadline: datetime) -> FakeTask:
        return self._schedule(Call("timer", "timer", deadline))

    def wait_for_external_event(self, name: str) -> FakeTask:
        return self._schedule(Call("external_event", name))

    def task_all(self, tasks: list[FakeTask]) -> WhenAll:
        return WhenAll(tasks)

    def task_any(self, tasks: list[FakeTask]) -> WhenAny:
        return WhenAny(tasks)

    # --- internals ---------------------------------------------------------

    def _schedule(self, call: Call) -> FakeTask:
        self.calls.append(call)
        index = self._call_counts.get(call.name, 0)
        self._call_counts[call.name] = index + 1

        configured = self.results.get(call.name)
        if isinstance(configured, Exception):
            return FakeTask(call, raises=configured)
        if isinstance(configured, list):
            value = configured[index] if index < len(configured) else None
            if isinstance(value, Exception):
                return FakeTask(call, raises=value)
            return FakeTask(call, result=value)
        return FakeTask(call, result=configured)


def run_orchestrator(generator_fn, context: FakeOrchestrationContext) -> Any:
    """Drive an orchestrator generator to completion and return its result.

    Mirrors what the Durable extension does: send each yielded task's result
    back into the generator, and throw into it when a task failed.
    """
    generator = generator_fn(context)
    to_send: Any = None
    to_throw: Exception | None = None

    while True:
        try:
            yielded = generator.throw(to_throw) if to_throw else generator.send(to_send)
        except StopIteration as stop:
            return stop.value

        to_send, to_throw = None, None

        if isinstance(yielded, WhenAll):
            failure = next((t for t in yielded.tasks if t._raises), None)
            if failure is not None:
                # task_all surfaces the first failure, as the real one does.
                to_throw = failure._raises
            else:
                to_send = [t.result for t in yielded.tasks]
        elif isinstance(yielded, WhenAny):
            # Whichever task the test armed with a result wins the race.
            winner = next((t for t in yielded.tasks if t.result is not None), yielded.tasks[0])
            to_send = winner
        elif isinstance(yielded, FakeTask):
            if yielded._raises:
                to_throw = yielded._raises
            else:
                to_send = yielded.result
        else:
            to_send = yielded
