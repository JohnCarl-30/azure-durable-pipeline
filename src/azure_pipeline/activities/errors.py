"""Permanent versus transient failure.

Durable Functions has no per-policy list of non-retryable error types -- unlike
Temporal, a `RetryOptions` retries every exception identically. So the
distinction has to live in the activity: a permanent failure is **returned**,
never raised, and only a genuinely transient one is allowed to propagate.

Getting this wrong is expensive in a specific way: a malformed page or a 404
raised out of an activity burns the entire retry budget (six attempts with
growing backoff, up to thirty minutes) re-doing something that cannot succeed.
"""

from __future__ import annotations


class TransientError(RuntimeError):
    """Worth retrying: a timeout, a 5xx, a rate limit. Raise this."""


class PermanentError(RuntimeError):
    """Not worth retrying: a 404, malformed input, a schema violation.

    Activities catch this and fold it into their result rather than letting it
    reach the orchestrator, so one bad page cannot consume the batch's budget.
    """


def classify_status(status: int) -> type[Exception] | None:
    """HTTP status -> which kind of failure, or None when it succeeded."""
    if status < 400:
        return None
    if status in (408, 425, 429) or status >= 500:
        return TransientError
    return PermanentError
