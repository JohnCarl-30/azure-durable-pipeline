"""Retry policies, one per failure shape.

Durable Functions expresses retries as `RetryOptions` passed per activity call,
rather than as a policy attached to the activity definition. That is a real
difference from Temporal, and a useful one: the same activity can be retried
differently depending on where it is called from.

The thing that bites people is `max_number_of_attempts` counting the *first*
attempt. A value of 1 means no retry at all, not one retry.

Note what is missing compared to Temporal: there is no per-policy list of
non-retryable error types. Durable Functions retries every failure the same
way, so "do not retry this" has to be expressed by the activity itself -- it
catches the permanent error and returns a failure result instead of raising.
`activities/errors.py` is where that convention lives.
"""

from __future__ import annotations

from datetime import timedelta

import azure.durable_functions as df


def scrape_retry() -> df.RetryOptions:
    """Crawling: cheap per attempt, frequently transient (429s, blips)."""
    options = df.RetryOptions(
        first_retry_interval_in_milliseconds=2_000,
        max_number_of_attempts=6,
    )
    options.backoff_coefficient = 2.0
    options.max_retry_interval_in_milliseconds = int(timedelta(minutes=5).total_seconds() * 1000)
    # Bounds the whole retry sequence, not one attempt. Without it, six attempts
    # with growing intervals can outlive whatever is waiting upstream.
    options.retry_timeout_in_milliseconds = int(timedelta(minutes=30).total_seconds() * 1000)
    return options


def api_retry() -> df.RetryOptions:
    """Paid third-party calls: each attempt costs money, so fewer and slower."""
    options = df.RetryOptions(
        first_retry_interval_in_milliseconds=5_000,
        max_number_of_attempts=4,
    )
    options.backoff_coefficient = 2.0
    options.max_retry_interval_in_milliseconds = int(timedelta(minutes=10).total_seconds() * 1000)
    options.retry_timeout_in_milliseconds = int(timedelta(minutes=40).total_seconds() * 1000)
    return options


def storage_retry() -> df.RetryOptions:
    """Writes to our own storage: fast, local, usually a transient blip."""
    options = df.RetryOptions(
        first_retry_interval_in_milliseconds=1_000,
        max_number_of_attempts=5,
    )
    options.backoff_coefficient = 2.0
    options.max_retry_interval_in_milliseconds = int(timedelta(minutes=1).total_seconds() * 1000)
    return options
