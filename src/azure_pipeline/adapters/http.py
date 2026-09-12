"""Outbound HTTP with the failure classification the retry policies depend on.

Deliberately thinner than a full resilience stack: Durable Functions already
owns the retry loop and the backoff, so duplicating it inside the client would
mean two nested retry budgets and a worst case nobody intended. The client's job
is to make one attempt and classify the outcome correctly.
"""

from __future__ import annotations

import httpx

from ..activities.errors import PermanentError, TransientError, classify_status

USER_AGENT = "azure-durable-pipeline/0.1 (+contact: devs@example.com)"


async def fetch(
    client: httpx.AsyncClient,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    params: dict[str, str] | None = None,
) -> str:
    """One attempt. Raises TransientError for anything the host should retry."""
    try:
        response = await client.get(
            url, headers={"User-Agent": USER_AGENT, **(headers or {})}, params=params
        )
    except (httpx.TimeoutException, httpx.TransportError) as exc:
        raise TransientError(f"{url}: {exc}") from exc

    failure = classify_status(response.status_code)
    if failure is TransientError:
        raise TransientError(f"{url}: HTTP {response.status_code}")
    if failure is PermanentError:
        raise PermanentError(f"{url}: HTTP {response.status_code}")
    return response.text
