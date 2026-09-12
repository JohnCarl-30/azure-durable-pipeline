"""Activity functions: everything that touches the outside world.

Activities are the opposite of orchestrators in every constraint. They run
exactly once per attempt, may do any I/O, and may use the clock and randomness
freely -- but they must be **idempotent**, because the host guarantees
at-least-once execution. An activity that completes its work and then dies
before reporting is re-run from the start.

Each function takes and returns plain JSON-serializable dicts. Durable
Functions stores those payloads in the orchestration history, which is why the
activity signature is a dict rather than a Pydantic model: the model is
reconstructed inside, so a serialization mismatch fails here with a clear
error rather than inside the host's own serializer.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any
from urllib.parse import urljoin, urlsplit

import httpx
from selectolax.parser import HTMLParser

from ..adapters.http import fetch
from ..config import get_settings
from ..domain.models import CompanyRecord
from ..observability import get_logger
from .errors import PermanentError

log = get_logger(__name__)


def _source_id(url: str) -> str:
    path = urlsplit(url).path.rstrip("/")
    return path.rsplit("/", 1)[-1] or path


async def _discover(category: str, max_pages: int) -> list[str]:
    settings = get_settings()
    base = settings.directory_base_url.rstrip("/")
    urls: list[str] = []
    seen: set[str] = set()

    async with httpx.AsyncClient(
        timeout=settings.request_timeout_s, follow_redirects=True
    ) as client:
        next_url: str | None = f"{base}/directory/{category}?page=1"
        pages = 0
        while next_url and pages < max_pages:
            html = await fetch(client, next_url)
            tree = HTMLParser(html)
            for node in tree.css("a.listing-link"):
                href = node.attributes.get("href")
                if not href:
                    continue
                absolute = urljoin(next_url, href)
                if absolute not in seen:
                    seen.add(absolute)
                    urls.append(absolute)
            pages += 1
            nxt = tree.css_first("a[rel=next]")
            href = nxt.attributes.get("href") if nxt else None
            next_url = urljoin(next_url, href) if href else None

    return urls


def discover_listings(payload: dict[str, Any]) -> list[str]:
    """Walk a category's pagination and return detail URLs."""
    category = payload["category"]
    max_pages = int(payload.get("max_pages", 3))
    urls = asyncio.run(_discover(category, max_pages))
    log.info("activity.discovered", category=category, urls=len(urls))
    return urls


def _extract(html: str, url: str, source: str) -> CompanyRecord | None:
    tree = HTMLParser(html)

    def text(selector: str) -> str | None:
        node = tree.css_first(selector)
        if node is None:
            return None
        for attr in ("content", "datetime"):
            if value := node.attributes.get(attr):
                return value.strip()
        href = node.attributes.get("href") or ""
        if href.startswith("mailto:"):
            return href[7:].strip()
        if href.startswith("tel:"):
            return href[4:].strip()
        if href.startswith(("http://", "https://")):
            return href.strip()
        return node.text(strip=True) or None

    name = text('[itemprop="name"]') or text("h1")
    if not name:
        return None

    categories = [n.text(strip=True) for n in tree.css('[itemprop="category"]')]
    employees = text('[itemprop="numberOfEmployees"]')
    founded = text('[itemprop="foundingDate"]')

    source_id = _source_id(url)
    return CompanyRecord(
        record_id=CompanyRecord.make_id(source, source_id),
        source=source,
        source_id=source_id,
        source_url=url,
        name=name,
        city=text('[itemprop="addressLocality"]'),
        region=text('[itemprop="addressRegion"]'),
        postal_code=text('[itemprop="postalCode"]'),
        phone=text('[itemprop="telephone"]'),
        email=text('[itemprop="email"]'),
        website=text('[itemprop="url"]'),
        categories=[c for c in categories if c],
        description=text('[itemprop="description"]'),
        employee_count=int(employees) if employees and employees.isdigit() else None,
        founded_year=int(founded) if founded and founded.isdigit() else None,
    )


async def _fetch_and_extract(urls: list[str], source: str) -> list[dict[str, Any]]:
    settings = get_settings()
    semaphore = asyncio.Semaphore(settings.fetch_concurrency)
    records: list[dict[str, Any]] = []

    async with httpx.AsyncClient(
        timeout=settings.request_timeout_s, follow_redirects=True
    ) as client:

        async def one(url: str) -> dict[str, Any] | None:
            async with semaphore:
                try:
                    html = await fetch(client, url)
                except PermanentError as exc:
                    # Folded into the result rather than raised: a 404 will fail
                    # identically on every retry and would burn the batch budget.
                    log.warning("activity.page_permanently_failed", url=url, error=str(exc))
                    return None
                record = _extract(html, url, source)
                return record.to_json() if record else None

        for result in await asyncio.gather(*(one(u) for u in urls), return_exceptions=True):
            if isinstance(result, BaseException):
                # Transient failures propagate so the host retries the activity.
                raise result
            if result is not None:
                records.append(result)

    return records


def fetch_and_extract(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Fetch a batch of detail pages and extract a record from each.

    Fetch and extract are one activity on purpose: passing raw HTML between
    activities would push megabytes of page source through the orchestration
    history, where it would be stored and replayed forever.
    """
    urls = list(payload["urls"])
    source = payload.get("source", "demo-directory")
    records = asyncio.run(_fetch_and_extract(urls, source))
    log.info("activity.extracted", requested=len(urls), extracted=len(records))
    return records


async def _enrich(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    settings = get_settings()
    url = f"{settings.enrichment_base_url.rstrip('/')}/v1/companies/lookup"
    # One in-flight request per distinct domain: many records share a parent
    # company, and the provider bills per lookup.
    cache: dict[str, dict[str, Any]] = {}

    async with httpx.AsyncClient(timeout=settings.request_timeout_s) as client:
        for record in records:
            website = record.get("website") or ""
            domain = urlsplit(website if "://" in website else f"//{website}").netloc
            domain = domain.lower().removeprefix("www.")
            if not domain:
                continue

            if domain not in cache:
                headers = {
                    "Authorization": f"Bearer {settings.enrichment_api_key}",
                    # Deterministic per record+schema, so a retry after a
                    # timeout cannot be billed twice.
                    "Idempotency-Key": f"{record['record_id']}:v1",
                }
                body = await fetch(client, url, headers=headers, params={"domain": domain})
                try:
                    cache[domain] = json.loads(body)
                except json.JSONDecodeError:
                    cache[domain] = {}

            found = cache[domain]
            if found.get("status") == "ok":
                record["industry"] = found.get("industry")
                record["revenue_usd"] = found.get("revenue_usd")
                if found.get("employee_count"):
                    record["employee_count"] = found["employee_count"]
                record["enriched"] = True

    return records


def enrich_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
    records = list(payload["records"])
    enriched = asyncio.run(_enrich(records))
    hits = sum(1 for r in enriched if r.get("enriched"))
    log.info("activity.enriched", records=len(records), enriched=hits)
    return enriched
