"""Activity behaviour, with the network mocked.

The property that matters most: a **permanent** failure must not reach the
orchestrator. Durable Functions retries every exception identically, so a 404
that escapes an activity burns six attempts and up to thirty minutes of backoff
re-fetching a page that will never exist.
"""

from __future__ import annotations

import httpx
import pytest
import respx

from azure_pipeline.activities import functions as activities
from azure_pipeline.activities.errors import PermanentError, TransientError, classify_status
from azure_pipeline.adapters.http import fetch

DETAIL_HTML = """
<html><body><article itemscope>
  <h1 itemprop="name">Northwind Analytics, Inc.</h1>
  <p itemprop="description">Customer data platform.</p>
  <span itemprop="category">Analytics</span>
  <span itemprop="addressLocality">Austin</span>
  <span itemprop="addressRegion">TX</span>
  <span itemprop="postalCode">78701</span>
  <span itemprop="telephone">+15125550142</span>
  <a itemprop="email" href="mailto:hello@northwind.com">email</a>
  <a itemprop="url" href="https://northwind.com">site</a>
  <span itemprop="numberOfEmployees">138</span>
  <span itemprop="foundingDate">2014</span>
</article></body></html>
"""

INDEX_HTML = """
<html><body>
  <a class="listing-link" href="/company/a">A</a>
  <a class="listing-link" href="/company/b">B</a>
  <a rel="next" href="/directory/software?page=2">Next</a>
</body></html>
"""
LAST_PAGE_HTML = '<html><body><a class="listing-link" href="/company/c">C</a></body></html>'


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (200, None),
        (204, None),
        (408, TransientError),
        (429, TransientError),
        (500, TransientError),
        (503, TransientError),
        (400, PermanentError),
        (404, PermanentError),
        (403, PermanentError),
    ],
)
def test_status_classification(status, expected):
    assert classify_status(status) is expected


@respx.mock
async def test_fetch_raises_transient_for_a_retryable_status():
    respx.get("http://t.test/x").mock(return_value=httpx.Response(503))
    async with httpx.AsyncClient() as client:
        with pytest.raises(TransientError):
            await fetch(client, "http://t.test/x")


@respx.mock
async def test_fetch_raises_permanent_for_a_404():
    respx.get("http://t.test/gone").mock(return_value=httpx.Response(404))
    async with httpx.AsyncClient() as client:
        with pytest.raises(PermanentError):
            await fetch(client, "http://t.test/gone")


@respx.mock
async def test_fetch_treats_a_connection_error_as_transient():
    respx.get("http://t.test/x").mock(side_effect=httpx.ConnectError("boom"))
    async with httpx.AsyncClient() as client:
        with pytest.raises(TransientError):
            await fetch(client, "http://t.test/x")


@respx.mock
def test_discover_walks_pagination(monkeypatch):
    monkeypatch.setenv("DIRECTORY_BASE_URL", "http://directory.test")
    from azure_pipeline import config

    config.get_settings.cache_clear()

    respx.get("http://directory.test/directory/software", params={"page": "1"}).mock(
        return_value=httpx.Response(200, text=INDEX_HTML)
    )
    respx.get("http://directory.test/directory/software", params={"page": "2"}).mock(
        return_value=httpx.Response(200, text=LAST_PAGE_HTML)
    )

    urls = activities.discover_listings({"category": "software", "max_pages": 5})
    assert urls == [
        "http://directory.test/company/a",
        "http://directory.test/company/b",
        "http://directory.test/company/c",
    ]
    config.get_settings.cache_clear()


@respx.mock
def test_max_pages_bounds_the_crawl(monkeypatch):
    monkeypatch.setenv("DIRECTORY_BASE_URL", "http://directory.test")
    from azure_pipeline import config

    config.get_settings.cache_clear()
    respx.get("http://directory.test/directory/software").mock(
        return_value=httpx.Response(200, text=INDEX_HTML)
    )

    urls = activities.discover_listings({"category": "software", "max_pages": 1})
    assert len(urls) == 2
    config.get_settings.cache_clear()


@respx.mock
def test_extraction_reads_microdata():
    respx.get("http://d.test/company/nw").mock(return_value=httpx.Response(200, text=DETAIL_HTML))
    records = activities.fetch_and_extract({"urls": ["http://d.test/company/nw"], "source": "demo"})

    assert len(records) == 1
    record = records[0]
    assert record["name"] == "Northwind Analytics, Inc."
    assert record["city"] == "Austin"
    assert record["phone"] == "+15125550142"
    assert record["email"] == "hello@northwind.com"
    assert record["website"] == "https://northwind.com"
    assert record["employee_count"] == 138
    assert record["founded_year"] == 2014
    assert record["categories"] == ["Analytics"]


@respx.mock
def test_a_permanently_failing_page_is_skipped_not_raised():
    """The central claim: a 404 must not cost the batch its retry budget."""
    respx.get("http://d.test/company/ok").mock(return_value=httpx.Response(200, text=DETAIL_HTML))
    respx.get("http://d.test/company/gone").mock(return_value=httpx.Response(404))

    records = activities.fetch_and_extract(
        {"urls": ["http://d.test/company/ok", "http://d.test/company/gone"], "source": "demo"}
    )
    assert len(records) == 1


@respx.mock
def test_a_transient_failure_propagates_so_the_host_retries():
    respx.get("http://d.test/company/x").mock(return_value=httpx.Response(503))
    with pytest.raises(TransientError):
        activities.fetch_and_extract({"urls": ["http://d.test/company/x"], "source": "demo"})


@respx.mock
def test_record_ids_are_deterministic():
    respx.get("http://d.test/company/nw").mock(return_value=httpx.Response(200, text=DETAIL_HTML))
    first = activities.fetch_and_extract({"urls": ["http://d.test/company/nw"], "source": "demo"})
    second = activities.fetch_and_extract({"urls": ["http://d.test/company/nw"], "source": "demo"})
    assert first[0]["record_id"] == second[0]["record_id"]


@respx.mock
def test_enrichment_sends_a_deterministic_idempotency_key(monkeypatch):
    """A fresh key per attempt defeats the entire mechanism."""
    monkeypatch.setenv("ENRICHMENT_BASE_URL", "http://enrich.test")
    from azure_pipeline import config

    config.get_settings.cache_clear()

    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers.get("Idempotency-Key", ""))
        return httpx.Response(
            200, json={"status": "ok", "industry": "Software", "revenue_usd": 1_000_000}
        )

    respx.get("http://enrich.test/v1/companies/lookup").mock(side_effect=handler)

    record = {
        "record_id": "abc123",
        "source": "demo",
        "source_id": "c1",
        "source_url": "http://d/1",
        "name": "X",
        "website": "https://x.example.com",
        "categories": [],
        "enriched": False,
        "schema_version": 1,
    }
    activities.enrich_records({"records": [dict(record)]})
    activities.enrich_records({"records": [dict(record)]})

    assert seen == ["abc123:v1", "abc123:v1"]
    config.get_settings.cache_clear()


@respx.mock
def test_one_lookup_per_domain_not_per_record(monkeypatch):
    """Records sharing a parent company must not each be billed."""
    monkeypatch.setenv("ENRICHMENT_BASE_URL", "http://enrich.test")
    from azure_pipeline import config

    config.get_settings.cache_clear()
    route = respx.get("http://enrich.test/v1/companies/lookup").mock(
        return_value=httpx.Response(200, json={"status": "ok", "industry": "Software"})
    )

    records = [
        {
            "record_id": f"r{i}",
            "source": "demo",
            "source_id": f"c{i}",
            "source_url": f"http://d/{i}",
            "name": f"C{i}",
            "website": "https://shared.example.com",
            "categories": [],
            "enriched": False,
            "schema_version": 1,
        }
        for i in range(5)
    ]
    enriched = activities.enrich_records({"records": records})

    assert route.call_count == 1
    assert all(r["enriched"] for r in enriched)
    config.get_settings.cache_clear()


@respx.mock
def test_a_not_found_lookup_leaves_the_record_unenriched(monkeypatch):
    monkeypatch.setenv("ENRICHMENT_BASE_URL", "http://enrich.test")
    from azure_pipeline import config

    config.get_settings.cache_clear()
    respx.get("http://enrich.test/v1/companies/lookup").mock(
        return_value=httpx.Response(200, json={"status": "not_found"})
    )

    records = activities.enrich_records(
        {
            "records": [
                {
                    "record_id": "r1",
                    "source": "demo",
                    "source_id": "c1",
                    "source_url": "http://d/1",
                    "name": "X",
                    "website": "https://unknown.example.com",
                    "categories": [],
                    "enriched": False,
                    "schema_version": 1,
                }
            ]
        }
    )
    assert records[0]["enriched"] is False
    config.get_settings.cache_clear()
