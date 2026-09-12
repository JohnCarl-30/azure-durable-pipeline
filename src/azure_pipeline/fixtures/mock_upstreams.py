"""Mock directory and enrichment API, so the pipeline runs with nothing external.

Both misbehave on purpose. The directory returns 429 under burst and 404s for
one listing; the enrichment API rate-limits, fails a fraction of calls with a
503, and honours Idempotency-Key. Without that, the retry policies and the
permanent/transient split are untested decoration.
"""

from __future__ import annotations

import random
import time
from collections import defaultdict
from typing import Any

from fastapi import FastAPI, Header, Response
from fastapi.responses import HTMLResponse, JSONResponse

directory = FastAPI(title="Mock Directory")
enrichment = FastAPI(title="Mock Enrichment API")

PAGE_SIZE = 4
FAILURE_RATE = 0.08
RATE_PER_SECOND = 25.0
BURST = 40

COMPANIES: list[dict[str, Any]] = [
    {
        "slug": "northwind-analytics",
        "name": "Northwind Analytics, Inc.",
        "category": "software",
        "city": "Austin",
        "region": "TX",
        "postal": "78701",
        "phone": "+15125550142",
        "email": "hello@northwindanalytics.com",
        "website": "https://northwindanalytics.com",
        "employees": "138",
        "founded": "2014",
        "tags": ["Analytics", "Data Platform"],
        "description": "Real-time customer data platform for mid-market retailers.",
    },
    {
        "slug": "harbor-point-labs",
        "name": "Harbor Point Labs",
        "category": "software",
        "city": "Boston",
        "region": "MA",
        "postal": "02210",
        "phone": "+16175550199",
        "email": "contact@harborpointlabs.io",
        "website": "https://harborpointlabs.io",
        "employees": "34",
        "founded": "2019",
        "tags": ["Computer Vision"],
        "description": "Computer-vision tooling for industrial inspection.",
    },
    {
        "slug": "atlas-robotics",
        "name": "Atlas Robotics",
        "category": "software",
        "city": "Seattle",
        "region": "WA",
        "postal": "98103",
        "phone": "+12065550164",
        "email": "hello@atlasrobotics.ai",
        "website": "https://atlasrobotics.ai",
        "employees": "410",
        "founded": "2016",
        "tags": ["Robotics", "Automation"],
        "description": "Warehouse automation and fleet-orchestration software.",
    },
    {
        "slug": "vantage-grid",
        "name": "Vantage Grid Energy",
        "category": "energy",
        "city": "Denver",
        "region": "CO",
        "postal": "80202",
        "phone": "+17205550133",
        "email": "projects@vantagegrid.com",
        "website": "https://vantagegrid.com",
        "employees": "47",
        "founded": "2020",
        "tags": ["Energy Storage"],
        "description": "Utility-scale battery storage project development.",
    },
    {
        "slug": "cascade-freight",
        "name": "Cascade Freight Systems",
        "category": "logistics",
        "city": "Portland",
        "region": "OR",
        "postal": "97210",
        "phone": "+15035550110",
        "email": "dispatch@cascadefreight.com",
        "website": "https://cascadefreight.com",
        "employees": "820",
        "founded": "1998",
        "tags": ["Freight", "Logistics"],
        "description": "Regional LTL trucking across the Pacific Northwest.",
    },
    {
        "slug": "quarry-lane-foods",
        "name": "Quarry Lane Foods",
        "category": "logistics",
        "city": "Columbus",
        "region": "OH",
        "postal": "43204",
        "phone": "+16145550155",
        "email": "orders@quarrylanefoods.com",
        "website": "https://quarrylanefoods.com",
        "employees": "150",
        "founded": "1987",
        "tags": ["Food", "Co-packing"],
        "description": "Co-packer for specialty sauces and shelf-stable condiments.",
    },
    # Returns 404: exercises the permanent-vs-transient split. A retry cannot
    # help, so the activity must fold it into the result rather than raise.
    {"slug": "gone-away", "name": "Gone Away Ltd", "category": "software", "missing": True},
]

BY_CATEGORY: dict[str, list[dict[str, Any]]] = defaultdict(list)
for _c in COMPANIES:
    BY_CATEGORY[_c["category"]].append(_c)
BY_SLUG = {c["slug"]: c for c in COMPANIES}

ENRICHMENT: dict[str, dict[str, Any]] = {
    "northwindanalytics.com": {
        "industry": "Software",
        "revenue_usd": 24_000_000,
        "employee_count": 138,
    },
    "harborpointlabs.io": {"industry": "Software", "revenue_usd": 6_500_000, "employee_count": 34},
    "atlasrobotics.ai": {"industry": "Robotics", "revenue_usd": 58_000_000, "employee_count": 410},
    "cascadefreight.com": {
        "industry": "Transportation",
        "revenue_usd": 145_000_000,
        "employee_count": 820,
    },
    "vantagegrid.com": {"industry": "Energy", "revenue_usd": 41_000_000, "employee_count": 47},
}

_hits: list[float] = []
_tokens = float(BURST)
_last_refill = time.monotonic()
_idempotency: dict[str, dict[str, Any]] = {}
_billable: dict[str, int] = defaultdict(int)


@directory.get("/directory/{category}", response_class=HTMLResponse)
async def listing(category: str, page: int = 1) -> HTMLResponse:
    now = time.monotonic()
    _hits[:] = [t for t in _hits if now - t < 1.0]
    if len(_hits) >= 60:
        return HTMLResponse("rate limited", status_code=429, headers={"Retry-After": "1"})
    _hits.append(now)

    companies = BY_CATEGORY.get(category, [])
    start = (page - 1) * PAGE_SIZE
    chunk = companies[start : start + PAGE_SIZE]
    if not chunk and page > 1:
        return HTMLResponse("<html><body><ul></ul></body></html>")

    rows = "\n".join(
        f'<li><a class="listing-link" href="/company/{c["slug"]}">{c["name"]}</a></li>'
        for c in chunk
    )
    nxt = (
        f'<a rel="next" href="/directory/{category}?page={page + 1}">Next</a>'
        if start + PAGE_SIZE < len(companies)
        else ""
    )
    return HTMLResponse(f"<html><body><ul>{rows}</ul>{nxt}</body></html>")


@directory.get("/company/{slug}", response_class=HTMLResponse)
async def company(slug: str) -> HTMLResponse:
    record = BY_SLUG.get(slug)
    if record is None or record.get("missing"):
        return HTMLResponse("<html><body><h1>Not found</h1></body></html>", status_code=404)

    tags = "".join(f'<span itemprop="category">{t}</span>' for t in record["tags"])
    return HTMLResponse(f"""<!doctype html>
<html><body>
<article itemscope itemtype="https://schema.org/Organization">
  <h1 itemprop="name">{record["name"]}</h1>
  <p itemprop="description">{record["description"]}</p>
  <div>{tags}</div>
  <span itemprop="addressLocality">{record["city"]}</span>
  <span itemprop="addressRegion">{record["region"]}</span>
  <span itemprop="postalCode">{record["postal"]}</span>
  <span itemprop="telephone">{record["phone"]}</span>
  <a itemprop="email" href="mailto:{record["email"]}">email</a>
  <a itemprop="url" href="{record["website"]}">site</a>
  <span itemprop="numberOfEmployees">{record["employees"]}</span>
  <span itemprop="foundingDate">{record["founded"]}</span>
</article>
</body></html>""")


@directory.get("/healthz")
async def directory_health() -> dict[str, str]:
    return {"status": "ok"}


@enrichment.get("/v1/companies/lookup")
async def lookup(
    domain: str | None = None,
    authorization: str | None = Header(default=None),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> Response:
    global _tokens, _last_refill

    if not authorization or not authorization.startswith("Bearer "):
        return JSONResponse({"error": "missing bearer token"}, status_code=401)

    # A replay of a request we already answered costs the caller nothing --
    # which is the entire point of the client sending a deterministic key.
    if idempotency_key and idempotency_key in _idempotency:
        return JSONResponse(_idempotency[idempotency_key], headers={"X-Idempotent-Replay": "true"})

    now = time.monotonic()
    _tokens = min(float(BURST), _tokens + (now - _last_refill) * RATE_PER_SECOND)
    _last_refill = now
    if _tokens < 1:
        return JSONResponse(
            {"error": "rate limited"}, status_code=429, headers={"Retry-After": "1"}
        )
    _tokens -= 1

    if random.random() < FAILURE_RATE:
        return JSONResponse({"error": "upstream unavailable"}, status_code=503)

    key = (domain or "").lower()
    _billable[key] += 1
    record = ENRICHMENT.get(key)
    payload = (
        {"status": "ok", "provider": "demo-enrichment", **record}
        if record
        else {"status": "not_found", "query": {"domain": domain}}
    )
    if idempotency_key:
        _idempotency[idempotency_key] = payload
    return JSONResponse(payload)


@enrichment.get("/v1/_stats")
async def stats() -> dict[str, Any]:
    return {
        "billable_calls": dict(_billable),
        "total_billable_calls": sum(_billable.values()),
        "idempotency_keys_seen": len(_idempotency),
    }


@enrichment.get("/healthz")
async def enrichment_health() -> dict[str, str]:
    return {"status": "ok"}
