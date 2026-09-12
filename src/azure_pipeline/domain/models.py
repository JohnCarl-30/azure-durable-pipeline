"""Contracts that cross orchestrator and activity boundaries.

Durable Functions serializes every activity input and output to JSON and stores
it in the orchestration history. That has two consequences these models are
shaped around:

**Everything must round-trip through JSON.** No datetimes without an explicit
encoder, no sets, no tuples. Pydantic's `mode="json"` dump is used everywhere
rather than `model_dump()`, because the latter leaves `datetime` objects in
place and the host's serializer then fails at the boundary.

**History is replayed forever.** An orchestration that ran last week is replayed
from the same stored payloads when it resumes, so these are versioned add-only
contracts exactly like Temporal's -- a removed field breaks an in-flight run.
"""

from __future__ import annotations

import hashlib
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

SCHEMA_VERSION = 1


class Stage(StrEnum):
    PENDING = "pending"
    DISCOVERING = "discovering"
    FETCHING = "fetching"
    ENRICHING = "enriching"
    INDEXING = "indexing"
    DONE = "done"
    FAILED = "failed"


class CrawlRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str = "demo-directory"
    categories: list[str] = Field(default_factory=lambda: ["software"])
    max_pages: int = 3
    batch_size: int = 10
    enrich: bool = True

    # Continuation state. A run that exceeds the per-run URL budget hands the
    # remainder to a fresh orchestration; without these the work and the counts
    # would be lost at the boundary.
    pending_urls: list[str] = Field(default_factory=list)
    carried_totals: dict[str, int] = Field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class CompanyRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    record_id: str
    source: str
    source_id: str
    source_url: str
    name: str
    city: str | None = None
    region: str | None = None
    postal_code: str | None = None
    phone: str | None = None
    email: str | None = None
    website: str | None = None
    categories: list[str] = Field(default_factory=list)
    description: str | None = None
    employee_count: int | None = None
    founded_year: int | None = None
    industry: str | None = None
    revenue_usd: int | None = None
    enriched: bool = False
    schema_version: int = SCHEMA_VERSION

    @staticmethod
    def make_id(source: str, source_id: str) -> str:
        """Deterministic: a re-run overwrites rather than duplicating."""
        return hashlib.sha1(f"{source}:{source_id}".encode()).hexdigest()

    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class BatchRequest(BaseModel):
    """Input to the per-batch sub-orchestration."""

    model_config = ConfigDict(extra="forbid")

    urls: list[str]
    source: str
    enrich: bool = True
    batch_index: int = 0

    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class BatchResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    batch_index: int = 0
    extracted: int = 0
    enriched: int = 0
    indexed: int = 0
    failures: list[str] = Field(default_factory=list)

    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class CrawlResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    instance_id: str = ""
    stage: Stage = Stage.PENDING
    urls_discovered: int = 0
    batches: int = 0
    extracted: int = 0
    enriched: int = 0
    indexed: int = 0
    continued_as_new: bool = False
    failures: list[str] = Field(default_factory=list)

    def merge_batch(self, batch: BatchResult) -> None:
        self.extracted += batch.extracted
        self.enriched += batch.enriched
        self.indexed += batch.indexed
        self.failures.extend(batch.failures)

    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode="json")
