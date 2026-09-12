from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from azure_pipeline.config import Settings  # noqa: E402


@pytest.fixture
def settings() -> Settings:
    return Settings(
        directory_base_url="http://directory.test",
        enrichment_base_url="http://enrich.test",
        batch_size=4,
        max_pages=2,
    )
