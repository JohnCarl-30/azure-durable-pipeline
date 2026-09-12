"""Configuration.

Azure Functions injects configuration as flat environment variables through app
settings, so everything is read from the environment with no file layered
underneath. `AzureWebJobsStorage` is the one name that is not ours to choose --
the Functions host requires it, and Durable Functions uses it as the backing
store for orchestration history.

The local values below point at Azurite's well-known development account. That
key is published by Microsoft and identical on every machine, so it is not a
secret and is safe to commit; every other connection string is read from the
environment and never has a default.
"""

from __future__ import annotations

import os
from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Microsoft's published Azurite development credentials. Deliberately inlined:
# a reader needs to know these are *not* a leaked secret.
AZURITE_CONNECTION_STRING = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
    "QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;"
    "TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"
)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore", env_file=None)

    # Set by the Functions host; Durable Functions stores history here.
    azure_webjobs_storage: str = Field(
        default=AZURITE_CONNECTION_STRING, alias="AzureWebJobsStorage"
    )
    service_bus_connection: str = Field(default="", alias="ServiceBusConnection")

    # --- Containers / tables ----------------------------------------------
    artifacts_container: str = "artifacts"
    checkpoints_table: str = "checkpoints"
    ingest_queue: str = "ingest-requests"

    # --- Upstreams ---------------------------------------------------------
    directory_base_url: str = "http://127.0.0.1:8081"
    enrichment_base_url: str = "http://127.0.0.1:8082"
    enrichment_api_key: str = "demo-key"

    # --- Throughput --------------------------------------------------------
    batch_size: int = 10
    max_pages: int = 3
    fetch_concurrency: int = 8
    request_timeout_s: float = 20.0

    # Above this many URLs a run hands the remainder to a fresh orchestration,
    # to bound replay history growth.
    max_urls_per_run: int = 500

    @property
    def running_locally(self) -> bool:
        """True under the local Functions host; false in a real Function App."""
        return os.environ.get("AZURE_FUNCTIONS_ENVIRONMENT", "Development") == "Development"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
