"""Function App entry point (Python v2 programming model).

Everything the Functions host can see must be registered on `app` in this one
file -- the v2 model discovers triggers by import, not by a `function.json` per
folder. The implementations live in `src/azure_pipeline/` so they stay testable
without the host; this file is the binding layer and deliberately contains no
logic of its own.

`DFApp` rather than `FunctionApp`: it adds the Durable-specific decorators
(`orchestration_trigger`, `activity_trigger`, `durable_client_input`).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import azure.durable_functions as df
import azure.functions as func

sys.path.insert(0, str(Path(__file__).parent / "src"))

from azure_pipeline.activities import functions as activity_impl  # noqa: E402
from azure_pipeline.activities import persistence as persist_impl  # noqa: E402
from azure_pipeline.domain.models import CrawlRequest  # noqa: E402
from azure_pipeline.observability import configure_logging, get_logger  # noqa: E402
from azure_pipeline.orchestration import orchestrators  # noqa: E402

configure_logging(json_output=False)
log = get_logger(__name__)

# Anonymous auth is correct for local development only. The Terraform in
# infra/ fronts the deployed app with a function key and restricts inbound
# access; see infra/README for what changes on deploy.
app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)


# --- HTTP starters -----------------------------------------------------------


@app.route(route="crawl", methods=["POST"])
@app.durable_client_input(client_name="client")
async def start_crawl(req: func.HttpRequest, client) -> func.HttpResponse:
    """Kick off a crawl and return the management URLs.

    Returns 202 with a status-query link rather than blocking: a crawl runs for
    minutes, and `create_check_status_response` hands back the standard set of
    URLs for polling, terminating and raising events on the instance.
    """
    try:
        body = req.get_json() if req.get_body() else {}
    except ValueError:
        return func.HttpResponse("body must be JSON", status_code=400)

    request = CrawlRequest.model_validate(body or {})
    # A caller-supplied instance id makes the start idempotent: POSTing the
    # same crawl twice attaches to the running one instead of starting a second.
    instance_id = req.params.get("instance_id") or None
    started = await client.start_new("crawl_orchestrator", instance_id, request.to_json())
    log.info("http.crawl_started", instance_id=started)
    return client.create_check_status_response(req, started)


@app.route(route="status/{instance_id}", methods=["GET"])
@app.durable_client_input(client_name="client")
async def get_status(req: func.HttpRequest, client) -> func.HttpResponse:
    """Runtime status plus the orchestrator's custom status."""
    instance_id = req.route_params["instance_id"]
    state = await client.get_status(instance_id, show_history=False)
    if state is None or state.runtime_status is None:
        return func.HttpResponse("unknown instance", status_code=404)

    return func.HttpResponse(
        json.dumps(
            {
                "instance_id": instance_id,
                "runtime_status": str(state.runtime_status.name),
                "custom_status": state.custom_status,
                "output": state.output,
                "created_at": str(state.created_time),
                "updated_at": str(state.last_updated_time),
            },
            default=str,
        ),
        mimetype="application/json",
    )


@app.route(route="approve/{instance_id}", methods=["POST"])
@app.durable_client_input(client_name="client")
async def approve(req: func.HttpRequest, client) -> func.HttpResponse:
    """Raise the external event the approval orchestration is waiting on."""
    instance_id = req.route_params["instance_id"]
    approved = req.params.get("approved", "true").lower() != "false"
    await client.raise_event(instance_id, "ApprovalReceived", approved)
    return func.HttpResponse(
        json.dumps({"instance_id": instance_id, "approved": approved}),
        mimetype="application/json",
        status_code=202,
    )


@app.route(route="terminate/{instance_id}", methods=["POST"])
@app.durable_client_input(client_name="client")
async def terminate(req: func.HttpRequest, client) -> func.HttpResponse:
    instance_id = req.route_params["instance_id"]
    await client.terminate(instance_id, req.params.get("reason", "terminated by operator"))
    return func.HttpResponse(status_code=202)


# --- Orchestrators -----------------------------------------------------------


@app.orchestration_trigger(context_name="context")
def crawl_orchestrator(context: df.DurableOrchestrationContext):
    return orchestrators.crawl_orchestrator(context)


@app.orchestration_trigger(context_name="context")
def batch_orchestrator(context: df.DurableOrchestrationContext):
    return orchestrators.batch_orchestrator(context)


@app.orchestration_trigger(context_name="context")
def approval_orchestrator(context: df.DurableOrchestrationContext):
    return orchestrators.approval_orchestrator(context)


# --- Activities --------------------------------------------------------------


@app.activity_trigger(input_name="payload")
def discover_listings(payload: dict) -> list[str]:
    return activity_impl.discover_listings(payload)


@app.activity_trigger(input_name="payload")
def fetch_and_extract(payload: dict) -> list[dict]:
    return activity_impl.fetch_and_extract(payload)


@app.activity_trigger(input_name="payload")
def enrich_records(payload: dict) -> list[dict]:
    return activity_impl.enrich_records(payload)


@app.activity_trigger(input_name="payload")
def persist_records(payload: dict) -> int:
    return persist_impl.persist_records(payload)


@app.activity_trigger(input_name="payload")
def write_run_artifact(payload: dict) -> str:
    return persist_impl.write_run_artifact(payload)
