# azure-durable-pipeline

A directory enrichment pipeline on **Azure Durable Functions**, with Terraform
infrastructure and identity-based access throughout — verified locally against
the real Functions host, Azurite and Terraform. **No Azure subscription, no
credentials, no spend.**

```bash
make install    # venv, python deps, project-local terraform + Functions tooling
make test       # 60+ tests
make run        # end-to-end on the real Functions host
```

This is deliberately the same workload as
[directory-pipeline](https://github.com/JohnCarl-30/directory-pipeline), which
runs on Temporal. Building it twice is the point: the comparison below is the
most useful thing in this repo.

---

## Durable Functions vs Temporal, having written both

Both are durable-execution engines: an orchestration survives process restarts
by **replaying its history** rather than holding state in memory. That single
shared idea produces near-identical constraints and very different ergonomics.

### The same, and for the same reason

| Constraint | Temporal | Durable Functions |
|---|---|---|
| Orchestration code must be deterministic | workflow | orchestrator |
| No I/O in orchestration code | activities | activities |
| No wall-clock time | `workflow.now()` | `context.current_utc_datetime` |
| No random / uuid | `workflow.uuid4()` | `context.new_guid()` |
| Bound history growth | `continue_as_new` | `context.continue_as_new` |
| Isolate failure blast radius | child workflow | sub-orchestration |
| Wait for a human, at zero cost | signal | `wait_for_external_event` |

If you understand one, you understand the other. The replay model is the
concept; the SDKs are an implementation detail.

### Where they genuinely differ

**Retry policy scope.** Temporal attaches a retry policy — including a list of
non-retryable error *types* — to the activity invocation, and the server
enforces it. Durable Functions has `RetryOptions` per call but **no
non-retryable type list at all**: every exception retries identically.

That is not a small difference. It means "do not retry a 404" has to be
expressed by the activity itself, which catches the permanent error and folds
it into its result rather than raising. `activities/errors.py` is that
convention, and without it a single missing page burns six attempts and up to
thirty minutes of backoff re-fetching something that cannot exist.

**Queryable state.** Temporal has first-class query handlers — arbitrary
synchronous reads against a running workflow. Durable Functions has
`set_custom_status`, a single JSON blob the orchestrator overwrites. Simpler,
and markedly less capable: one shape for all callers, no parameters, and you
must remember to set it.

**Testing.** Durable Functions wins clearly. An orchestrator is a plain
generator, so you drive it yourself with a fake context and assert on the exact
sequence of scheduled operations — no test server, no time-skipping harness,
sub-millisecond. `tests/fake_context.py` is ~130 lines and the orchestrator
tests run in 0.13s. Temporal's time-skipping test environment is excellent but
it downloads and runs a real server.

**Operational surface.** Temporal is a system you run (or pay Temporal Cloud
for) — a server, a database, workers, a UI. Durable Functions has no server at
all: orchestration history lives in the Storage account, and scale-out is the
Functions host's problem. Much less to operate, and correspondingly less
control — the storage provider, partition count and concurrency limits are the
only knobs.

**Local development.** Temporal is `docker compose up`. Durable Functions needs
the Functions Core Tools, and the first `func start` silently downloads a
~200MB extension bundle, printing its banner and then nothing. It looks
exactly like a hang. `scripts/run_local.py` passes `--verbose` for that reason.

### Which I would choose

Durable Functions when the workload already lives in Azure and the
orchestrations are modest — the operational saving is real and there is no
cluster to run. Temporal when orchestrations are long-lived and complex, when
retry semantics need to differ per error type without hand-rolling it, or when
the workload should not be tied to one cloud.

---

## How a crawl starts

Two ways in, and only one of them is for production:

```
Service Bus queue ──▶ ingest_requests ──▶ crawl_orchestrator
POST /api/crawl   ──▶ start_crawl     ──▶ crawl_orchestrator
```

The HTTP route is for driving it by hand. Real work arrives as a queued
message, and that path has three decisions in it, none of them defaults:

**Delivery is at least once.** The broker redelivers on any consumer crash,
lock expiry or rebalance, so the same message *will* arrive twice. The
orchestration instance id is derived from the message id rather than generated,
so a redelivery addresses the instance that is already running instead of
starting a second crawl over the same categories. A message published without
an id falls back to a hash of the body — generating one per delivery would
defeat the deduplication entirely.

**A malformed message is completed, not retried.** It would fail identically
five times and then dead-letter, spending five delivery attempts and five lock
durations to learn nothing. This is the same permanent/transient split the
activities use: a parse failure is permanent.

**A transient failure raises.** If the Durable client is unavailable, the
message goes back on the queue and the broker redelivers — which is exactly
when redelivery is the right answer. Collapsing both failure kinds into one
`except` loses the distinction, and the distinction is the whole point.

All of that lives in `ingest/message.py` rather than in the trigger, so it is
testable with a fake client and a bytes body — no broker, no host. The binding
itself is three lines.

## Security posture

The part of the Terraform worth reading: **there are no connection strings in
app settings.**

```hcl
"AzureWebJobsStorage__accountName" = azurerm_storage_account.main.name
"AzureWebJobsStorage__credential"  = "managedidentity"
```

The Function App authenticates to Storage and Service Bus with a
system-assigned managed identity. The only actual secret — a third-party API
key — lives in Key Vault and is referenced, never copied:

```hcl
"ENRICHMENT_API_KEY" = "@Microsoft.KeyVault(SecretUri=${...versionless_id})"
```

A leaked app-settings dump therefore yields nothing. Role assignments are the
narrowest that work — `Storage Blob Data Owner`, not `Contributor` — and
`tests/test_infra.py` asserts these properties directly, so an edit that
reintroduces a connection string fails CI rather than a review.

## Design notes worth the detour

**Table Storage partition keys are the whole design.** Transactions are atomic
only *within* a partition, batches cap at 100 entities, and partitions are the
unit of scale distribution. Partitioning by `source` gives cheap per-source
queries and one hot partition per source; partitioning by `record_id` gives
perfect write distribution and no batching at all. This uses `source` because
the access pattern is per-source reporting at small volume — a decision to
revisit at scale, which is why `adapters/storage.py` names it rather than
burying it.

**Deterministic child instance ids.** Sub-orchestrations are addressed as
`{parent}:batch:{index}`, never a new guid. A guid would mint a fresh child on
every replay — the classic Durable Functions duplication bug —
and `test_batches_are_given_deterministic_child_instance_ids` pins it.

**Fetch and extract are one activity.** Passing raw HTML between activities
would push megabytes of page source into the orchestration history, where it is
stored and replayed for the life of the instance.

**Orchestrator logging is replay-guarded.** A plain `log.info` in an
orchestrator fires again on every replay, so an instance that resumes ten times
logs the same line ten times. Every call routes through a helper that checks
`context.is_replaying`.

## Testing

60+ tests. The orchestrator tests need no emulator, no host and no network —
they drive the generators directly and assert the scheduled call sequence,
which is what replay determinism actually means:

```
tests/test_orchestrators.py   generators driven by a fake context (0.13s)
tests/test_ingest_message.py  the queue path: dedup, malformed, transient
tests/test_activities.py      HTTP mocked; permanent vs transient classification
tests/test_storage.py         real Azurite: batch limits, partition rules
tests/test_infra.py           terraform validate + security properties asserted
```

Storage and infra tests self-skip when Azurite or the terraform binary is
absent, so a bare checkout still runs the suite.

## The measured run

`make run` starts Azurite, the mock upstreams and the real Functions host, then
drives a genuine orchestration:

```
3. Start the orchestration
   POST /api/crawl {"categories": ["software","logistics","energy"], "batch_size": 3}
   instance: 4aee2649768042f7a3477536cc623cf9

4. Orchestration progress
   Pending
   Completed    {'stage': 'done', 'indexed': 6}

5. Result
   urls_discovered    7
   batches            3
   extracted          6      <- 7 discovered, 1 is a deliberate 404
   enriched           5
   indexed            6

6. Verify against storage
   table rows for 'demo-directory': 6
     Northwind Analytics, Inc.   Austin     Software
     Atlas Robotics              Seattle    Robotics
     Cascade Freight Systems     Portland   Transportation
     ...
   billable enrichment calls: 6 (idempotency keys seen: 6)
   blob artifacts: 1
   PASS
```

The 7→6 gap is the point: one fixture listing returns 404, and the activity
classifies it as permanent and folds it into the result. Had it raised, the
host would have retried six times with growing backoff — up to thirty minutes
re-fetching a page that cannot exist.

## Three things that only failed on the real host

Unit tests passed throughout. These surfaced only when the actual Functions
host ran, and all three are documented in the code where they bite:

**`extendedSessionsEnabled` is .NET-only.** Setting it in `host.json` is
accepted by every schema check and then refuses to start the Python worker:
*"only supported when using the in-process or isolated .NET worker."* Removed.

**Worker/interpreter version mismatch.** Core Tools selects its bundled worker
by the Python version it *detects*, and each worker ships a gRPC native
extension built for that exact minor version. Pointing
`languageWorkers__python__defaultExecutablePath` at a 3.12 venv while the
system Python is 3.14 loads the 3.14 worker and runs it under 3.12, which fails
with a bare `ImportError: cannot import name 'cygrpc'` and no hint that a
version mismatch is the cause. The fix is to put the venv first on `PATH` so
detection and execution agree.

**First `func start` looks like a hang.** It prints its banner and then goes
silent for several minutes while downloading a ~200MB extension bundle. Only
`--verbose` reveals *"Downloading extension bundles..."*, so the runner passes
it. Subsequent starts take seconds.

A fourth was caught by the run rather than the tests: `write_run_artifact` was
defined and registered but never called, so `blob artifacts` read 0. An
orchestration that completes should always leave a durable artifact; it is now
written before the instance reports done, with a test asserting it.

## Verified locally, not deployed

To be explicit about what has and has not been exercised:

- **Verified:** Durable orchestrations on the real Functions host; Blob, Queue
  and Table against Azurite; `terraform validate` and `fmt`; every security
  property asserted from the configuration.
- **Not verified:** an actual Azure deployment. `terraform plan` requires a
  subscription, and applying costs money. The configuration is validated and
  statically analysed, not applied.

The AWS equivalent of this stack (ECS/Fargate, SQS, Step Functions) is the
remaining gap against the original brief and is not covered here.

## Layout

```
function_app.py            binding layer: every trigger the host discovers
src/azure_pipeline/
├── config.py              env-driven; Azurite's published dev key is inlined
├── domain/models.py       JSON-round-trippable contracts stored in history
├── orchestration/         orchestrators + per-failure-shape retry policies
├── activities/            I/O, plus the permanent/transient convention
├── adapters/              storage (Table + Blob) and classified HTTP
└── fixtures/              mock directory and enrichment API
infra/                     Terraform: Flex Consumption, Service Bus, Key Vault
tests/fake_context.py      the orchestrator test harness
```
