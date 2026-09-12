#!/usr/bin/env python3
"""End-to-end run against the real Azure Functions host.

Starts Azurite, the mock upstreams and `func start`, then drives a genuine
Durable Functions orchestration: HTTP starter, orchestrator, sub-orchestrations,
activities, Table Storage writes.

Nothing is mocked except the two upstream websites, and nothing touches Azure.

    python scripts/run_local.py
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import httpx  # noqa: E402
import uvicorn  # noqa: E402

from azure_pipeline.adapters.storage import ArtifactStore, RecordStore  # noqa: E402
from azure_pipeline.config import AZURITE_CONNECTION_STRING  # noqa: E402
from azure_pipeline.fixtures.mock_upstreams import directory, enrichment  # noqa: E402

BOLD, DIM, GREEN, RED, YELLOW, RESET = (
    "\033[1m",
    "\033[2m",
    "\033[32m",
    "\033[31m",
    "\033[33m",
    "\033[0m",
)


def banner(text: str) -> None:
    print(f"\n{BOLD}{text}{RESET}\n" + "-" * len(text))


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_for(url: str, timeout: float, label: str) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            httpx.get(url, timeout=1.5)
            return True
        except httpx.HTTPError:
            time.sleep(0.4)
    print(f"  {RED}{label} did not come up within {timeout:.0f}s{RESET}")
    return False


class BackgroundApp:
    """A mock upstream on a daemon thread."""

    def __init__(self, app, port: int) -> None:
        self.port = port
        self.server = uvicorn.Server(
            uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
        )
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    def start(self) -> None:
        self.thread.start()
        wait_for(f"http://127.0.0.1:{self.port}/healthz", 20, f"mock on :{self.port}")

    def stop(self) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=5)


class Process:
    """A child process whose output is captured to a file."""

    def __init__(self, name: str, args: list[str], env: dict[str, str], log: Path) -> None:
        self.name = name
        self.args = args
        self.env = env
        self.log_path = log
        self.proc: subprocess.Popen | None = None

    def start(self) -> None:
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.log_path.open("w")
        self.proc = subprocess.Popen(
            self.args,
            cwd=ROOT,
            env={**os.environ, **self.env},
            stdout=handle,
            stderr=subprocess.STDOUT,
            # Own process group, so stopping the host also stops its worker.
            preexec_fn=os.setsid if os.name != "nt" else None,
        )

    def stop(self) -> None:
        if self.proc is None:
            return
        try:
            os.killpg(os.getpgid(self.proc.pid), signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            self.proc.terminate()
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()

    def tail(self, lines: int = 25) -> str:
        if not self.log_path.exists():
            return "(no output)"
        return "\n".join(self.log_path.read_text().splitlines()[-lines:])


def main(args: argparse.Namespace) -> int:
    logs = ROOT / ".logs"
    if logs.exists():
        shutil.rmtree(logs)
    logs.mkdir(parents=True, exist_ok=True)

    func_bin = ROOT / "node_modules" / ".bin" / "func"
    azurite_bin = ROOT / "node_modules" / ".bin" / "azurite"
    if not func_bin.exists():
        print(f"{RED}Functions Core Tools missing. Run: npm install{RESET}")
        return 2

    directory_port, enrichment_port = free_port(), free_port()
    func_port = args.port
    processes: list[Process] = []
    mocks: list[BackgroundApp] = []

    try:
        banner("1. Emulator and upstreams")
        azurite_running = False
        try:
            httpx.get("http://127.0.0.1:10002/devstoreaccount1", timeout=1.5)
            azurite_running = True
        except httpx.HTTPError:
            pass

        if azurite_running:
            print(f"  azurite            {GREEN}already running{RESET}")
        else:
            azurite = Process(
                "azurite",
                [
                    str(azurite_bin),
                    "--silent",
                    "--location",
                    str(ROOT / ".azurite"),
                    "--blobHost",
                    "127.0.0.1",
                    "--queueHost",
                    "127.0.0.1",
                    "--tableHost",
                    "127.0.0.1",
                ],
                {},
                logs / "azurite.log",
            )
            azurite.start()
            processes.append(azurite)
            if not wait_for("http://127.0.0.1:10002/devstoreaccount1", 30, "azurite"):
                return 1
            print(f"  azurite            {GREEN}started{RESET} (blob/queue/table on 10000-10002)")

        for app, port, label in (
            (directory, directory_port, "mock directory"),
            (enrichment, enrichment_port, "mock enrichment"),
        ):
            mock = BackgroundApp(app, port)
            mock.start()
            mocks.append(mock)
            print(f"  {label:<18} http://127.0.0.1:{port}")

        banner("2. Azure Functions host")
        host = Process(
            "func",
            # `--verbose` matters here: without it the host prints its banner
            # and then goes silent while it downloads the extension bundle
            # (~200MB on first run), which is indistinguishable from a hang.
            [str(func_bin), "start", "--port", str(func_port), "--verbose"],
            {
                "AzureWebJobsStorage": "UseDevelopmentStorage=true",
                "AzureWebJobsFeatureFlags": "EnableWorkerIndexing",
                "FUNCTIONS_WORKER_RUNTIME": "python",
                "DIRECTORY_BASE_URL": f"http://127.0.0.1:{directory_port}",
                "ENRICHMENT_BASE_URL": f"http://127.0.0.1:{enrichment_port}",
                "ENRICHMENT_API_KEY": "demo-key",
                # Put the venv first on PATH rather than setting
                # languageWorkers__python__defaultExecutablePath.
                #
                # Core Tools selects its bundled worker by the Python version
                # it *detects*, and each worker ships a gRPC native extension
                # built for exactly that minor version. Overriding only the
                # executable path leaves detection on the system interpreter,
                # so a 3.14 host loads its 3.14 worker and then runs it under
                # 3.12 -- which fails with a bare
                # `ImportError: cannot import name 'cygrpc'` and no hint that
                # a version mismatch is the cause.
                # Letting detection and execution agree avoids the whole class.
                "PATH": f"{ROOT / '.venv' / 'bin'}:{os.environ.get('PATH', '')}",
            },
            logs / "func.log",
        )
        host.start()
        processes.append(host)
        print(f"  starting on :{func_port}")
        print(
            f"  {DIM}first run downloads the extension bundle (~200MB); "
            f"subsequent runs start in seconds{RESET}"
        )

        base = f"http://127.0.0.1:{func_port}"
        if not wait_for(f"{base}/api/status/none", args.host_timeout, "functions host"):
            print(f"\n{DIM}{host.tail(30)}{RESET}")
            return 1
        print(f"  {GREEN}host ready{RESET}")

        banner("3. Start the orchestration")
        payload = {
            "source": "demo-directory",
            "categories": args.categories,
            "max_pages": args.max_pages,
            "batch_size": args.batch_size,
            "enrich": True,
        }
        print(f"  POST /api/crawl {json.dumps(payload)}")
        started = httpx.post(f"{base}/api/crawl", json=payload, timeout=30)
        if started.status_code >= 400:
            print(f"  {RED}HTTP {started.status_code}{RESET} {started.text[:400]}")
            print(f"\n{DIM}{host.tail(30)}{RESET}")
            return 1

        management = started.json()
        instance_id = management.get("id") or management.get("instanceId")
        print(f"  instance: {instance_id}")

        banner("4. Orchestration progress")
        deadline = time.monotonic() + args.run_timeout
        final: dict | None = None
        last_status = None
        while time.monotonic() < deadline:
            state = httpx.get(f"{base}/api/status/{instance_id}", timeout=15).json()
            runtime, custom = state.get("runtime_status"), state.get("custom_status")
            if (runtime, json.dumps(custom, sort_keys=True)) != last_status:
                print(f"  {runtime:<12} {custom if custom else ''}")
                last_status = (runtime, json.dumps(custom, sort_keys=True))
            if runtime in ("Completed", "Failed", "Terminated"):
                final = state
                break
            time.sleep(1.0)

        if final is None:
            print(f"  {RED}timed out after {args.run_timeout}s{RESET}")
            print(f"\n{DIM}{host.tail(30)}{RESET}")
            return 1

        if final["runtime_status"] != "Completed":
            print(f"  {RED}orchestration {final['runtime_status']}{RESET}: {final.get('output')}")
            print(f"\n{DIM}{host.tail(30)}{RESET}")
            return 1

        output = final["output"]
        if isinstance(output, str):
            output = json.loads(output)

        banner("5. Result")
        print(f"  {GREEN}Completed{RESET}")
        for key in ("urls_discovered", "batches", "extracted", "enriched", "indexed"):
            print(f"  {key:<18} {output.get(key)}")
        for failure in output.get("failures", []):
            print(f"  {YELLOW}!{RESET} {failure}")

        banner("6. Verify against storage")
        store = RecordStore(AZURITE_CONNECTION_STRING)
        rows = store.query_source("demo-directory")
        print(f"  table rows for 'demo-directory': {len(rows)}")
        for row in rows[:6]:
            industry = row.get("industry") or "-"
            print(
                f"    {str(row.get('name'))[:32]:<34} {str(row.get('city') or '-'):<10} {industry}"
            )

        stats = httpx.get(f"http://127.0.0.1:{enrichment_port}/v1/_stats", timeout=10).json()
        print(
            f"\n  billable enrichment calls: {stats['total_billable_calls']} "
            f"{DIM}(idempotency keys seen: {stats['idempotency_keys_seen']}){RESET}"
        )

        artifacts = ArtifactStore(AZURITE_CONNECTION_STRING)
        print(f"  blob artifacts: {len(artifacts.list_names())}")

        ok = output.get("indexed", 0) > 0 and len(rows) > 0
        print(f"\n  {GREEN}PASS{RESET}" if ok else f"\n  {RED}FAIL{RESET}")
        return 0 if ok else 1

    finally:
        for mock in mocks:
            mock.stop()
        for process in reversed(processes):
            process.stop()


def parse() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--categories", nargs="+", default=["software", "logistics", "energy"])
    p.add_argument("--max-pages", type=int, default=2)
    p.add_argument("--batch-size", type=int, default=3)
    p.add_argument("--port", type=int, default=7071)
    # Generous: the first run downloads the extension bundle before it serves.
    p.add_argument("--host-timeout", type=float, default=900.0)
    p.add_argument("--run-timeout", type=float, default=180.0)
    return p.parse_args()


if __name__ == "__main__":
    raise SystemExit(main(parse()))
