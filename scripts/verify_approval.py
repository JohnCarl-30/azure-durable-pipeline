#!/usr/bin/env python3
"""Prove the human-in-the-loop path works against the real Functions host.

Starts an approval, confirms it suspends waiting, raises the external event,
and confirms it resumes and completes. This is the loop that was unreachable
before: the orchestrator existed and was unit-tested, but nothing could start it.
"""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import httpx  # noqa: E402

GREEN, RED, DIM, BOLD, RESET = "\033[32m", "\033[31m", "\033[2m", "\033[1m", "\033[0m"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def main() -> int:
    logs = ROOT / ".logs"
    logs.mkdir(exist_ok=True)
    port = free_port()

    azurite = None
    try:
        httpx.get("http://127.0.0.1:10002/devstoreaccount1", timeout=1.5)
    except httpx.HTTPError:
        azurite = subprocess.Popen(
            [
                str(ROOT / "node_modules/.bin/azurite"),
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
            stdout=(logs / "azurite.log").open("w"),
            stderr=subprocess.STDOUT,
            preexec_fn=os.setsid,
        )
        for _ in range(60):
            try:
                httpx.get("http://127.0.0.1:10002/devstoreaccount1", timeout=1)
                break
            except httpx.HTTPError:
                time.sleep(0.5)

    host = subprocess.Popen(
        [str(ROOT / "node_modules/.bin/func"), "start", "--port", str(port), "--verbose"],
        cwd=ROOT,
        env={
            **os.environ,
            "AzureWebJobsStorage": "UseDevelopmentStorage=true",
            "AzureWebJobsFeatureFlags": "EnableWorkerIndexing",
            "FUNCTIONS_WORKER_RUNTIME": "python",
            "PATH": f"{ROOT / '.venv' / 'bin'}:{os.environ.get('PATH', '')}",
        },
        stdout=(logs / "approval.log").open("w"),
        stderr=subprocess.STDOUT,
        preexec_fn=os.setsid,
    )
    base = f"http://127.0.0.1:{port}"
    try:
        print("waiting for host...", end="", flush=True)
        for _ in range(180):
            try:
                httpx.get(f"{base}/api/status/none", timeout=2)
                break
            except httpx.HTTPError:
                time.sleep(1)
        else:
            print(f" {RED}never came up{RESET}")
            return 1
        print(f" {GREEN}ready{RESET}\n")

        print(f"{BOLD}1. Start an approval (24h timeout){RESET}")
        started = httpx.post(f"{base}/api/approval", json={"timeout_hours": 24}, timeout=30)
        instance = started.json().get("id") or started.json().get("instanceId")
        print(f"   instance: {instance}")

        time.sleep(3)
        state = httpx.get(f"{base}/api/status/{instance}", timeout=15).json()
        print(f"\n{BOLD}2. Is it suspended, waiting?{RESET}")
        print(f"   runtime_status: {state['runtime_status']}")
        waiting = state["runtime_status"] == "Running"
        print(
            f"   {GREEN}suspended on an external event, 0 CPU{RESET}"
            if waiting
            else f"   {RED}unexpected state{RESET}"
        )

        print(f"\n{BOLD}3. Approve it{RESET}")
        httpx.post(f"{base}/api/approve/{instance}", params={"approved": "true"}, timeout=30)
        print("   POST /api/approve -> event raised")

        print(f"\n{BOLD}4. Did it resume and complete?{RESET}")
        final = None
        for _ in range(30):
            time.sleep(1)
            state = httpx.get(f"{base}/api/status/{instance}", timeout=15).json()
            if state["runtime_status"] in ("Completed", "Failed", "Terminated"):
                final = state
                break
        if final is None:
            print(f"   {RED}still running after 30s{RESET}")
            return 1

        output = final["output"]
        if isinstance(output, str):
            output = json.loads(output)
        print(f"   runtime_status: {final['runtime_status']}")
        print(f"   output: {output}")

        ok = final["runtime_status"] == "Completed" and output == {
            "approved": True,
            "timed_out": False,
        }
        print(f"\n   {GREEN}PASS{RESET}" if ok else f"\n   {RED}FAIL{RESET}")
        return 0 if ok else 1
    finally:
        for proc in (host, azurite):
            if proc:
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                except Exception:
                    proc.terminate()


if __name__ == "__main__":
    raise SystemExit(main())
