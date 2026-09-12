"""Infrastructure checks.

`terraform validate` catches syntax and type errors but says nothing about
whether the configuration is *safe*. These assert the security properties that
matter, by reading the configuration directly -- so a future edit that
reintroduces a connection string in app settings fails here rather than in a
review.

Skipped when the project-local terraform binary is absent.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
INFRA = ROOT / "infra"
TERRAFORM = ROOT / "bin" / "terraform"

pytestmark = pytest.mark.skipif(
    not TERRAFORM.exists(), reason="terraform binary not present (see make install)"
)


def _run(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [str(TERRAFORM), *args], cwd=INFRA, capture_output=True, text=True, timeout=300
    )


@pytest.fixture(scope="module")
def initialised() -> bool:
    if not (INFRA / ".terraform").exists():
        result = _run("init", "-backend=false", "-input=false")
        assert result.returncode == 0, result.stderr
    return True


def test_configuration_is_valid(initialised):
    result = _run("validate", "-json")
    payload = json.loads(result.stdout)
    assert payload["valid"] is True, payload.get("diagnostics")
    assert payload["error_count"] == 0


def test_configuration_is_canonically_formatted(initialised):
    result = _run("fmt", "-check", "-recursive")
    assert result.returncode == 0, f"unformatted files:\n{result.stdout}"


def _main_tf() -> str:
    """Raw configuration source. Use for anything inside a string literal."""
    return (INFRA / "main.tf").read_text()


def _attributes() -> str:
    """Source with assignment alignment collapsed.

    `terraform fmt` pads `=` into columns, so asserting on raw text breaks
    whenever an unrelated attribute in the same block changes length. This
    normalises *assignments only* -- applying it to the whole file would also
    rewrite `=` inside string values such as a Key Vault reference URI.
    """
    raw = _main_tf()
    return re.sub(r"^(\s*[\w.\[\]\"]+)\s+=\s+", r"\1 = ", raw, flags=re.MULTILINE)


def test_no_connection_strings_in_app_settings():
    """The core security property: identity-based access, not shared keys.

    A leaked app-settings dump should yield nothing useful.
    """
    source = _attributes()
    assert "__accountName" in source
    assert '"managedidentity"' in source
    assert "primary_connection_string" not in source
    assert "primary_access_key" not in source


def test_the_only_secret_is_a_key_vault_reference():
    source = _main_tf()
    assert "@Microsoft.KeyVault(SecretUri=" in source


def test_storage_refuses_plaintext_and_legacy_tls():
    source = _attributes()
    assert "https_traffic_only_enabled = true" in source
    assert 'min_tls_version = "TLS1_2"' in source
    assert "allow_nested_items_to_be_public = false" in source


def test_rbac_roles_are_data_plane_not_contributor():
    """Least privilege: the app reads and writes data, it does not manage resources."""
    source = _attributes()
    for role in (
        "Storage Blob Data Owner",
        "Storage Queue Data Contributor",
        "Storage Table Data Contributor",
        "Azure Service Bus Data Receiver",
        "Key Vault Secrets User",
    ):
        assert role in source, f"missing least-privilege role: {role}"

    # The blunt instruments that would make the identity over-powered.
    assert 'role_definition_name = "Contributor"' not in source
    assert 'role_definition_name = "Owner"' not in source


def test_the_queue_dead_letters_instead_of_looping_forever():
    source = _attributes()
    assert "max_delivery_count" in source
    assert "dead_lettering_on_message_expiration = true" in source


def test_dead_letters_are_alerted_on():
    source = _main_tf()
    assert "DeadletteredMessages" in source


def test_diagnostics_flow_to_log_analytics():
    source = _main_tf()
    assert source.count("azurerm_monitor_diagnostic_setting") >= 2
    assert "workspace_id" in source


def test_terraform_binary_is_gitignored():
    """Downloaded tooling must not be committed."""
    assert "bin/" in (ROOT / ".gitignore").read_text()


def test_state_and_local_settings_are_gitignored():
    """State can contain secrets; local.settings.json holds connection strings."""
    ignored = (ROOT / ".gitignore").read_text()
    for pattern in ("*.tfstate", "local.settings.json", ".terraform/"):
        assert pattern in ignored, f"{pattern} must be gitignored"


def test_no_state_file_is_present_in_the_repo():
    assert not list(INFRA.glob("*.tfstate")), "terraform state must never be committed"


def test_terraform_is_available_and_recent():
    assert shutil.which(str(TERRAFORM))
    result = _run("version", "-json")
    version = json.loads(result.stdout)["terraform_version"]
    major, minor, *_ = (int(p) for p in version.split("."))
    assert (major, minor) >= (1, 9), f"terraform {version} is older than required"
