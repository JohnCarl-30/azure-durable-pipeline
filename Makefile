.DEFAULT_GOAL := help
VENV := .venv
PY   := $(VENV)/bin/python
TF   := ./bin/terraform

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
	  awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n",$$1,$$2}'

install: ## Create the venv, install python deps and project-local tooling
	uv venv --python 3.12 $(VENV)
	uv pip install --python $(PY) -e ".[dev]"
	npm install
	@test -x $(TF) || ( \
	  echo "downloading terraform..." && mkdir -p bin && \
	  curl -sSL -o /tmp/tf.zip "https://releases.hashicorp.com/terraform/1.16.2/terraform_1.16.2_$$(uname -s | tr A-Z a-z)_$$(uname -m | sed -e s/x86_64/amd64/ -e s/aarch64/arm64/).zip" && \
	  unzip -oq /tmp/tf.zip -d bin/ && chmod +x $(TF) && rm -f /tmp/tf.zip )
	@echo "terraform: $$($(TF) version | head -1)"

test: ## Run the suite (storage + infra tests self-skip if unavailable)
	$(PY) -m pytest -q

test-unit: ## Orchestrator and activity tests only -- no emulator, no terraform
	$(PY) -m pytest -q tests/test_orchestrators.py tests/test_activities.py

azurite: ## Start the storage emulator in the foreground
	npm run azurite

run: ## End-to-end against the real Functions host
	$(PY) scripts/run_local.py

approval: ## Verify the human-in-the-loop path (start -> wait -> approve -> resume)
	$(PY) scripts/verify_approval.py

start: ## Just the Functions host (expects azurite + mocks already running)
	npx func start --python

lint: ## Lint and format check, python and terraform
	$(VENV)/bin/ruff check src tests scripts function_app.py
	$(VENV)/bin/ruff format --check src tests scripts function_app.py
	$(TF) -chdir=infra fmt -check -recursive

fmt: ## Auto-format
	$(VENV)/bin/ruff format src tests scripts function_app.py
	$(VENV)/bin/ruff check --fix src tests scripts function_app.py
	$(TF) -chdir=infra fmt -recursive

tf-init: ## Initialise terraform without a backend (no Azure account needed)
	$(TF) -chdir=infra init -backend=false -input=false

tf-validate: tf-init ## Validate the infrastructure configuration
	$(TF) -chdir=infra validate

clean: ## Remove emulator state, logs and caches
	rm -rf .azurite .logs .pytest_cache .ruff_cache

.PHONY: help install test test-unit azurite run start lint fmt tf-init tf-validate clean
