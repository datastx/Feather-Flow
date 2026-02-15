SHELL := /bin/bash

.PHONY: build build-release test lint fmt check doc clean ci build-target checksums \
        ff-parse ff-parse-json ff-parse-deps ff-compile ff-compile-verbose \
        ff-run ff-run-full-refresh ff-run-select ff-ls ff-ls-json ff-ls-tree \
        ff-test ff-test-verbose ff-test-fail-fast ff-seed ff-seed-full-refresh \
        ff-function-deploy ff-function-validate \
        ff-docs ff-docs-json ff-docs-serve ff-docs-export ff-validate ff-validate-strict ff-sources ff-help \
        dev-cycle dev-validate dev-fresh ci-e2e help run watch test-verbose test-integration \
	test-unit test-quick test-failed test-sa test-sa-rust test-sa-all fmt-check clippy doc-open update ci-quick ci-full install install-cargo claude-auto-run \
        version version-bump-patch version-bump-minor version-set version-tag \
        build-linux clean-dist \
        docker-build docker-build-release docker-push docker-login docker-run \
        create-release \
        vscode-install vscode-build vscode-build-production vscode-watch vscode-test vscode-clean \
        vscode-package vscode-publish

# =============================================================================
# Configuration
# =============================================================================

# Version (read from workspace Cargo.toml)
VERSION := $(shell grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)"/\1/')

# Build output
DIST_DIR := dist

# Cargo
CARGO_BIN := $(HOME)/.cargo/bin/cargo

# Docker
DOCKER_REGISTRY := ghcr.io
DOCKER_IMAGE    := $(DOCKER_REGISTRY)/datastx/feather-flow
DOCKER_TAG      := $(VERSION)

# Default project for testing CLI commands
PROJECT_DIR ?= tests/fixtures/sample_project

# =============================================================================
# Development
# =============================================================================

build: ## Build all crates
	cargo build --workspace

build-release: ## Build release binaries
	cargo build --workspace --release

run: ## Run CLI with no arguments
	cargo run -p ff-cli --

watch: ## Watch and rebuild on changes
	cargo watch -x 'build --workspace'

# =============================================================================
# Rust Testing
# =============================================================================

test: ## Run all tests with summary
	@echo "Running tests..."
	@cargo test --workspace --all-features 2>&1 | tee /tmp/test-output.txt; \
	EXIT_CODE=$${PIPESTATUS[0]}; \
	echo ""; \
	echo "============================================================"; \
	echo "TEST SUMMARY"; \
	echo "============================================================"; \
	grep -E "^test result:" /tmp/test-output.txt | while read line; do \
		echo "$$line"; \
	done; \
	echo ""; \
	if [ $$EXIT_CODE -eq 0 ]; then \
		echo "✓ All tests passed!"; \
	else \
		echo "✗ Some tests failed. See output above for details."; \
		echo ""; \
		echo "Failed tests:"; \
		grep -E "^test .* FAILED" /tmp/test-output.txt || true; \
		exit $$EXIT_CODE; \
	fi
	@$(MAKE) test-sa-all

test-quick: ## Run tests without output capture (faster feedback)
	cargo test --workspace --all-features -- --test-threads=4

test-verbose: ## Run tests with stdout/stderr output
	cargo test --workspace -- --nocapture

test-integration: ## Run integration tests only
	cargo test --test '*' -- --test-threads=1

test-unit: ## Run unit tests only
	cargo test --workspace --lib

test-failed: ## Re-run only previously failed tests
	cargo test --workspace -- --failed

test-sa: ## Run static analysis CLI integration tests
	cargo test -p ff-cli --test sa_integration_tests -- --test-threads=1

test-sa-rust: ## Run Rust-level static analysis tests
	cargo test -p ff-cli --test integration_tests -- test_analysis --test-threads=1

test-sa-all: test-sa test-sa-rust ## Run all static analysis tests

# =============================================================================
# Code Quality
# =============================================================================

lint: fmt-check clippy ## Run fmt-check and clippy

fmt: ## Format code
	cargo fmt --all

fmt-check: ## Check code formatting
	cargo fmt --all -- --check

clippy: ## Run clippy linter
	cargo clippy --workspace --all-targets -- -D warnings

check: ## Run cargo check
	cargo check --workspace --all-targets

# =============================================================================
# Documentation
# =============================================================================

doc: ## Generate documentation
	cargo doc --workspace --no-deps

doc-open: ## Generate and open documentation
	cargo doc --workspace --no-deps --open

# =============================================================================
# CLI Commands - Featherflow (ff)
# =============================================================================

ff-parse: ## Parse SQL files
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) parse

ff-parse-json: ## Parse with JSON output
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) parse --output json

ff-parse-deps: ## Parse with deps output
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) parse --output deps

ff-compile: ## Compile Jinja to SQL
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) compile

ff-compile-verbose: ## Compile with verbose output
	cargo run -p ff-cli -- --verbose --project-dir $(PROJECT_DIR) compile

ff-run: ## Execute models
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) run

ff-run-full-refresh: ## Execute with full refresh
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) run --full-refresh

ff-run-select: ## Run specific models (set MODELS='+model_name')
	@if [ -z "$(MODELS)" ]; then \
		echo "Error: MODELS not set. Usage: make ff-run-select MODELS='+model_name'"; \
		exit 1; \
	fi
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) run --select $(MODELS)

ff-ls: ## List models
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) ls

ff-ls-json: ## List models as JSON
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) ls --output json

ff-ls-tree: ## List models as tree
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) ls --output tree

ff-test: ## Run schema tests
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) test

ff-test-verbose: ## Run tests with verbose output
	cargo run -p ff-cli -- --verbose --project-dir $(PROJECT_DIR) test

ff-test-fail-fast: ## Run tests, stop on first failure
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) test --fail-fast

ff-seed: ## Load seed data
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) seed

ff-seed-full-refresh: ## Load seeds with full refresh
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) seed --full-refresh

ff-function-deploy: ## Deploy user-defined functions
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) function deploy

ff-function-validate: ## Validate user-defined functions
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) function validate

ff-docs: ## Generate documentation
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) docs

ff-docs-json: ## Generate documentation as JSON
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) docs --format json

ff-docs-serve: ## Serve interactive documentation site
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) docs serve --no-browser

ff-docs-export: ## Export static documentation site
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) docs serve --static-export $(PROJECT_DIR)/target/docs-site --no-browser

ff-validate: ## Validate project
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) validate

ff-validate-strict: ## Validate with strict mode
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) validate --strict

ff-sources: ## List sources (JSON filtered)
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) ls --output json | jq '.[] | select(.type == "source")'

ff-help: ## Show CLI help
	cargo run -p ff-cli -- --help

# =============================================================================
# Workflows
# =============================================================================

dev-cycle: ff-seed ff-run ff-function-deploy ff-test ## Full cycle: seed -> run -> deploy functions -> test
	@echo "Development cycle complete!"

dev-validate: ff-compile ff-validate ## Quick validation: compile -> validate
	@echo "Validation complete!"

dev-fresh: ff-seed-full-refresh ff-run-full-refresh ff-function-deploy ff-test ## Full refresh pipeline
	@echo "Fresh pipeline complete!"

ci-e2e: ## End-to-end pipeline test against sample project
	@echo "=== E2E: clean ==="
	rm -rf crates/ff-cli/tests/fixtures/sample_project/target
	@echo "=== E2E: compile ==="
	$(MAKE) ff-compile PROJECT_DIR=crates/ff-cli/tests/fixtures/sample_project
	@echo "=== E2E: seed ==="
	$(MAKE) ff-seed-full-refresh PROJECT_DIR=crates/ff-cli/tests/fixtures/sample_project
	@echo "=== E2E: run (builds model tables — table functions not yet deployed) ==="
	-$(MAKE) ff-run-full-refresh PROJECT_DIR=crates/ff-cli/tests/fixtures/sample_project
	@echo "=== E2E: deploy functions (requires model tables from first run) ==="
	$(MAKE) ff-function-deploy PROJECT_DIR=crates/ff-cli/tests/fixtures/sample_project
	@echo "=== E2E: run (all models including table-function dependents) ==="
	$(MAKE) ff-run-full-refresh PROJECT_DIR=crates/ff-cli/tests/fixtures/sample_project
	@echo "=== E2E: test (materialized models) ==="
	cargo run -p ff-cli -- --project-dir crates/ff-cli/tests/fixtures/sample_project test -n dim_customers,fct_orders,dim_products,rpt_order_volume
	@echo "=== E2E: build (seed + per-model run/test) ==="
	cargo run -p ff-cli -- --project-dir crates/ff-cli/tests/fixtures/sample_project build --full-refresh
	@echo "E2E pipeline passed!"

# =============================================================================
# Maintenance
# =============================================================================

clean: ## Remove build artifacts
	cargo clean
	rm -rf $(PROJECT_DIR)/target

update: ## Update dependencies
	cargo update

# =============================================================================
# CI
# =============================================================================

ci: fmt-check clippy test doc ## Full CI check
	@echo "CI checks passed!"

ci-quick: check fmt-check clippy ## Quick CI check (no tests)
	@echo "Quick CI checks passed!"

ci-full: ci ff-compile ff-validate ## CI + compile + validate
	@echo "Full CI checks passed!"

# =============================================================================
# Version Management
# =============================================================================

version: ## Print current version
	@echo $(VERSION)

version-bump-patch: ## Bump patch version: 0.1.0 → 0.1.1
	@CURRENT=$(VERSION); \
	MAJOR=$$(echo $$CURRENT | cut -d. -f1); \
	MINOR=$$(echo $$CURRENT | cut -d. -f2); \
	PATCH=$$(echo $$CURRENT | cut -d. -f3); \
	NEW="$$MAJOR.$$MINOR.$$((PATCH + 1))"; \
	sed -i '' "s/^version = \"$$CURRENT\"/version = \"$$NEW\"/" Cargo.toml; \
	cargo generate-lockfile; \
	echo "Bumped $$CURRENT → $$NEW"

version-bump-minor: ## Bump minor version: 0.1.0 → 0.2.0
	@CURRENT=$(VERSION); \
	MAJOR=$$(echo $$CURRENT | cut -d. -f1); \
	MINOR=$$(echo $$CURRENT | cut -d. -f2); \
	NEW="$$MAJOR.$$((MINOR + 1)).0"; \
	sed -i '' "s/^version = \"$$CURRENT\"/version = \"$$NEW\"/" Cargo.toml; \
	cargo generate-lockfile; \
	echo "Bumped $$CURRENT → $$NEW"

version-set: ## Set explicit version (make version-set NEW_VERSION=0.2.0)
	@if [ -z "$(NEW_VERSION)" ]; then \
		echo "Error: NEW_VERSION not set. Usage: make version-set NEW_VERSION=0.2.0"; \
		exit 1; \
	fi
	sed -i '' 's/^version = ".*"/version = "$(NEW_VERSION)"/' Cargo.toml
	cargo generate-lockfile
	@echo "Version set to $(NEW_VERSION)"

version-tag: ## Create git tag from current Cargo.toml version
	git tag -a "v$(VERSION)" -m "Release v$(VERSION)"

# =============================================================================
# Release
# =============================================================================

install-cargo: ## Install Rust toolchain via rustup (if cargo missing)
	@if ! command -v cargo >/dev/null 2>&1; then \
		echo "cargo not found, installing Rust toolchain with rustup..."; \
		curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y; \
	fi

install: install-cargo ## Install CLI binary
	@set -e; \
	if command -v cargo >/dev/null 2>&1; then \
		cargo install --path crates/ff-cli; \
	elif [ -x "$(CARGO_BIN)" ]; then \
		"$(CARGO_BIN)" install --path crates/ff-cli; \
	else \
		echo "cargo not found after rustup install; restart your shell or source $$HOME/.cargo/env"; \
		exit 1; \
	fi

build-target: ## Build for target (set TARGET=x86_64-unknown-linux-gnu)
	@if [ -z "$(TARGET)" ]; then \
		echo "Error: TARGET not set. Usage: make build-target TARGET=x86_64-unknown-linux-gnu"; \
		exit 1; \
	fi
	cargo build --release --target $(TARGET) -p ff-cli

build-linux: ## Build Linux binary (gnu)
	cargo build --release --target x86_64-unknown-linux-gnu -p ff-cli
	mkdir -p $(DIST_DIR)
	cp target/x86_64-unknown-linux-gnu/release/ff $(DIST_DIR)/ff-x86_64-linux-gnu
	sha256sum $(DIST_DIR)/ff-x86_64-linux-gnu > $(DIST_DIR)/ff-x86_64-linux-gnu.sha256

clean-dist: ## Remove dist artifacts
	rm -rf $(DIST_DIR)

create-release: ## Create GitHub release (CI only — requires gh CLI auth)
	@if [ -z "$(TAG)" ]; then \
		echo "Error: TAG not set. Usage: make create-release TAG=v0.1.1"; \
		exit 1; \
	fi
	gh release create $(TAG) \
		$(DIST_DIR)/* \
		--title "$(TAG)" \
		--generate-notes

checksums: ## Create checksums (set ARTIFACTS_DIR=artifacts)
	cd $(ARTIFACTS_DIR) && \
	for dir in */; do \
		cd "$$dir" && \
		for file in *; do \
			sha256sum "$$file" > "$${file}.sha256"; \
		done && \
		cd ..; \
	done

# =============================================================================
# Docker
# =============================================================================

docker-build: ## Build Docker image (full multi-stage, for local dev)
	docker build \
		--build-arg VERSION=$(VERSION) \
		-t $(DOCKER_IMAGE):$(DOCKER_TAG) \
		-t $(DOCKER_IMAGE):latest \
		.

docker-build-release: ## Build Docker image from pre-built binary (CI only)
	docker build \
		-f Dockerfile.release \
		-t $(DOCKER_IMAGE):$(DOCKER_TAG) \
		-t $(DOCKER_IMAGE):latest \
		.

docker-push: ## Push Docker image to GHCR (requires: make docker-login)
	docker push $(DOCKER_IMAGE):$(DOCKER_TAG)
	docker push $(DOCKER_IMAGE):latest

docker-login: ## Authenticate to GHCR (CI sets GITHUB_TOKEN and GITHUB_ACTOR)
	@echo "$(GITHUB_TOKEN)" | docker login ghcr.io -u "$(GITHUB_ACTOR)" --password-stdin

docker-run: ## Run ff in Docker (pass CMD="validate" etc.)
	docker run --rm -v $(PWD):/workspace -w /workspace $(DOCKER_IMAGE):latest $(CMD)

# =============================================================================
# VS Code Extension
# =============================================================================

VSCODE_DIR := vscode-featherflow

vscode-install: ## Install VS Code extension dependencies
	cd $(VSCODE_DIR) && npm ci

vscode-build: vscode-install ## Build VS Code extension (dev)
	cd $(VSCODE_DIR) && node esbuild.js

vscode-build-production: vscode-install ## Build VS Code extension (production, minified)
	cd $(VSCODE_DIR) && node esbuild.js --production

vscode-watch: vscode-install ## Watch and rebuild VS Code extension on changes
	cd $(VSCODE_DIR) && node esbuild.js --watch

vscode-test: vscode-install ## Run VS Code extension tests
	cd $(VSCODE_DIR) && npx vitest run

vscode-package: vscode-build-production ## Package VS Code extension as .vsix
	mkdir -p $(DIST_DIR)
	cd $(VSCODE_DIR) && npx vsce package --out ../$(DIST_DIR)/

vscode-publish: vscode-build-production ## Publish VS Code extension to Marketplace (requires VSCE_PAT)
	cd $(VSCODE_DIR) && npx vsce publish

vscode-clean: ## Remove VS Code extension build artifacts
	rm -rf $(VSCODE_DIR)/dist $(VSCODE_DIR)/node_modules

# =============================================================================
# Claude Code
# =============================================================================

claude-auto-run: ## Run Claude with auto-permissions
	claude --dangerously-skip-permissions

# =============================================================================
# Help
# =============================================================================

help: ## Show this help message
	@echo "Featherflow Development Makefile"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-24s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Environment Variables:"
	@echo "  PROJECT_DIR    Test project path (default: tests/fixtures/sample_project)"
	@echo "  TARGET         Rust build target (e.g., x86_64-unknown-linux-gnu)"
	@echo "  NEW_VERSION    Version to set (e.g., 0.2.0)"
	@echo "  TAG            Git tag for release (e.g., v0.1.1)"
	@echo "  CMD            Docker command to run (e.g., validate)"
