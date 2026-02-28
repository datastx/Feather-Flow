MAKEFLAGS += --warn-undefined-variables
SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help
.DELETE_ON_ERROR:
.SUFFIXES:

# =============================================================================
# Configuration
# =============================================================================

# Version (read from workspace Cargo.toml)
VERSION := $(shell grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)"/\1/')

# Build output
DIST_DIR := dist

# Cargo
cargo_bin := $(HOME)/.cargo/bin/cargo

# Docker
DOCKER_REGISTRY := ghcr.io
DOCKER_IMAGE    := $(DOCKER_REGISTRY)/datastx/feather-flow
DOCKER_TAG      := $(VERSION)

# Default project for testing CLI commands
PROJECT_DIR ?= crates/ff-cli/tests/fixtures/sample_project

# VS Code extension directory
vscode_dir := vscode-featherflow

# =============================================================================
# Development
# =============================================================================

.PHONY: build
build: ## Build all crates
	cargo build --workspace

.PHONY: build-release
build-release: ## Build release binaries
	cargo build --workspace --release

.PHONY: run
run: ## Run CLI with no arguments
	cargo run -p ff-cli --

.PHONY: watch
watch: ## Watch and rebuild on changes
	cargo watch -x 'build --workspace'

# =============================================================================
# Rust Testing
# =============================================================================

.PHONY: test
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
		echo "All tests passed!"; \
	else \
		echo "Some tests failed. See output above for details."; \
		echo ""; \
		echo "Failed tests:"; \
		grep -E "^test .* FAILED" /tmp/test-output.txt || true; \
		exit $$EXIT_CODE; \
	fi
	@$(MAKE) test-sa-all

.PHONY: test-quick
test-quick: ## Run tests without output capture (faster feedback)
	cargo test --workspace --all-features -- --test-threads=4

.PHONY: test-verbose
test-verbose: ## Run tests with stdout/stderr output
	cargo test --workspace -- --nocapture

.PHONY: test-integration
test-integration: ## Run integration tests only
	cargo test --test '*' -- --test-threads=1

.PHONY: test-unit
test-unit: ## Run unit tests only
	cargo test --workspace --lib

.PHONY: test-failed
test-failed: ## Re-run only previously failed tests
	cargo test --workspace -- --failed

.PHONY: test-sa
test-sa: ## Run static analysis CLI integration tests
	cargo test -p ff-cli --test sa_integration_tests -- --test-threads=1

.PHONY: test-sa-rust
test-sa-rust: ## Run Rust-level static analysis tests
	cargo test -p ff-cli --test integration_tests -- test_analysis --test-threads=1

.PHONY: test-sa-all
test-sa-all: test-sa test-sa-rust ## Run all static analysis tests

.PHONY: bench
bench: ## Run memory and performance benchmarks
	cargo test -p ff-cli --release --test memory_bench -- --nocapture

# =============================================================================
# Code Quality
# =============================================================================

.PHONY: lint
lint: fmt-check clippy ## Run fmt-check and clippy

.PHONY: fmt
fmt: ## Format code
	cargo fmt --all

.PHONY: fmt-check
fmt-check: ## Check code formatting
	cargo fmt --all -- --check

.PHONY: fmt-fixtures
fmt-fixtures: ## Format SQL in all test fixtures
	@echo "Formatting test fixtures..."
	@for dir in crates/ff-cli/tests/fixtures/*/; do \
		if [ -f "$$dir/featherflow.yml" ] && [ "$$(basename $$dir)" != "jinja_error_project" ]; then \
			echo "  Formatting $$(basename $$dir)..."; \
			(cd "$$dir" && $(PWD)/target/release/ff dt fmt 2>/dev/null) || true; \
		fi \
	done

.PHONY: fmt-fixtures-check
fmt-fixtures-check: ## Check fixture SQL formatting (CI gate)
	@echo "Checking test fixture formatting..."
	@failed=0; \
	for dir in crates/ff-cli/tests/fixtures/*/; do \
		if [ -f "$$dir/featherflow.yml" ] && [ "$$(basename $$dir)" != "jinja_error_project" ]; then \
			if ! (cd "$$dir" && $(PWD)/target/release/ff dt fmt --check 2>/dev/null); then \
				echo "  UNFORMATTED: $$(basename $$dir)"; \
				failed=1; \
			fi \
		fi \
	done; \
	if [ $$failed -eq 1 ]; then echo "Some fixtures need formatting. Run 'make fmt-fixtures'"; exit 1; fi

.PHONY: clippy
clippy: ## Run clippy linter
	cargo clippy --workspace --all-targets -- -D warnings

.PHONY: check
check: ## Run cargo check
	cargo check --workspace --all-targets

# =============================================================================
# Documentation
# =============================================================================

.PHONY: doc
doc: ## Generate documentation
	cargo doc --workspace --no-deps

.PHONY: doc-open
doc-open: ## Generate and open documentation
	cargo doc --workspace --no-deps --open

# =============================================================================
# CLI Commands - Featherflow (ff)
# =============================================================================

.PHONY: ff-compile
ff-compile: ## Compile Jinja to SQL
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) compile

.PHONY: ff-compile-verbose
ff-compile-verbose: ## Compile with verbose output
	cargo run -p ff-cli -- --verbose --project-dir $(PROJECT_DIR) compile

.PHONY: ff-run
ff-run: ## Execute models
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) run

.PHONY: ff-run-full-refresh
ff-run-full-refresh: ## Execute with full refresh
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) run --full-refresh

.PHONY: ff-run-select
ff-run-select: ## Run specific models (set MODELS='+model_name')
	@if [ -z "$(MODELS)" ]; then \
		echo "Error: MODELS not set. Usage: make ff-run-select MODELS='+model_name'"; \
		exit 1; \
	fi
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) run --nodes $(MODELS)

.PHONY: ff-ls
ff-ls: ## List models
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) ls

.PHONY: ff-ls-json
ff-ls-json: ## List models as JSON
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) ls --output json

.PHONY: ff-ls-tree
ff-ls-tree: ## List models as tree
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) ls --output tree

.PHONY: ff-test
ff-test: ## Run schema tests
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) run --mode test

.PHONY: ff-test-verbose
ff-test-verbose: ## Run tests with verbose output
	cargo run -p ff-cli -- --verbose --project-dir $(PROJECT_DIR) run --mode test

.PHONY: ff-test-fail-fast
ff-test-fail-fast: ## Run tests, stop on first failure
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) run --mode test --fail-fast

.PHONY: ff-seed
ff-seed: ## Load seed data
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) deploy seeds

.PHONY: ff-seed-full-refresh
ff-seed-full-refresh: ## Load seeds with full refresh
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) deploy seeds --full-refresh

.PHONY: ff-function-deploy
ff-function-deploy: ## Deploy user-defined functions
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) deploy functions deploy

.PHONY: ff-function-validate
ff-function-validate: ## Validate user-defined functions
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) deploy functions validate

.PHONY: ff-docs
ff-docs: ## Generate documentation
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) docs

.PHONY: ff-docs-json
ff-docs-json: ## Generate documentation as JSON
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) docs --format json

.PHONY: ff-docs-serve
ff-docs-serve: ## Serve interactive documentation site
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) docs serve --no-browser

.PHONY: ff-docs-export
ff-docs-export: ## Export static documentation site
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) docs serve --static-export $(PROJECT_DIR)/target/docs-site --no-browser

.PHONY: ff-validate
ff-validate: ## Validate project (via compile)
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) compile --parse-only

.PHONY: ff-validate-strict
ff-validate-strict: ## Validate with strict mode (via compile --strict)
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) compile --parse-only --strict

.PHONY: ff-sources
ff-sources: ## List sources (JSON filtered)
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) ls --output json | jq '.[] | select(.type == "source")'

.PHONY: ff-help
ff-help: ## Show CLI help
	cargo run -p ff-cli -- --help

# =============================================================================
# Workflows
# =============================================================================

.PHONY: dev-cycle
dev-cycle: ff-seed ff-run ff-function-deploy ff-test ## Full cycle: seed -> run -> deploy functions -> test
	@echo "Development cycle complete!"

.PHONY: dev-validate
dev-validate: ff-compile ## Quick validation: compile (includes validate)
	@echo "Validation complete!"

.PHONY: dev-fresh
dev-fresh: ff-seed-full-refresh ff-run-full-refresh ff-function-deploy ff-test ## Full refresh pipeline
	@echo "Fresh pipeline complete!"

.PHONY: ci-e2e
ci-e2e: ## End-to-end pipeline test against sample project
	$(eval e2e_dir := crates/ff-cli/tests/fixtures/sample_project)
	@echo "=== E2E: clean ==="
	rm -rf $(e2e_dir)/target
	@echo "=== E2E: compile ==="
	cargo run -p ff-cli -- --project-dir $(e2e_dir) dt compile
	@echo "=== E2E: seed ==="
	cargo run -p ff-cli -- --project-dir $(e2e_dir) dt deploy seeds --full-refresh
	@echo "=== E2E: deploy scalar functions (needed by models like fct_orders) ==="
	cargo run -p ff-cli -- --project-dir $(e2e_dir) dt deploy functions deploy --functions safe_divide,cents_to_dollars
	@echo "=== E2E: run --mode models (scalar functions available, table functions not yet) ==="
	-cargo run -p ff-cli -- --project-dir $(e2e_dir) run --mode models --full-refresh
	@echo "=== E2E: deploy table functions (requires model tables from first run) ==="
	cargo run -p ff-cli -- --project-dir $(e2e_dir) dt deploy functions deploy --functions order_volume_by_status
	@echo "=== E2E: run --mode models (all models including table-function dependents) ==="
	cargo run -p ff-cli -- --project-dir $(e2e_dir) run --mode models --full-refresh
	@echo "=== E2E: run --mode test (materialized models) ==="
	cargo run -p ff-cli -- --project-dir $(e2e_dir) run --mode test -n dim_customers,fct_orders,dim_products,rpt_order_volume
	@echo "=== E2E: run --mode build (seed + per-model run/test) ==="
	cargo run -p ff-cli -- --project-dir $(e2e_dir) run --mode build --full-refresh
	@echo "E2E pipeline passed!"

# =============================================================================
# Maintenance
# =============================================================================

.PHONY: clean
clean: ## Remove build artifacts
	cargo clean
	rm -rf $(PROJECT_DIR)/target

.PHONY: update
update: ## Update dependencies
	cargo update

# =============================================================================
# CI
# =============================================================================

.PHONY: ci
ci: fmt-check clippy test doc ## Full CI check
	@echo "CI checks passed!"

.PHONY: ci-quick
ci-quick: check fmt-check clippy ## Quick CI check (no tests)
	@echo "Quick CI checks passed!"

.PHONY: ci-full
ci-full: ci ff-compile ## CI + compile (includes validate)
	@echo "Full CI checks passed!"

# =============================================================================
# Version Management
# =============================================================================

.PHONY: version
version: ## Print current version
	@echo $(VERSION)

.PHONY: version-bump-patch
version-bump-patch: ## Bump patch version: 0.1.0 -> 0.1.1
	@CURRENT=$(VERSION); \
	MAJOR=$$(echo $$CURRENT | cut -d. -f1); \
	MINOR=$$(echo $$CURRENT | cut -d. -f2); \
	PATCH=$$(echo $$CURRENT | cut -d. -f3); \
	NEW="$$MAJOR.$$MINOR.$$((PATCH + 1))"; \
	sed -i '' "s/^version = \"$$CURRENT\"/version = \"$$NEW\"/" Cargo.toml; \
	cargo generate-lockfile; \
	echo "Bumped $$CURRENT -> $$NEW"

.PHONY: version-bump-minor
version-bump-minor: ## Bump minor version: 0.1.0 -> 0.2.0
	@CURRENT=$(VERSION); \
	MAJOR=$$(echo $$CURRENT | cut -d. -f1); \
	MINOR=$$(echo $$CURRENT | cut -d. -f2); \
	NEW="$$MAJOR.$$((MINOR + 1)).0"; \
	sed -i '' "s/^version = \"$$CURRENT\"/version = \"$$NEW\"/" Cargo.toml; \
	cargo generate-lockfile; \
	echo "Bumped $$CURRENT -> $$NEW"

.PHONY: version-set
version-set: ## Set explicit version (make version-set NEW_VERSION=0.2.0)
	@if [ -z "$(NEW_VERSION)" ]; then \
		echo "Error: NEW_VERSION not set. Usage: make version-set NEW_VERSION=0.2.0"; \
		exit 1; \
	fi
	sed -i '' 's/^version = ".*"/version = "$(NEW_VERSION)"/' Cargo.toml
	cargo generate-lockfile
	@echo "Version set to $(NEW_VERSION)"

.PHONY: version-tag
version-tag: ## Create git tag from current Cargo.toml version
	git tag -a "v$(VERSION)" -m "Release v$(VERSION)"

# =============================================================================
# Release
# =============================================================================

.PHONY: install-cargo
install-cargo: ## Install Rust toolchain via rustup (if cargo missing)
	@if ! command -v cargo >/dev/null 2>&1; then \
		echo "cargo not found, installing Rust toolchain with rustup..."; \
		curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y; \
	fi

.PHONY: install
install: install-cargo ## Install CLI binary
	@set -e; \
	if command -v cargo >/dev/null 2>&1; then \
		cargo install --path crates/ff-cli; \
	elif [ -x "$(cargo_bin)" ]; then \
		"$(cargo_bin)" install --path crates/ff-cli; \
	else \
		echo "cargo not found after rustup install; restart your shell or source $$HOME/.cargo/env"; \
		exit 1; \
	fi

.PHONY: build-target
build-target: ## Build for target (set TARGET=x86_64-unknown-linux-gnu)
	@if [ -z "$(TARGET)" ]; then \
		echo "Error: TARGET not set. Usage: make build-target TARGET=x86_64-unknown-linux-gnu"; \
		exit 1; \
	fi
	cargo build --release --target $(TARGET) -p ff-cli

.PHONY: build-linux
build-linux: ## Build Linux binary (gnu)
	cargo build --release --target x86_64-unknown-linux-gnu -p ff-cli
	mkdir -p $(DIST_DIR)
	cp target/x86_64-unknown-linux-gnu/release/ff $(DIST_DIR)/ff-x86_64-linux-gnu
	sha256sum $(DIST_DIR)/ff-x86_64-linux-gnu > $(DIST_DIR)/ff-x86_64-linux-gnu.sha256

.PHONY: clean-dist
clean-dist: ## Remove dist artifacts
	rm -rf $(DIST_DIR)

.PHONY: create-release
create-release: ## Create GitHub release (CI only — requires gh CLI auth)
	@if [ -z "$(TAG)" ]; then \
		echo "Error: TAG not set. Usage: make create-release TAG=v0.1.1"; \
		exit 1; \
	fi
	gh release create $(TAG) \
		$(DIST_DIR)/* \
		--title "$(TAG)" \
		--generate-notes

.PHONY: checksums
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

.PHONY: docker-build
docker-build: ## Build Docker image (full multi-stage, for local dev)
	docker build \
		--build-arg VERSION=$(VERSION) \
		-t $(DOCKER_IMAGE):$(DOCKER_TAG) \
		-t $(DOCKER_IMAGE):latest \
		.

.PHONY: docker-build-release
docker-build-release: ## Build Docker image from pre-built binary (CI only)
	docker build \
		-f Dockerfile.release \
		-t $(DOCKER_IMAGE):$(DOCKER_TAG) \
		-t $(DOCKER_IMAGE):latest \
		.

.PHONY: docker-push
docker-push: ## Push Docker image to GHCR (requires: make docker-login)
	docker push $(DOCKER_IMAGE):$(DOCKER_TAG)
	docker push $(DOCKER_IMAGE):latest

.PHONY: docker-login
docker-login: ## Authenticate to GHCR (CI sets GITHUB_TOKEN and GITHUB_ACTOR)
	@echo "$(GITHUB_TOKEN)" | docker login ghcr.io -u "$(GITHUB_ACTOR)" --password-stdin

.PHONY: docker-run
docker-run: ## Run ff in Docker (pass CMD="validate" etc.)
	docker run --rm -v $(PWD):/workspace -w /workspace $(DOCKER_IMAGE):latest $(CMD)

# =============================================================================
# VS Code Extension
# =============================================================================

.PHONY: vscode-install
vscode-install: ## Install VS Code extension dependencies
	cd $(vscode_dir) && npm ci

.PHONY: vscode-build
vscode-build: vscode-install ## Build VS Code extension (dev)
	cd $(vscode_dir) && node esbuild.js

.PHONY: vscode-build-production
vscode-build-production: vscode-install ## Build VS Code extension (production, minified)
	cd $(vscode_dir) && node esbuild.js --production

.PHONY: vscode-watch
vscode-watch: vscode-install ## Watch and rebuild VS Code extension on changes
	cd $(vscode_dir) && node esbuild.js --watch

.PHONY: vscode-test
vscode-test: vscode-install ## Run VS Code extension tests
	cd $(vscode_dir) && npx vitest run

.PHONY: vscode-package
vscode-package: vscode-build-production ## Package VS Code extension as .vsix
	mkdir -p $(DIST_DIR)
	cd $(vscode_dir) && npx vsce package --out ../$(DIST_DIR)/

.PHONY: vscode-install-local
vscode-install-local: vscode-build-production ## Package and install VS Code extension locally
	cd $(vscode_dir) && npx @vscode/vsce package --no-dependencies && code --install-extension featherflow-*.vsix && rm -f featherflow-*.vsix

.PHONY: vscode-publish
vscode-publish: vscode-build-production ## Publish VS Code extension to Marketplace (requires VSCE_PAT)
	cd $(vscode_dir) && npx vsce publish

.PHONY: vscode-clean
vscode-clean: ## Remove VS Code extension build artifacts
	rm -rf $(vscode_dir)/dist $(vscode_dir)/node_modules

# =============================================================================
# Claude Code
# =============================================================================

.PHONY: claude-auto-run
claude-auto-run: ## Run Claude with auto-permissions
	claude --dangerously-skip-permissions

# =============================================================================
# Help
# =============================================================================

.PHONY: help
help: ## Show this help message
	@echo "Featherflow Development Makefile"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-24s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Environment Variables:"
	@echo "  PROJECT_DIR    Test project path (default: tests/fixtures/sample_project)"
	@echo "  TARGET         Rust build target (e.g., x86_64-unknown-linux-gnu)"
	@echo "  NEW_VERSION    Version to set (e.g., 0.2.0)"
	@echo "  TAG            Git tag for release (e.g., v0.1.1)"
	@echo "  CMD            Docker command to run (e.g., validate)"
