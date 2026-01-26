.PHONY: build build-release test lint fmt check doc clean ci build-target checksums \
        ff-parse ff-parse-json ff-parse-deps ff-compile ff-compile-verbose \
        ff-run ff-run-full-refresh ff-run-select ff-ls ff-ls-json ff-ls-tree \
        ff-test ff-test-verbose ff-test-fail-fast ff-seed ff-seed-full-refresh \
        ff-docs ff-docs-json ff-validate ff-validate-strict ff-sources ff-help \
        dev-cycle dev-validate dev-fresh help run watch test-verbose test-integration \
        test-unit fmt-check clippy doc-open update ci-quick ci-full install claude-auto-run

# =============================================================================
# Configuration
# =============================================================================

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

test: ## Run all tests
	cargo test --workspace --all-features

test-verbose: ## Run tests with output
	cargo test --workspace -- --nocapture

test-integration: ## Run integration tests only
	cargo test --test '*' -- --test-threads=1

test-unit: ## Run unit tests only
	cargo test --workspace --lib

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

ff-docs: ## Generate documentation
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) docs

ff-docs-json: ## Generate documentation as JSON
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) docs --format json

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

dev-cycle: ff-seed ff-run ff-test ## Full cycle: seed -> run -> test
	@echo "Development cycle complete!"

dev-validate: ff-compile ff-validate ## Quick validation: compile -> validate
	@echo "Validation complete!"

dev-fresh: ff-seed-full-refresh ff-run-full-refresh ff-test ## Full refresh pipeline
	@echo "Fresh pipeline complete!"

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
# Release
# =============================================================================

install: ## Install CLI binary
	cargo install --path crates/ff-cli

build-target: ## Build for target (set TARGET=x86_64-unknown-linux-gnu)
	@if [ -z "$(TARGET)" ]; then \
		echo "Error: TARGET not set. Usage: make build-target TARGET=x86_64-unknown-linux-gnu"; \
		exit 1; \
	fi
	cargo build --release --target $(TARGET) -p ff-cli

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
