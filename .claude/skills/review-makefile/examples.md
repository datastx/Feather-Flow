## Review Makefile - Extended Examples

This file contains advanced patterns and real-world examples for complex Makefile scenarios.

## Advanced Self-Documenting Help Patterns

### Multi-section help with categories

```makefile
.PHONY: help
help: ## Show this help message with categories
	@echo "$(shell tput bold)Featherflow Development Makefile$(shell tput sgr0)"
	@echo ""
	@echo "$(shell tput setaf 3)USAGE:$(shell tput sgr0)"
	@echo "  make [target]"
	@echo ""
	@echo "$(shell tput setaf 3)DEVELOPMENT:$(shell tput sgr0)"
	@grep -E '^[a-zA-Z_-]+:.*?## DEV: .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## DEV: "}; {printf "  $(shell tput setaf 6)%-20s$(shell tput sgr0) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(shell tput setaf 3)TESTING:$(shell tput sgr0)"
	@grep -E '^[a-zA-Z_-]+:.*?## TEST: .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## TEST: "}; {printf "  $(shell tput setaf 6)%-20s$(shell tput sgr0) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(shell tput setaf 3)CLI COMMANDS:$(shell tput sgr0)"
	@grep -E '^ff-[a-zA-Z_-]+:.*?## CLI: .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## CLI: "}; {printf "  $(shell tput setaf 6)%-20s$(shell tput sgr0) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(shell tput setaf 3)ENVIRONMENT VARIABLES:$(shell tput sgr0)"
	@echo "  $(shell tput setaf 2)PROJECT_DIR$(shell tput sgr0)     Path to test project (default: tests/fixtures/sample_project)"
	@echo "  $(shell tput setaf 2)MODELS$(shell tput sgr0)          Model selector for ff-run-select"
	@echo "  $(shell tput setaf 2)TARGET$(shell tput sgr0)          Target platform for build-target"

# Usage in targets:
.PHONY: build
build: ## DEV: Build all crates in the workspace
	cargo build --workspace

.PHONY: test
test: ## TEST: Run all tests with default settings
	cargo test --workspace

.PHONY: ff-compile
ff-compile: ## CLI: Compile Jinja templates to SQL
	cargo run -p ff-cli -- --project-dir $(PROJECT_DIR) compile
```

### Help with usage examples

```makefile
.PHONY: help
help: ## Show detailed help with examples
	@echo "Featherflow Development Makefile"
	@echo ""
	@echo "TARGETS:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-25s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "EXAMPLES:"
	@echo "  # Run full dev cycle"
	@echo "  make dev-cycle"
	@echo ""
	@echo "  # Use custom project directory"
	@echo "  make ff-compile PROJECT_DIR=examples/my_project"
	@echo ""
	@echo "  # Run specific models"
	@echo "  make ff-run-select MODELS='+orders +customers'"
	@echo ""
	@echo "  # Build for Linux"
	@echo "  make build-target TARGET=x86_64-unknown-linux-gnu"
```

## Advanced Pattern Rules

### Build system for multiple languages

```makefile
# Pattern rule for C files
build/%.o: src/%.c | build
	@echo "Compiling C: $<"
	$(CC) $(CFLAGS) -c $< -o $@

# Pattern rule for C++ files
build/%.o: src/%.cpp | build
	@echo "Compiling C++: $<"
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Pattern rule for Rust
build/%.rlib: src/%.rs | build
	@echo "Compiling Rust: $<"
	rustc --crate-type=lib $< --out-dir build

# Multiple output files from one source
build/%.tab.c build/%.tab.h: src/%.y | build
	@echo "Generating parser: $<"
	bison -d $< -o build/$*.tab.c
```

### Dependency tracking with automatic header dependencies

```makefile
# Automatically generate dependency files
sources := $(wildcard src/*.c)
objects := $(sources:src/%.c=build/%.o)
deps := $(objects:.o=.d)

# Include dependency files (- prefix suppresses errors if missing)
-include $(deps)

# Compile and generate dependencies simultaneously
build/%.o: src/%.c | build
	$(CC) -MMD -MP $(CFLAGS) -c $< -o $@
	# -MMD generates .d file alongside .o
	# -MP adds phony targets for headers to avoid errors when headers are deleted

.PHONY: clean
clean:
	rm -rf build

build:
	mkdir -p build
```

## Complex Workflows

### Parallel execution with synchronization

```makefile
# Safe parallel builds
.PHONY: all
all: lint test build docs ## Build everything in optimal order

# These can run in parallel (no dependencies)
.PHONY: lint test
lint: fmt-check clippy
test: test-unit test-integration

# These must run sequentially
.PHONY: build
build: $(objects)
	$(CC) $(objects) -o myapp

# Integration tests require the binary to be built
.PHONY: test-integration
test-integration: build
	./tests/integration_test.sh

# Run with: make -j4 all
```

### Multi-stage deployment workflow

```makefile
.PHONY: deploy-staging
deploy-staging: check-aws-creds build-release run-migrations deploy-to-s3 ## Deploy to staging environment
	@echo "✓ Deployed to staging"
	@echo "URL: https://staging.example.com"

.PHONY: check-aws-creds
check-aws-creds:
	@test -n "$(AWS_ACCESS_KEY_ID)" || (echo "Error: AWS_ACCESS_KEY_ID not set" && exit 1)
	@test -n "$(AWS_SECRET_ACCESS_KEY)" || (echo "Error: AWS_SECRET_ACCESS_KEY not set" && exit 1)
	@echo "✓ AWS credentials configured"

.PHONY: build-release
build-release:
	cargo build --release
	strip target/release/myapp
	@echo "✓ Release binary built and stripped"

.PHONY: run-migrations
run-migrations:
	./scripts/run_migrations.sh staging
	@echo "✓ Database migrations applied"

.PHONY: deploy-to-s3
deploy-to-s3:
	aws s3 cp target/release/myapp s3://releases/staging/$(VERSION)/
	aws s3 cp target/release/myapp s3://releases/staging/latest/
	@echo "✓ Binary uploaded to S3"
```

## Testing Patterns

### Test target with coverage

```makefile
.PHONY: test-coverage
test-coverage: ## Run tests with coverage report
	@echo "Running tests with coverage..."
	cargo tarpaulin --out Html --out Lcov --output-dir coverage
	@echo "✓ Coverage report generated in coverage/"
	@echo "  Open coverage/index.html to view"

.PHONY: test-with-retry
test-with-retry: ## Run flaky tests with retry logic
	@max_attempts=3; \
	attempt=1; \
	while [ $$attempt -le $$max_attempts ]; do \
		echo "Test attempt $$attempt of $$max_attempts"; \
		if cargo test --test integration_tests; then \
			echo "✓ Tests passed"; \
			exit 0; \
		fi; \
		attempt=$$((attempt + 1)); \
		if [ $$attempt -le $$max_attempts ]; then \
			echo "Retrying in 5 seconds..."; \
			sleep 5; \
		fi; \
	done; \
	echo "✗ Tests failed after $$max_attempts attempts"; \
	exit 1
```

### Property-based testing workflow

```makefile
.PHONY: test-quick
test-quick: ## Run tests with minimal iterations (fast feedback)
	PROPTEST_CASES=10 cargo test

.PHONY: test-thorough
test-thorough: ## Run tests with many iterations (thorough)
	PROPTEST_CASES=10000 cargo test

.PHONY: test-deterministic
test-deterministic: ## Run tests with fixed seed (reproducible)
	PROPTEST_RNG_SEED=12345 cargo test
```

## Documentation Generation

### Auto-generate docs from multiple sources

```makefile
.PHONY: docs
docs: docs-api docs-user docs-arch ## Generate all documentation

.PHONY: docs-api
docs-api: ## Generate API documentation
	cargo doc --no-deps --workspace
	@echo "✓ API docs: target/doc/index.html"

.PHONY: docs-user
docs-user: ## Generate user documentation from markdown
	mdbook build docs/user-guide
	@echo "✓ User guide: docs/user-guide/book/index.html"

.PHONY: docs-arch
docs-arch: ## Generate architecture diagrams
	@echo "Generating architecture diagrams..."
	plantuml -o ../generated docs/architecture/*.puml
	@echo "✓ Architecture diagrams: docs/generated/"

.PHONY: docs-serve
docs-serve: docs ## Serve documentation locally
	@echo "Starting documentation server..."
	@echo "API docs:  http://localhost:8000/api/"
	@echo "User guide: http://localhost:8000/user-guide/"
	python3 -m http.server --directory target/doc 8000
```

## Release Management

### Semantic versioning and changelog

```makefile
.PHONY: bump-patch
bump-patch: ## Bump patch version (0.1.0 -> 0.1.1)
	@current=$$(cargo pkgid | cut -d\# -f2 | cut -d: -f2); \
	IFS='.' read -ra VERSION_PARTS <<< "$$current"; \
	new_patch=$$((VERSION_PARTS[2] + 1)); \
	new_version="$${VERSION_PARTS[0]}.$${VERSION_PARTS[1]}.$$new_patch"; \
	echo "Bumping version: $$current -> $$new_version"; \
	sed -i.bak "s/version = \"$$current\"/version = \"$$new_version\"/" Cargo.toml; \
	rm Cargo.toml.bak; \
	cargo check --quiet; \
	git add Cargo.toml Cargo.lock; \
	git commit -m "chore: bump version to $$new_version"; \
	git tag "v$$new_version"; \
	echo "✓ Version bumped to $$new_version"

.PHONY: bump-minor
bump-minor: ## Bump minor version (0.1.0 -> 0.2.0)
	@current=$$(cargo pkgid | cut -d\# -f2 | cut -d: -f2); \
	IFS='.' read -ra VERSION_PARTS <<< "$$current"; \
	new_minor=$$((VERSION_PARTS[1] + 1)); \
	new_version="$${VERSION_PARTS[0]}.$$new_minor.0"; \
	echo "Bumping version: $$current -> $$new_version"; \
	sed -i.bak "s/version = \"$$current\"/version = \"$$new_version\"/" Cargo.toml; \
	rm Cargo.toml.bak; \
	cargo check --quiet; \
	git add Cargo.toml Cargo.lock; \
	git commit -m "chore: bump version to $$new_version"; \
	git tag "v$$new_version"; \
	echo "✓ Version bumped to $$new_version"

.PHONY: changelog
changelog: ## Generate changelog from git history
	@echo "Generating CHANGELOG.md..."
	@echo "# Changelog" > CHANGELOG.md
	@echo "" >> CHANGELOG.md
	@git tag -l | tac | while read tag; do \
		echo "## $$tag" >> CHANGELOG.md; \
		git log $$tag --pretty=format:"- %s" --no-merges >> CHANGELOG.md || true; \
		echo "" >> CHANGELOG.md; \
		echo "" >> CHANGELOG.md; \
	done
	@echo "✓ CHANGELOG.md generated"
```

### Cross-platform builds

```makefile
# Supported targets
TARGETS := \
	x86_64-unknown-linux-gnu \
	x86_64-unknown-linux-musl \
	x86_64-apple-darwin \
	aarch64-apple-darwin \
	x86_64-pc-windows-gnu

.PHONY: build-all-targets
build-all-targets: $(addprefix build-,$(TARGETS)) ## Build for all platforms

.PHONY: build-%
build-: ## Build for specific target (auto-generated)
	@target=$*; \
	echo "Building for $$target..."; \
	cargo build --release --target $$target; \
	echo "✓ Built for $$target"

.PHONY: package-releases
package-releases: build-all-targets ## Package all release artifacts
	@mkdir -p dist
	@for target in $(TARGETS); do \
		echo "Packaging $$target..."; \
		tar czf dist/myapp-$$target.tar.gz \
			-C target/$$target/release \
			myapp$(if $(findstring windows,$$target),.exe,); \
		sha256sum dist/myapp-$$target.tar.gz > dist/myapp-$$target.tar.gz.sha256; \
	done
	@echo "✓ All releases packaged in dist/"
```

## Database Management

### Database migration workflow

```makefile
DATABASE_URL ?= postgres://localhost/myapp_dev

.PHONY: db-create
db-create: ## Create database
	createdb $(notdir $(DATABASE_URL))
	@echo "✓ Database created"

.PHONY: db-migrate
db-migrate: ## Run pending migrations
	sqlx migrate run --database-url $(DATABASE_URL)
	@echo "✓ Migrations applied"

.PHONY: db-rollback
db-rollback: ## Rollback last migration
	@read -p "Rollback last migration? [y/N] " confirm; \
	if [ "$$confirm" = "y" ]; then \
		sqlx migrate revert --database-url $(DATABASE_URL); \
		echo "✓ Migration rolled back"; \
	else \
		echo "Cancelled"; \
	fi

.PHONY: db-reset
db-reset: db-drop db-create db-migrate db-seed ## Reset database completely
	@echo "✓ Database reset complete"

.PHONY: db-seed
db-seed: ## Seed database with test data
	cargo run --bin seed_data -- --database-url $(DATABASE_URL)
	@echo "✓ Database seeded"

.PHONY: db-status
db-status: ## Show migration status
	sqlx migrate info --database-url $(DATABASE_URL)
```

## Performance Analysis

### Benchmarking workflow

```makefile
.PHONY: bench
bench: ## Run benchmarks
	cargo bench --workspace

.PHONY: bench-baseline
bench-baseline: ## Save benchmark baseline
	cargo bench --workspace -- --save-baseline main
	@echo "✓ Baseline saved as 'main'"

.PHONY: bench-compare
bench-compare: ## Compare against baseline
	cargo bench --workspace -- --baseline main
	@echo "✓ Comparison complete"

.PHONY: flamegraph
flamegraph: ## Generate flamegraph
	@command -v cargo-flamegraph >/dev/null 2>&1 || \
		(echo "Installing cargo-flamegraph..." && cargo install flamegraph)
	cargo flamegraph --bin myapp -- --benchmark-mode
	@echo "✓ Flamegraph generated: flamegraph.svg"

.PHONY: profile
profile: ## Profile application with perf
	cargo build --release --bin myapp
	perf record --call-graph=dwarf target/release/myapp
	perf report
```

## Conditional Logic and Guards

### Environment-based behavior

```makefile
# Detect OS
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
    OS := linux
    OPEN := xdg-open
endif
ifeq ($(UNAME_S),Darwin)
    OS := macos
    OPEN := open
endif

# Detect CI environment
ifdef CI
    CARGO_FLAGS := --color=always --locked
    TEST_FLAGS := -- --nocapture
else
    CARGO_FLAGS := 
    TEST_FLAGS :=
endif

.PHONY: test
test:
	cargo test $(CARGO_FLAGS) $(TEST_FLAGS)

.PHONY: open-docs
open-docs: docs
	$(OPEN) target/doc/myapp/index.html
```

### Required environment variables

```makefile
# Guard against missing environment variables
guard-%:
	@if [ -z "$(${*})" ]; then \
		echo "Error: $* is not set"; \
		echo ""; \
		echo "Usage:"; \
		echo "  export $*=<value>"; \
		echo "  make [target]"; \
		exit 1; \
	fi

# Usage in targets
.PHONY: deploy-production
deploy-production: guard-AWS_ACCESS_KEY_ID guard-AWS_SECRET_ACCESS_KEY
	@echo "Deploying to production..."
	# Deployment commands here
```

## Debugging and Introspection

### Debug helpers

```makefile
# Inspect any variable
.PHONY: print-%
print-%:
	@echo '$*=$($*)'
	@echo "  origin: $(origin $*)"
	@echo "  flavor: $(flavor $*)"

# Example usage:
# make print-PROJECT_DIR
# make print-MAKEFLAGS

.PHONY: debug-targets
debug-targets: ## List all targets
	@$(MAKE) -pRrq -f $(firstword $(MAKEFILE_LIST)) : 2>/dev/null | \
		awk -v RS= -F: '/^# File/,/^# Finished Make data base/ {if ($$1 !~ "^[#.]") {print $$1}}' | \
		sort | grep -v -e '^[^[:alnum:]]' -e '^$@$$'

.PHONY: debug-phony
debug-phony: ## List all PHONY targets
	@grep -E '^\.PHONY:' $(MAKEFILE_LIST) | \
		cut -d: -f2 | \
		tr ' ' '\n' | \
		sort -u

.PHONY: debug-vars
debug-vars: ## Show all user-defined variables
	@echo "Environment variables:"
	@$(foreach var,$(filter-out .% _%,$(sort $(.VARIABLES))),$(if $(filter file,$(origin $(var))),@echo "  $(var) = $(value $(var))",))
```

These advanced examples demonstrate production-ready patterns for complex Makefile scenarios while maintaining the core principles of clarity, maintainability, and self-documentation.
