# GitHub Actions Extended Examples

Advanced patterns and complete workflow examples for production CI/CD pipelines.

## Complete CI Pipeline

Comprehensive CI workflow with all best practices:

```yaml
name: CI

on:
  push:
    branches: [main, develop]
  pull_request:
  merge_group:

# Global environment
env:
  CARGO_TERM_COLOR: always
  RUSTFLAGS: -Dwarnings
  RUST_BACKTRACE: 1

# Cancel stale runs
concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true

jobs:
  # Fast feedback - syntax and formatting
  check:
    name: Check and format
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: rustfmt, clippy
      
      - uses: Swatinem/rust-cache@v2
      
      - name: Check code formatting
        run: make fmt-check
      
      - name: Run clippy lints
        run: make clippy

  # Compile check (faster than full build)
  compile:
    name: Compile on ${{ matrix.os }}
    runs-on: ${{ matrix.os }}
    strategy:
      fail-fast: false
      matrix:
        os: [ubuntu-latest, macos-latest, windows-latest]
    steps:
      - uses: actions/checkout@v4
      
      - uses: dtolnay/rust-toolchain@stable
      
      - uses: Swatinem/rust-cache@v2
        with:
          key: ${{ matrix.os }}
      
      - name: Check compilation
        run: make check

  # Full test suite
  test:
    name: Test on ${{ matrix.os }}
    runs-on: ${{ matrix.os }}
    needs: [check, compile]
    strategy:
      fail-fast: false
      matrix:
        os: [ubuntu-latest, macos-latest, windows-latest]
    steps:
      - uses: actions/checkout@v4
      
      - uses: dtolnay/rust-toolchain@stable
      
      - uses: Swatinem/rust-cache@v2
        with:
          key: ${{ matrix.os }}-test
      
      - name: Run test suite
        run: make test
      
      - name: Run integration tests
        run: make test-integration

  # Documentation build
  docs:
    name: Build documentation
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - uses: dtolnay/rust-toolchain@stable
      
      - uses: Swatinem/rust-cache@v2
      
      - name: Generate documentation
        run: make doc
      
      - name: Upload docs artifact
        uses: actions/upload-artifact@v4
        with:
          name: documentation
          path: target/doc

  # Code coverage
  coverage:
    name: Code coverage
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: llvm-tools-preview
      
      - uses: Swatinem/rust-cache@v2
      
      - name: Install coverage tools
        run: make install-coverage
      
      - name: Generate coverage
        run: make coverage
      
      - name: Upload to codecov
        uses: codecov/codecov-action@v3
        with:
          files: ./coverage.lcov
          token: ${{ secrets.CODECOV_TOKEN }}

  # All checks passed gate
  ci-success:
    name: All CI checks passed
    runs-on: ubuntu-latest
    needs: [check, compile, test, docs, coverage]
    steps:
      - run: echo "All checks passed!"
```

## Multi-Platform Release Workflow

Complete release workflow with cross-compilation and asset management:

```yaml
name: Release

on:
  push:
    tags: ['v*.*.*']

env:
  CARGO_TERM_COLOR: always

permissions:
  contents: write  # Needed for creating releases

jobs:
  # Create GitHub Release
  create-release:
    name: Create GitHub Release
    runs-on: ubuntu-latest
    outputs:
      upload_url: ${{ steps.create_release.outputs.upload_url }}
    steps:
      - uses: actions/checkout@v4
      
      - name: Create Release
        id: create_release
        uses: softprops/action-gh-release@v1
        with:
          draft: false
          prerelease: false
          generate_release_notes: true

  # Build for all platforms
  build:
    name: Build ${{ matrix.target }}
    runs-on: ${{ matrix.os }}
    needs: create-release
    strategy:
      fail-fast: false
      matrix:
        include:
          # Linux targets
          - os: ubuntu-latest
            target: x86_64-unknown-linux-gnu
            artifact_name: ff
            asset_name: ff-x86_64-linux-gnu
          
          - os: ubuntu-latest
            target: x86_64-unknown-linux-musl
            artifact_name: ff
            asset_name: ff-x86_64-linux-musl
          
          - os: ubuntu-latest
            target: aarch64-unknown-linux-gnu
            artifact_name: ff
            asset_name: ff-aarch64-linux-gnu
          
          # macOS targets
          - os: macos-latest
            target: x86_64-apple-darwin
            artifact_name: ff
            asset_name: ff-x86_64-apple-darwin
          
          - os: macos-latest
            target: aarch64-apple-darwin
            artifact_name: ff
            asset_name: ff-aarch64-apple-darwin
          
          # Windows targets
          - os: windows-latest
            target: x86_64-pc-windows-msvc
            artifact_name: ff.exe
            asset_name: ff-x86_64-windows-msvc.exe
    
    steps:
      - uses: actions/checkout@v4
      
      - uses: dtolnay/rust-toolchain@stable
        with:
          targets: ${{ matrix.target }}
      
      - uses: Swatinem/rust-cache@v2
        with:
          key: ${{ matrix.target }}
      
      # Install cross-compilation dependencies
      - name: Install Linux cross-compiler
        if: matrix.target == 'aarch64-unknown-linux-gnu'
        run: make install-cross-linker TARGET=${{ matrix.target }}
      
      - name: Install musl tools
        if: matrix.target == 'x86_64-unknown-linux-musl'
        run: make install-musl-tools
      
      # Build the binary
      - name: Build release binary
        run: make build-release TARGET=${{ matrix.target }}
      
      # Package the binary
      - name: Package binary
        run: make package TARGET=${{ matrix.target }}
      
      # Upload to release
      - name: Upload Release Asset
        uses: actions/upload-release-asset@v1
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        with:
          upload_url: ${{ needs.create-release.outputs.upload_url }}
          asset_path: ./dist/${{ matrix.asset_name }}.tar.gz
          asset_name: ${{ matrix.asset_name }}.tar.gz
          asset_content_type: application/gzip

  # Generate checksums
  checksums:
    name: Generate checksums
    runs-on: ubuntu-latest
    needs: build
    steps:
      - uses: actions/checkout@v4
      
      - name: Download all artifacts
        uses: actions/download-artifact@v4
        with:
          path: artifacts
      
      - name: Generate checksums
        run: make checksums ARTIFACTS_DIR=artifacts
      
      - name: Upload checksums
        uses: actions/upload-release-asset@v1
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        with:
          upload_url: ${{ needs.create-release.outputs.upload_url }}
          asset_path: ./checksums.txt
          asset_name: checksums.txt
          asset_content_type: text/plain
```

## Dependency Update Automation

Automated dependency updates with testing:

```yaml
name: Dependency Updates

on:
  schedule:
    - cron: '0 0 * * 1'  # Weekly on Monday
  workflow_dispatch:

permissions:
  contents: write
  pull-requests: write

jobs:
  update-deps:
    name: Update dependencies
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - uses: dtolnay/rust-toolchain@stable
      
      - name: Update dependencies
        run: make update-deps
      
      - name: Run tests
        run: make test
      
      - name: Create Pull Request
        uses: peter-evans/create-pull-request@v5
        with:
          token: ${{ secrets.GITHUB_TOKEN }}
          commit-message: "chore: update dependencies"
          title: "chore: Weekly dependency updates"
          body: |
            Automated dependency updates.
            
            - Updated all dependencies to latest compatible versions
            - All tests pass
          branch: deps/automated-updates
          delete-branch: true
```

## Security Scanning

Comprehensive security checks:

```yaml
name: Security

on:
  push:
    branches: [main]
  pull_request:
  schedule:
    - cron: '0 0 * * 0'  # Weekly

permissions:
  security-events: write
  contents: read

jobs:
  audit:
    name: Security audit
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - uses: dtolnay/rust-toolchain@stable
      
      - name: Install cargo-audit
        run: make install-audit
      
      - name: Run security audit
        run: make audit

  scan:
    name: CodeQL scan
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Initialize CodeQL
        uses: github/codeql-action/init@v2
        with:
          languages: rust
      
      - uses: dtolnay/rust-toolchain@stable
      
      - name: Build for analysis
        run: make build
      
      - name: Perform CodeQL Analysis
        uses: github/codeql-action/analyze@v2

  secrets-scan:
    name: Scan for secrets
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0
      
      - name: Run gitleaks
        uses: gitleaks/gitleaks-action@v2
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

## Deployment Workflow

Staged deployment with approval gates:

```yaml
name: Deploy

on:
  push:
    tags: ['v*.*.*']
  workflow_dispatch:
    inputs:
      environment:
        description: 'Environment to deploy to'
        required: true
        type: choice
        options:
          - staging
          - production

permissions:
  contents: read
  id-token: write

jobs:
  # Deploy to staging
  deploy-staging:
    name: Deploy to staging
    runs-on: ubuntu-latest
    environment:
      name: staging
      url: https://staging.example.com
    steps:
      - uses: actions/checkout@v4
      
      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ secrets.AWS_ROLE_STAGING }}
          aws-region: us-east-1
      
      - name: Deploy to staging
        run: make deploy ENV=staging
      
      - name: Run smoke tests
        run: make test-smoke URL=https://staging.example.com

  # Deploy to production (requires approval)
  deploy-production:
    name: Deploy to production
    runs-on: ubuntu-latest
    needs: deploy-staging
    if: github.event_name == 'push' || github.event.inputs.environment == 'production'
    environment:
      name: production
      url: https://example.com
    steps:
      - uses: actions/checkout@v4
      
      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ secrets.AWS_ROLE_PRODUCTION }}
          aws-region: us-east-1
      
      - name: Deploy to production
        run: make deploy ENV=production
      
      - name: Run smoke tests
        run: make test-smoke URL=https://example.com
      
      - name: Notify deployment
        run: make notify-deployment ENV=production VERSION=${{ github.ref_name }}
```

## Performance Benchmarking

Automated performance regression detection:

```yaml
name: Benchmarks

on:
  push:
    branches: [main]
  pull_request:

jobs:
  benchmark:
    name: Run benchmarks
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - uses: dtolnay/rust-toolchain@stable
      
      - uses: Swatinem/rust-cache@v2
      
      - name: Run benchmarks
        run: make bench
      
      - name: Store benchmark results
        uses: benchmark-action/github-action-benchmark@v1
        with:
          tool: 'cargo'
          output-file-path: target/criterion/output.json
          github-token: ${{ secrets.GITHUB_TOKEN }}
          auto-push: true
          alert-threshold: '150%'
          comment-on-alert: true
          fail-on-alert: false
```

## Container Build and Publish

Docker image build and registry push:

```yaml
name: Docker

on:
  push:
    branches: [main]
    tags: ['v*.*.*']
  pull_request:

env:
  REGISTRY: ghcr.io
  IMAGE_NAME: ${{ github.repository }}

permissions:
  contents: read
  packages: write

jobs:
  build-and-push:
    name: Build and push Docker image
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Docker Buildx
        uses: docker/setup-buildx-action@v3
      
      - name: Log in to registry
        if: github.event_name != 'pull_request'
        uses: docker/login-action@v3
        with:
          registry: ${{ env.REGISTRY }}
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}
      
      - name: Extract metadata
        id: meta
        uses: docker/metadata-action@v5
        with:
          images: ${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}
          tags: |
            type=semver,pattern={{version}}
            type=semver,pattern={{major}}.{{minor}}
            type=semver,pattern={{major}}
            type=ref,event=branch
            type=sha
      
      - name: Build and push
        uses: docker/build-push-action@v5
        with:
          context: .
          push: ${{ github.event_name != 'pull_request' }}
          tags: ${{ steps.meta.outputs.tags }}
          labels: ${{ steps.meta.outputs.labels }}
          cache-from: type=gha
          cache-to: type=gha,mode=max
          build-args: |
            BUILDKIT_INLINE_CACHE=1
```

## Reusable Workflow

Create reusable workflow components:

```yaml
# .github/workflows/test-template.yml
name: Test Template

on:
  workflow_call:
    inputs:
      os:
        required: true
        type: string
      rust-version:
        required: false
        type: string
        default: 'stable'
    secrets:
      codecov-token:
        required: false

jobs:
  test:
    name: Test on ${{ inputs.os }}
    runs-on: ${{ inputs.os }}
    steps:
      - uses: actions/checkout@v4
      
      - uses: dtolnay/rust-toolchain@master
        with:
          toolchain: ${{ inputs.rust-version }}
      
      - uses: Swatinem/rust-cache@v2
      
      - name: Run tests
        run: make test
      
      - name: Upload coverage
        if: inputs.codecov-token != ''
        uses: codecov/codecov-action@v3
        with:
          token: ${{ secrets.codecov-token }}
```

Usage in main workflow:

```yaml
# .github/workflows/ci.yml
name: CI

on: [push, pull_request]

jobs:
  test-linux:
    uses: ./.github/workflows/test-template.yml
    with:
      os: ubuntu-latest
    secrets:
      codecov-token: ${{ secrets.CODECOV_TOKEN }}
  
  test-macos:
    uses: ./.github/workflows/test-template.yml
    with:
      os: macos-latest
```

## Advanced Patterns

### Conditional Job Execution

```yaml
jobs:
  deploy:
    if: github.ref == 'refs/heads/main' && github.event_name == 'push'
    runs-on: ubuntu-latest
    steps:
      - run: make deploy
```

### Dynamic Matrix from File

```yaml
jobs:
  setup:
    runs-on: ubuntu-latest
    outputs:
      matrix: ${{ steps.set-matrix.outputs.matrix }}
    steps:
      - uses: actions/checkout@v4
      - id: set-matrix
        run: echo "matrix=$(make print-matrix-json)" >> $GITHUB_OUTPUT
  
  test:
    needs: setup
    strategy:
      matrix: ${{ fromJson(needs.setup.outputs.matrix) }}
    runs-on: ubuntu-latest
    steps:
      - run: make test TARGET=${{ matrix.target }}
```

### Job Outputs and Dependencies

```yaml
jobs:
  build:
    runs-on: ubuntu-latest
    outputs:
      version: ${{ steps.version.outputs.version }}
    steps:
      - id: version
        run: echo "version=$(make print-version)" >> $GITHUB_OUTPUT
  
  deploy:
    needs: build
    runs-on: ubuntu-latest
    steps:
      - run: make deploy VERSION=${{ needs.build.outputs.version }}
```

### Composite Actions

Create reusable action in `.github/actions/setup/action.yml`:

```yaml
name: 'Setup Build Environment'
description: 'Sets up Rust and caches dependencies'
inputs:
  rust-version:
    description: 'Rust version'
    default: 'stable'
runs:
  using: 'composite'
  steps:
    - uses: dtolnay/rust-toolchain@master
      with:
        toolchain: ${{ inputs.rust-version }}
    
    - uses: Swatinem/rust-cache@v2
      with:
        cache-on-failure: true
```

Usage:

```yaml
jobs:
  build:
    steps:
      - uses: actions/checkout@v4
      - uses: ./.github/actions/setup
      - run: make build
```

These examples demonstrate production-ready GitHub Actions workflows that follow all best practices while maintaining the core principle of delegating execution to Makefile targets.
