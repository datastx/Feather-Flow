# Feather-Flow: Release & Distribution Plan

## Goals

1. **Linux CLI binary** — a statically-linked binary any Linux box can `curl` and run
2. **Docker image** — published to GitHub Container Registry (ghcr.io)
3. **Patch versioning** — `0.1.PATCH`, bumped manually via `make release`
4. **Makefile-driven** — all build, version, and packaging commands live in the Makefile; GitHub Actions only calls `make` targets

---

## How to Release

One command:

```bash
make release
```

This does everything:
1. Bumps the patch version in `Cargo.toml` (e.g. `0.1.0` → `0.1.1`)
2. Updates `Cargo.lock`
3. Commits: `release: v0.1.1`
4. Creates annotated tag: `v0.1.1`
5. Pushes commit + tag to `origin main`
6. The tag push triggers the `release.yml` workflow which builds binaries, creates a GitHub Release, and pushes a Docker image

No PATs, no bot tokens, no auto-release workflows. You decide when to release.

To set an explicit version (e.g. jumping to `0.2.0`):

```bash
make version-set NEW_VERSION=0.2.0
# then commit, tag, push manually — or just run `make release` next time
```

---

## Versioning

### Scheme: `0.1.PATCH`

Major and minor stay at `0.1` while the project is nascent. Only the patch increments via `make release`.

### Source of truth

- `Cargo.toml` workspace `version` field — all crates inherit via `version.workspace = true`
- Git tags (`v0.1.X`) trigger the release pipeline
- `ff --version` prints the version automatically (clap derives it from `Cargo.toml`)

### Makefile targets

| Target | Description |
|---|---|
| `make version` | Print current version |
| `make version-bump-patch` | Bump patch in Cargo.toml + Cargo.lock |
| `make version-set NEW_VERSION=X.Y.Z` | Set an explicit version |
| `make version-tag` | Create annotated git tag |
| `make release` | Bump + commit + tag + push (one command) |

---

## Linux Binary

A **static musl-linked binary** — zero runtime dependencies, runs on any Linux distro.

Built by `make build-linux` in CI. Users download it with:

```bash
curl -fsSL https://github.com/datastx/Feather-Flow/releases/latest/download/ff-x86_64-linux-musl -o ff
chmod +x ff
sudo mv ff /usr/local/bin/
```

Checksum verification:

```bash
curl -fsSL https://github.com/datastx/Feather-Flow/releases/latest/download/ff-x86_64-linux-musl.sha256 -o ff.sha256
sha256sum -c ff.sha256
```

### Risk: musl + DuckDB bundled

DuckDB compiles C++ from source via its `bundled` feature. Musl cross-compilation of C++ is a known pain point. The release workflow has never been triggered (zero tags exist), so this is untested. If it fails in CI, the fallback is switching to the `gnu` target (works on any modern Linux with glibc 2.17+).

---

## Docker Image

### Image: `ghcr.io/datastx/feather-flow`

Multi-stage Dockerfile:
- **Stage 1 (planner)**: copies only `Cargo.toml`/`Cargo.lock` + stub sources, builds dependencies. This layer caches until deps change.
- **Stage 2 (builder)**: copies real source, builds the musl binary.
- **Stage 3 (runtime)**: Alpine 3.19, non-root `ff` user, OCI labels. Final image ~15-20 MB.

Tagged with both the version (`0.1.1`) and `latest`.

### Makefile targets

| Target | Description |
|---|---|
| `make docker-build` | Build image locally |
| `make docker-push` | Push to ghcr.io (requires `make docker-login` first) |
| `make docker-login` | Auth to GHCR (CI sets `GITHUB_TOKEN` + `GITHUB_ACTOR`) |
| `make docker-run CMD=validate` | Run ff in Docker against current directory |

---

## CI/CD Workflows

### `ci.yml` — unchanged

Runs on every push/PR: `check`, `fmt`, `clippy`, `test`, `docs`. Already delegates to `make`.

### `release.yml` — triggered by `v*.*.*` tags

```
Tag pushed (e.g. v0.1.1)
    │
    ├─→ build-linux (ubuntu)     make build-linux + verify-binary
    │
    ├─→ build-macos-x86 (macos-13)
    │
    ├─→ build-macos-arm (macos-latest)
    │
    └─→ release (after all builds)
         ├─ make create-release TAG=v0.1.1   (gh CLI)
         ├─ make docker-login
         ├─ make docker-build
         └─ make docker-push
```

Permissions: `contents: write` (release), `packages: write` (GHCR).

All build/release logic goes through `make` targets. No off-the-shelf release actions — uses `gh release create` via the Makefile.

---

## Files

| File | Status | Purpose |
|---|---|---|
| `Makefile` | Modified | Version, build, docker, release targets |
| `Dockerfile` | New | Multi-stage build, dep caching, non-root user |
| `.dockerignore` | New | Exclude target/, .git/, tests/ from build context |
| `.gitignore` | Modified | Added `dist/` |
| `.github/workflows/release.yml` | Rewritten | Makefile-driven, gh CLI, Docker push |
| `README.md` | Modified | Installation: binary download, Docker, source |

---

## Future: Automated Releases

When the project matures and frequent releases become a burden, add an `auto-release.yml` workflow that:

1. Triggers on push to `main`
2. Runs `make version-bump-patch`
3. Commits with `[skip-release]` in the message (to prevent infinite loops)
4. Creates a tag and pushes it (triggering `release.yml`)

This requires either a **GitHub App token** or a **Personal Access Token** stored as `RELEASE_PAT`, because `GITHUB_TOKEN` pushes cannot trigger other workflows (GitHub security feature). Additionally, the workflow needs dual infinite-loop guards: commit message check (`[skip-release]`) AND committer identity check (`github-actions[bot]`).

This is not implemented today because the overhead of managing tokens is not worth it for a nascent project with infrequent releases. `make release` is simpler and more transparent.
