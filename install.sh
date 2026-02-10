#!/usr/bin/env bash
set -euo pipefail

# Feather-Flow installer
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/datastx/Feather-Flow/main/install.sh | bash
#   FF_VERSION=0.1.0 ... | bash          # pin a version
#   INSTALL_DIR=/usr/local/bin ... | bash # custom install directory

REPO="datastx/Feather-Flow"
BINARY_NAME="ff"
INSTALL_DIR="${INSTALL_DIR:-$HOME/.local/bin}"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

info()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
error() { printf '\033[1;31merror:\033[0m %s\n' "$*" >&2; exit 1; }

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    error "need '$1' (command not found)"
  fi
}

# ---------------------------------------------------------------------------
# Detect platform
# ---------------------------------------------------------------------------

detect_platform() {
  local os arch

  os="$(uname -s)"
  arch="$(uname -m)"

  case "$os" in
    Linux)  os="linux-gnu" ;;
    Darwin) os="apple-darwin" ;;
    *)      error "unsupported OS: $os" ;;
  esac

  case "$arch" in
    x86_64)         arch="x86_64" ;;
    aarch64|arm64)  arch="aarch64" ;;
    *)              error "unsupported architecture: $arch" ;;
  esac

  # No Linux ARM build yet
  if [ "$os" = "linux-gnu" ] && [ "$arch" = "aarch64" ]; then
    error "linux/aarch64 builds are not available yet"
  fi

  PLATFORM="${arch}-${os}"
}

# ---------------------------------------------------------------------------
# Resolve version
# ---------------------------------------------------------------------------

resolve_version() {
  if [ -n "${FF_VERSION:-}" ]; then
    VERSION="$FF_VERSION"
  else
    info "fetching latest release..."
    VERSION="$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
      | grep '"tag_name"' \
      | sed -E 's/.*"tag_name":\s*"v?([^"]+)".*/\1/')"
    if [ -z "$VERSION" ]; then
      error "could not determine latest version — set FF_VERSION explicitly"
    fi
  fi
  TAG="v${VERSION}"
}

# ---------------------------------------------------------------------------
# Download + verify + install
# ---------------------------------------------------------------------------

download_and_install() {
  local asset="ff-${PLATFORM}"
  local url="https://github.com/${REPO}/releases/download/${TAG}/${asset}"
  local checksum_url="${url}.sha256"
  local tmpdir

  tmpdir="$(mktemp -d)"
  trap 'rm -rf "$tmpdir"' EXIT

  info "downloading ${BINARY_NAME} ${VERSION} for ${PLATFORM}..."
  curl -fsSL -o "${tmpdir}/${asset}" "$url" \
    || error "download failed — does release ${TAG} have asset '${asset}'?"

  # Verify checksum if available
  if curl -fsSL -o "${tmpdir}/${asset}.sha256" "$checksum_url" 2>/dev/null; then
    info "verifying checksum..."
    local expected actual
    expected="$(awk '{print $1}' "${tmpdir}/${asset}.sha256")"
    if command -v sha256sum >/dev/null 2>&1; then
      actual="$(sha256sum "${tmpdir}/${asset}" | awk '{print $1}')"
    elif command -v shasum >/dev/null 2>&1; then
      actual="$(shasum -a 256 "${tmpdir}/${asset}" | awk '{print $1}')"
    else
      info "no sha256sum or shasum found — skipping checksum verification"
      actual="$expected"
    fi
    if [ "$expected" != "$actual" ]; then
      error "checksum mismatch (expected ${expected}, got ${actual})"
    fi
  else
    info "no checksum file found — skipping verification"
  fi

  # Install
  mkdir -p "$INSTALL_DIR"
  mv "${tmpdir}/${asset}" "${INSTALL_DIR}/${BINARY_NAME}"
  chmod +x "${INSTALL_DIR}/${BINARY_NAME}"

  info "installed ${BINARY_NAME} ${VERSION} to ${INSTALL_DIR}/${BINARY_NAME}"

  # PATH hint
  if ! echo "$PATH" | tr ':' '\n' | grep -qx "$INSTALL_DIR"; then
    printf '\n'
    info "add %s to your PATH:" "$INSTALL_DIR"
    printf '    export PATH="%s:$PATH"\n' "$INSTALL_DIR"
    printf '\n'
  fi
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

main() {
  need_cmd curl
  need_cmd uname
  need_cmd mktemp
  need_cmd chmod

  detect_platform
  resolve_version
  download_and_install

  info "done! Run 'ff --help' to get started."
}

main
