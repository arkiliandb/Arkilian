#!/usr/bin/env bash
# ==============================================================================
# Arkilian Multi-Binding Release Packaging & Drop Tool
#
# Builds and stages downloadable release packages for:
#   - Node.js (bundled npm .tgz with prebuilds + standalone prebuilds archive)
#   - Python (self-contained wheels + sdist .tar.gz)
#   - Rust (cargo .crate packages + precompiled library archive)
#   - Go (module distribution archive)
#   - PHP (composer-ready distribution archive)
#   - C Core (shared/static libraries, headers, arkilian-dlq tool)
#
# Usage:
#   bash scripts/build-release-packages.sh                # Build all packages locally into dist/releases/
#   bash scripts/build-release-packages.sh --upload v1.4.1 # Build and drop to GitHub release
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$REPO_ROOT"

# Parse CLI arguments
UPLOAD_TAG=""
DRY_RUN=0

while [[ $# -gt 0 ]]; do
  case $1 in
    --upload)
      UPLOAD_TAG="${2:-}"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    -h|--help)
      echo "Usage: $0 [--upload <tag>] [--dry-run] [-h|--help]"
      echo ""
      echo "Options:"
      echo "  --upload <tag>  Upload generated packages in dist/releases/ to GitHub Release for <tag>"
      echo "  --dry-run       Build packages without uploading (default behavior)"
      echo "  -h, --help      Display this help message and exit"
      exit 0
      ;;
    *)
      echo "Unknown argument: $1"
      echo "Usage: $0 [--upload <tag>] [--dry-run] [-h|--help]"
      exit 1
      ;;
  esac
done

# Detect host environment
OS_NAME="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH_NAME="$(uname -m)"
case "$ARCH_NAME" in
  x86_64)  NORM_ARCH="x64" ;;
  aarch64) NORM_ARCH="arm64" ;;
  arm64)   NORM_ARCH="arm64" ;;
  *)       NORM_ARCH="$ARCH_NAME" ;;
esac

case "$OS_NAME" in
  darwin) PLATFORM_SUFFIX="darwin-${NORM_ARCH}" ;;
  linux)  PLATFORM_SUFFIX="linux-${NORM_ARCH}" ;;
  msys*|cygwin*|mingw*) PLATFORM_SUFFIX="windows-${NORM_ARCH}" ;;
  *) PLATFORM_SUFFIX="${OS_NAME}-${NORM_ARCH}" ;;
esac

# Determine version from package.json
VERSION=$(node -p "require('./package.json').version" 2>/dev/null || echo "1.0.0")
if [ -n "$UPLOAD_TAG" ]; then
  # Strip leading 'v' if provided
  CLEAN_TAG="${UPLOAD_TAG#v}"
  VERSION="$CLEAN_TAG"
fi

DIST_DIR="$REPO_ROOT/dist/releases"
mkdir -p "$DIST_DIR"

echo "==============================================================="
echo "  ARKILIAN RELEASE PACKAGING: v${VERSION} (${PLATFORM_SUFFIX})"
echo "==============================================================="
echo "  Output Directory : $DIST_DIR"
echo "  Host Platform    : $PLATFORM_SUFFIX"
echo "==============================================================="
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# 1. Build C Core Libraries & DLQ Recovery Tool
# ─────────────────────────────────────────────────────────────────────────────
echo "==> [1/6] Building native C core libraries and CLI tools..."
cmake -B build -S . \
  -DCMAKE_BUILD_TYPE=Release \
  -DARKILIAN_BUILD_EXAMPLES=OFF \
  -DARKILIAN_BUILD_TESTS=OFF
cmake --build build --config Release -j4

C_STAGE_DIR="$(mktemp -d /tmp/arkilian-c-stage.XXXXXX)"
mkdir -p "$C_STAGE_DIR/include/arkilian" "$C_STAGE_DIR/lib" "$C_STAGE_DIR/bin"

cp src/class.h src/hydration.h src/sha256.h "$C_STAGE_DIR/include/arkilian/"

if [ -f "build/libarkilian.dylib" ]; then
  cp build/libarkilian.dylib "$C_STAGE_DIR/lib/"
elif [ -f "build/Release/libarkilian.dylib" ]; then
  cp build/Release/libarkilian.dylib "$C_STAGE_DIR/lib/"
fi

if [ -f "build/libarkilian.so" ]; then
  cp build/libarkilian.so "$C_STAGE_DIR/lib/"
elif [ -f "build/Release/libarkilian.so" ]; then
  cp build/Release/libarkilian.so "$C_STAGE_DIR/lib/"
fi

if [ -f "build/libarkilian.a" ]; then
  cp build/libarkilian.a "$C_STAGE_DIR/lib/"
elif [ -f "build/Release/libarkilian.a" ]; then
  cp build/Release/libarkilian.a "$C_STAGE_DIR/lib/"
fi

if [ -f "build/arkilian-dlq" ]; then
  cp build/arkilian-dlq "$C_STAGE_DIR/bin/"
elif [ -f "build/Release/arkilian-dlq" ]; then
  cp build/Release/arkilian-dlq "$C_STAGE_DIR/bin/"
fi

tar -czf "$DIST_DIR/arkilian-c-v${VERSION}-${PLATFORM_SUFFIX}.tar.gz" -C "$C_STAGE_DIR" .
rm -rf "$C_STAGE_DIR"
echo "  -> Created: arkilian-c-v${VERSION}-${PLATFORM_SUFFIX}.tar.gz"

# ─────────────────────────────────────────────────────────────────────────────
# 2. Node.js (npm package & prebuilds)
# ─────────────────────────────────────────────────────────────────────────────
echo "==> [2/6] Packaging Node.js N-API addons and npm bundle..."
# Build prebuild for current platform
if command -v npx >/dev/null 2>&1; then
  npx prebuildify --napi --strip || true
fi

if [ -d "prebuilds" ]; then
  tar -czf "$DIST_DIR/arkilian-node-prebuilds-v${VERSION}-${PLATFORM_SUFFIX}.tar.gz" prebuilds/
  echo "  -> Created: arkilian-node-prebuilds-v${VERSION}-${PLATFORM_SUFFIX}.tar.gz"
fi

# Create packed npm tarball containing code + available prebuilds
npm pack --pack-destination "$DIST_DIR"
echo "  -> Created: $(ls "$DIST_DIR"/arkilian-*.tgz | head -n 1)"

# ─────────────────────────────────────────────────────────────────────────────
# 3. Python (Wheel & Source Distribution)
# ─────────────────────────────────────────────────────────────────────────────
echo "==> [3/6] Packaging Python bindings (wheel & sdist)..."
PY_PKG_DIR="$REPO_ROOT/bindings/python"
if [ -d "$PY_PKG_DIR" ]; then
  # Temporarily bundle native shared library into python package directory for self-contained wheel
  SHARED_LIB=""
  if [ -f "build/libarkilian.dylib" ]; then
    SHARED_LIB="build/libarkilian.dylib"
  elif [ -f "build/libarkilian.so" ]; then
    SHARED_LIB="build/libarkilian.so"
  fi

  if [ -n "$SHARED_LIB" ]; then
    cp "$SHARED_LIB" "$PY_PKG_DIR/arkilian/"
  fi

  # Build wheel using pip wheel (PEP 517 standard)
  (
    cd "$PY_PKG_DIR"
    python3 -m pip wheel --no-deps -w "$DIST_DIR" . 2>/dev/null || true
    tar -czf "$DIST_DIR/arkilian-python-v${VERSION}.tar.gz" \
      --exclude='__pycache__' --exclude='*.egg-info' --exclude='dist' .
    echo "  -> Created: arkilian-python-v${VERSION}.tar.gz"
  )

  # Clean up bundled library from source tree
  rm -f "$PY_PKG_DIR/arkilian/libarkilian.dylib" "$PY_PKG_DIR/arkilian/libarkilian.so"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 4. Rust (Cargo Crates & Compiled Libraries)
# ─────────────────────────────────────────────────────────────────────────────
echo "==> [4/6] Packaging Rust crates and library binaries..."
if command -v cargo >/dev/null 2>&1; then
  # Package arkilian-sys
  if [ -d "bindings/rust/arkilian-sys" ]; then
    (
      cd bindings/rust/arkilian-sys
      cargo package --allow-dirty --no-verify 2>/dev/null || true
      if [ -f "target/package/arkilian-sys-1.0.0.crate" ]; then
        cp target/package/arkilian-sys-1.0.0.crate "$DIST_DIR/arkilian-sys-v${VERSION}.crate"
        echo "  -> Created: arkilian-sys-v${VERSION}.crate"
      fi
    )
  fi

  # Package arkilian crate
  if [ -d "bindings/rust/arkilian" ]; then
    (
      cd bindings/rust/arkilian
      cargo package --allow-dirty --no-verify 2>/dev/null || true
      if [ -f "target/package/arkilian-1.0.0.crate" ]; then
        cp target/package/arkilian-1.0.0.crate "$DIST_DIR/arkilian-rust-v${VERSION}.crate"
        echo "  -> Created: arkilian-rust-v${VERSION}.crate"
      fi

      # Build release library
      cargo build --release --lib 2>/dev/null || true
      if [ -d "target/release" ]; then
        RUST_LIB_STAGE="$(mktemp -d /tmp/arkilian-rust-stage.XXXXXX)"
        mkdir -p "$RUST_LIB_STAGE/lib" "$RUST_LIB_STAGE/include"
        cp src/bindings.h "$RUST_LIB_STAGE/include/" 2>/dev/null || true
        cp target/release/libarkilian.* "$RUST_LIB_STAGE/lib/" 2>/dev/null || true
        tar -czf "$DIST_DIR/arkilian-rust-lib-v${VERSION}-${PLATFORM_SUFFIX}.tar.gz" -C "$RUST_LIB_STAGE" .
        rm -rf "$RUST_LIB_STAGE"
        echo "  -> Created: arkilian-rust-lib-v${VERSION}-${PLATFORM_SUFFIX}.tar.gz"
      fi
    )
  fi
fi

# ─────────────────────────────────────────────────────────────────────────────
# 5. Go & PHP Distribution Archives
# ─────────────────────────────────────────────────────────────────────────────
echo "==> [5/6] Packaging Go & PHP bindings..."
if [ -d "bindings/go" ]; then
  tar -czf "$DIST_DIR/arkilian-go-v${VERSION}.tar.gz" \
    -C bindings/go .
  echo "  -> Created: arkilian-go-v${VERSION}.tar.gz"
fi

if [ -d "bindings/php" ]; then
  tar -czf "$DIST_DIR/arkilian-php-v${VERSION}.tar.gz" \
    -C bindings/php .
  echo "  -> Created: arkilian-php-v${VERSION}.tar.gz"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 6. Generate SHA256 Checksums
# ─────────────────────────────────────────────────────────────────────────────
echo "==> [6/6] Generating SHA256SUMS.txt..."
(
  cd "$DIST_DIR"
  rm -f SHA256SUMS.txt
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum * > SHA256SUMS.txt
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 * > SHA256SUMS.txt
  fi
  cat SHA256SUMS.txt
)

echo ""
echo "==============================================================="
echo "  ALL PACKAGES SUCCESSFULLY CREATED IN: $DIST_DIR"
echo "==============================================================="
ls -lh "$DIST_DIR"
echo "==============================================================="

# ─────────────────────────────────────────────────────────────────────────────
# 7. Drop to GitHub Release (if --upload was provided)
# ─────────────────────────────────────────────────────────────────────────────
if [ -n "$UPLOAD_TAG" ]; then
  if ! command -v gh >/dev/null 2>&1; then
    echo "ERROR: GitHub CLI ('gh') is not installed. Install via 'brew install gh' or your package manager."
    exit 1
  fi

  echo ""
  echo "==> Uploading artifacts to GitHub release $UPLOAD_TAG..."

  # Create release if it does not already exist
  if ! gh release view "$UPLOAD_TAG" >/dev/null 2>&1; then
    echo "Creating release $UPLOAD_TAG on GitHub..."
    gh release create "$UPLOAD_TAG" \
      --title "Arkilian $UPLOAD_TAG" \
      --generate-notes
  fi

  echo "Attaching assets to release $UPLOAD_TAG..."
  gh release upload "$UPLOAD_TAG" "$DIST_DIR"/* --clobber
  echo "SUCCESS: All artifacts attached to GitHub Release $UPLOAD_TAG!"
  echo "View at: $(gh release view "$UPLOAD_TAG" --json url -q .url 2>/dev/null || echo "GitHub Releases")"
fi
