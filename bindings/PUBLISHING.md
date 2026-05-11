# Publishing Arkilian Bindings

This document covers how to publish Arkilian to various package managers.

## Table of Contents
- [Node.js/Bun (npm)](#nodejsbun-npm)
- [Python (PyPI)](#python-pypi)
- [Rust (crates.io)](#rust-cratesio)
- [PHP (Packagist)](#php-packagist)

---

## Node.js/Bun (npm)

### Prerequisites
- npm account (`npm login`)
- Node.js 18+ installed
- macOS/Linux (native addons require different build for Windows)

### Build & Publish

```bash
# Navigate to project root
cd /path/to/birth-of-Arkilian

# Clean previous builds
npm run clean

# Build the native addon
npm run build

# Test the build
npm test

# Update version in package.json
# Edit version in package.json or use:
npm version patch  # or minor/major

# Login to npm (if not already)
npm login

# Publish to npm
npm publish
```

### Package Structure
```
arkilian/
├── index.js           # JavaScript API
├── binding.gyp        # Native build config
├── package.json       # npm package config
├── src/
│   ├── class.c        # C library
│   ├── class.h        # C headers
│   ├── arkilian.cc    # N-API bindings
│   └── deps/sqlite/   # SQLite source
└── build/Release/
    └── arkilian.node  # Compiled addon
```

### Post-Publish
The published package will include prebuilt binaries for:
- macOS x64 (darwin-x64)
- macOS ARM64 (darwin-arm64)
- Linux x64 (linux-x64)

Users can install with:
```bash
npm install arkilian
```

---

## Python (PyPI)

### Prerequisites
- PyPI account (https://pypi.org/account/register/)
- twine installed: `pip install twine`
- Python 3.8+

### Build & Publish

```bash
# Navigate to Python bindings
cd /path/to/birth-of-Arkilian/bindings/python

# Install build dependencies
pip3 install -e .

# Or install build tools
pip3 install build twine

# Build the package
python3 -m build

# This creates:
# dist/arkilian-1.0.0-py3-none-any.whl
# dist/arkilian-1.0.0.tar.gz

# Upload to PyPI
twine upload dist/*

# Or test on Test PyPI first:
twine upload --repository testpypi dist/*
```

### Package Structure
```
bindings/python/
├── pyproject.toml          # Package config (PEP 517)
├── README.md               # Package README
├── arkilian/
│   ├── __init__.py         # Python API
│   └── binding.py          # CFFI wrapper
└── dist/                   # Built packages
```

### Configuration (pyproject.toml)
The package is configured to:
- Build with setuptools + cffi
- Support Python 3.8+
- Auto-discover the `arkilian` package

### Post-Publish
Users can install with:
```bash
pip install arkilian
```

### Note on Binary Distribution
The Python package uses CFFI which requires the C library. Options:
1. **Wheel with bundled lib**: Include `libarkilian.dylib`/`.so` in wheel
2. **System library**: Users need to have libarkilian installed
3. **Compile at install**: Add build instructions to pyproject.toml

---

## Rust (crates.io)

### Prerequisites
- crates.io account (https://crates.io/)
- Rust installed: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
- Cargo configured with credentials: `cargo login <token>`

### Build & Publish

```bash
# Navigate to Rust bindings
cd /path/to/birth-of-Arkilian/bindings/rust/arkilian

# Check package compiles
cargo build

# Run tests
cargo test

# Update version in Cargo.toml
# Edit version field

# Login to crates.io (if not done)
cargo login YOUR_API_TOKEN

# Publish to crates.io
cargo publish
```

### Package Structure
```
bindings/rust/arkilian/
├── Cargo.toml              # Package manifest
├── build.rs                # Build script (bindgen)
├── src/
│   ├── lib.rs              # Rust API
│   └── bindings.h          # C header for bindgen
└── README.md               # Crate README
```

### Cargo.toml Configuration
```toml
[package]
name = "arkilian"
version = "1.0.0"
edition = "2021"

[dependencies]
libc = "0.2"

[build-dependencies]
bindgen = "0.69"

[lib]
name = "arkilian"
crate-type = ["cdylib", "rlib"]
```

### Linking the C Library
The crate expects the C library to be available at `build/Release/libarkilian.dylib`.

**Build Steps:**

```bash
# From project root, run:
npm run build

# This creates build/Release/arkilian.node
# The shared library build/Release/libarkilian.dylib may need to be created:

# Only if libarkilian.dylib doesn't exist:
clang -dynamiclib -o build/Release/libarkilian.dylib \
    build/Release/obj.target/arkilian/src/class.o \
    build/Release/obj.target/arkilian/src/deps/sqlite/sqlite3.o \
    -lcurl -lpthread
```

**Rust binding:**

```bash
cd bindings/rust/arkilian
cargo build --lib
```

**Note**: 
- The library compiles without linking (`cargo build --lib`)
- Running examples/tests requires linking to the C library

### Post-Publish
Users can add to their Cargo.toml:
```toml
[dependencies]
arkilian = "1.0.0"
```

---

## PHP (Packagist)

### Prerequisites
- GitHub repository (Packagist pulls from GitHub)
- Packagist account linked to GitHub

### Build & Publish

**Option 1: Direct GitHub Integration (Recommended)**

1. Push code to GitHub:
   ```bash
   cd /path/to/birth-of-Arkilian/bindings/php
   git init
   git add .
   git commit -m "Initial commit"
   git remote add origin https://github.com/YOUR_USERNAME/arkilian-php.git
   git push -u origin main
   ```

2. Submit to Packagist:
   - Go to https://packagist.org/packages/submit
   - Enter your GitHub repository URL
   - Packagist will automatically track releases

**Option 2: Manual Upload**

```bash
# Create a distribution
cd /path/to/birth-of-Arkilian/bindings/php
zip -r arkilian.zip Arkilian.php composer.json

# Submit to Packagist manually
# (Not recommended - GitHub integration is better)
```

### composer.json Configuration

```json
{
    "name": "your-name/arkilian",
    "description": "Arkilian - SQLite wrapper with automated cloud backup",
    "type": "library",
    "require": {
        "php": ">=7.4"
    },
    "require-dev": {},
    "suggest": {
        "ext-ffi": "Required for FFI bindings"
    },
    "autoload": {
        "psr-4": {
            "Arkilian\\": ""
        }
    },
    "license": "MIT",
    "authors": [
        {
            "name": "CodeDynasty-dev"
        }
    ],
    "minimum-stability": "stable",
    "prefer-stable": true
}
```

### Package Structure
```
bindings/php/
├── composer.json       # Package manifest
├── Arkilian.php        # PHP class
└── README.md          # Documentation
```

### Post-Publish
Users can install with:
```bash
composer require your-name/arkilian
```

### Important Notes
- PHP FFI requires PHP 7.4+ with FFI extension enabled
- In php.ini: `ffi.enable=true` (or run with `-d ffi.enable=true`)
- The library path needs to be accessible at runtime

---

## Version Management

### Recommended Version Strategy

| Release Type | When to Use | Example |
|--------------|-------------|---------|
| patch | Bug fixes, small changes | 1.0.0 → 1.0.1 |
| minor | New features, backward compatible | 1.0.0 → 1.1.0 |
| major | Breaking changes | 1.0.0 → 2.0.0 |

### Changelog
Maintain a CHANGELOG.md in each package:
```markdown
## [1.0.1] - 2024-01-15
### Fixed
- Fixed memory leak in db_finalize

## [1.0.0] - 2024-01-01
### Added
- Initial release with db_init, db_exec, db_prepare, etc.
```

---

## Cross-Platform Considerations

### Node.js Addon
- Requires node-gyp
- Prebuilt binaries can be distributed via npm
- Windows: requires compilation on Windows machine

### Python (cffi)
- Works cross-platform with same C library
- Wheel can include platform-specific binary

### Rust
- Compiled on user's machine
- Cross-compilation requires Rust toolchain

### PHP
- FFI works on any platform with PHP 7.4+
- Library must be available as .dylib/.so/.dll

---

## Testing Before Publishing

### Node.js
```bash
npm run test  # Runs test.js
```

### Python
```bash
python -c "
from arkilian import Arkilian
db = Arkilian('test.db')
db.run('SELECT 1')
print('OK')
"
```

### Rust
```bash
cargo test
```

### PHP
```bash
php -d ffi.enable=true -r "
require 'Arkilian.php';
\$db = new Arkilian('test.db');
echo 'OK';
"
```

---

## Common Issues

### npm
- **Error: node-gyp not found**: Install with `npm install -g node-gyp`
- **Error: node-pre-gyp fallback**: Use `npm run build` to build locally first

### PyPI
- **Error: missing twine**: `pip install twine`
- **Error: 403 unauthorized**: Check PyPI credentials with `twine whoami`

### crates.io
- **Error: invalid token**: Run `cargo login` with correct token
- **Error: name already taken**: Choose unique crate name

### Packagist
- **Package not found**: Ensure GitHub repo is public
- **FFI not working**: Enable with `-d ffi.enable=true` flag

---

## Links
- npm: https://www.npmjs.com/
- PyPI: https://pypi.org/
- crates.io: https://crates.io/
- Packagist: https://packagist.org/