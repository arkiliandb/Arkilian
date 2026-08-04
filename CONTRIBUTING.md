# Contributing to Arkilian
Arkilian follows these contribution policy:

**Arkilian is open-source, and open-contribution.**

We welcome external contributions, but expect a high standard of collaboration.

## Pull Requests

In order to keep Arkilian legally secure and technically robust, we place a barrier to entry. We do accept patches and pull requests, provided they meet the following mandatory requirements:

1. **Discuss First:** You must open a GitHub Issue and receive explicit design approval from a core maintainer before opening a Pull Request. **No unapproved PRs will be reviewed.**
2. **Immaculate Code:** Your code must compile with absolutely zero warnings under `-Wall -Wextra -Wpedantic -Werror` across macOS, Linux, and Windows. A single warning failing the CI pipeline immediately closes the PR.
3. **Comprehensive Testing:** Every logic change must include tests and undergo rigorous memory safety verification. Memory leaks or undefined behavior are automatic grounds for rejection.
   - Tests should be placed in the `tests/` directory
   - Enable tests with: `cmake -B build -S . -DARKILIAN_BUILD_TESTS=ON`
   - Build and run tests:
     ```bash
     cmake --build build
     cd build && ctest --output-on-failure
     ```
     This runs every `tests/*.c` as its own ctest entry — a regression
     in any one of them is visible individually. `tests/run_all.sh`
     is kept as a POSIX-portable fallback but `ctest` is canonical.
   - ASAN+UBSAN must be clean (`-fsanitize=address,undefined`); the CI
     `sanitizer` job enforces this. Run locally:
     ```bash
     cmake -B build_asan -S . -DCMAKE_BUILD_TYPE=Debug \
       -DARKILIAN_BUILD_TESTS=ON \
       -DCMAKE_C_FLAGS="-fsanitize=address,undefined -fno-omit-frame-pointer -g" \
       -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=address,undefined"
     cmake --build build_asan
     cd build_asan && ASAN_OPTIONS=detect_leaks=1 ctest --output-on-failure
     ```
   - ThreadSanitizer must also be clean for synchronization changes — the
     client runs dedicated game, flush, and snapshot threads that share
     heartbeat counters and the outbox, so the CI `tsan` job gates data
     races. Run locally:
     ```bash
     cmake -B build_tsan -S . -DCMAKE_BUILD_TYPE=Debug \
       -DARKILIAN_BUILD_TESTS=ON \
       -DCMAKE_C_FLAGS="-fsanitize=thread -fno-omit-frame-pointer -g" \
       -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=thread"
     cmake --build build_tsan
     cd build_tsan && TSAN_OPTIONS=halt_on_error=1 ctest --output-on-failure
     ```
   - The test matrix includes `windows-latest` via MSYS2 UCRT64 MinGW.
     The BSD-socket mock-server tests (`test_kill_switch`,
     `test_kill_resilience`, `test_load_contention`, `test_hydration`)
     are intentionally POSIX-only (MinGW-w64 ships no `<sys/socket.h>`);
     never silently compile them in on Windows. The MSVC `build` job
     covers native compile-fitness of the library itself.
4. **Atomic, Clean History:** Commits must be squashed and logically separated.

## Bug Reports

If you find a bug in Arkilian, we are very happy to hear about it! Please report the bug by opening an issue on our GitHub repository. 

When reporting a bug, please:
1. Provide a clear description of the problem.
2. Include reproducible steps and any relevant log traces.
3. State what version of Arkilian and operating system you are using.
4. Include relevant environment variables (with secrets redacted).

**Important:** If you intend to submit a patch to fix the bug, please explicitly state your intentions within the issue. Wait for a core maintainer to assign or approve the fix before writing code.

## Feature Requests

You are welcome to suggest new features.

Please open an Issue for discussion. Major architectural or API changes require extensive technical vetting from the core team and will face rigorous scrutiny. Only after the core team explicitly approves the proposal should you begin implementing the feature. 

## Build Instructions for Contributors

```bash
# Clone the repository
git clone https://github.com/CodeDynasty-dev/birth-of-Arkilian.git
cd birth-of-Arkilian

# Build with all options enabled (for development)
cmake -B build -S . -DCMAKE_BUILD_TYPE=Debug -DARKILIAN_BUILD_TESTS=ON -DARKILIAN_BUILD_EXAMPLES=ON

# Compile
cmake --build build --config Debug

# Run tests (canonical — every tests/*.c is its own ctest entry)
cd build && ctest --output-on-failure

# Run example
./build/arkilian_example
```

## Environment Variables

When contributing, be aware that Arkilian uses `ARKILIAN_` prefixed environment variables for configuration. See `README.md` for the full list.

## Your Own Forks

Because Arkilian is open-source, you are completely free to fork the repository and modify it for your specific private needs. 

If you do intend to merge those changes back into this official upstream repository someday, ensure your fork adheres entirely to our strict CI, zero-warning code constraints, and legal obligations outlined above.
