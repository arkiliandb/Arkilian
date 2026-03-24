# Contributing to Arkilian
Arkilian follows these contribution policy:

**Arkilian is open-source, and open-contribution.**

We welcome external contributions, but expect a high standard of collaboration.

## Pull Requests

In order to keep Arkilian legally secure and technically robust, we place a barrier to entry. We do accept patches and pull requests, provided they meet the following mandatory requirements:

1. **Discuss First:** You must open a GitHub Issue and receive explicit design approval from a core maintainer before opening a Pull Request. **No unapproved PRs will be reviewed.**
2. **Immaculate Code:** Your code must compile with absolutely zero warnings under `-Wall -Wextra -Wpedantic -Werror` across macOS, Linux, and Windows. A single warning failing the CI pipeline immediately closes the PR.
3. **Comprehensive Testing:** Every logic change must include tests and undergo rigorous memory safety verification. Memory leaks or undefined behavior are automatic grounds for rejection.
4. **Atomic, Clean History:** Commits must be squashed and logically separated.

## Bug Reports

If you find a bug in Arkilian, we are very happy to hear about it! Please report the bug by opening an issue on our GitHub repository. 

When reporting a bug, please:
1. Provide a clear description of the problem.
2. Include reproducible steps and any relevant log traces.
3. State what version of Arkilian and operating system you are using.

**Important:** If you intend to submit a patch to fix the bug, please explicitly state your intentions within the issue. Wait for a core maintainer to assign or approve the fix before writing code.

## Feature Requests

You are welcome to suggest new features.

Please open an Issue for discussion. Major architectural or API changes require extensive technical vetting from the core team and will face rigorous scrutiny. Only after the core team explicitly approves the proposal should you begin implementing the feature. 

## Your Own Forks

Because Arkilian is open-source, you are completely free to fork the repository and modify it for your specific private needs. 

If you do intend to merge those changes back into this official upstream repository someday, ensure your fork adheres entirely to our strict CI, zero-warning code constraints, and legal obligations outlined above.
