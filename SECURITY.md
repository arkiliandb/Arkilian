# Security Policy

Arkilian is an embedded SQLite wrapper with a cloud control plane, so any
vulnerability can affect an **unlimited number of downstream databases**.
We treat security reports with priority and publish fixes promptly.

## Reporting a vulnerability

Please **do not open a public GitHub issue** for a suspected vulnerability.

Report privately through the GitHub Security Advisory channel:

https://github.com/CodeDynasty-dev/birth-of-Arkilian/security/advisories

If the advisory form cannot be used, email `security@arkilian.com` with the
subject prefix `[Arkilian Security]`.

> **PGP contact:** we are migrating release provenance to sigstore/cosign
> keyless signatures (OIDC), so security-correspondence PGP is **not yet**
> published. Until this page lists a real key fingerprint, treat
> `security@arkilian.com` as an unauthenticated channel and prefer the
> GitHub Security Advisory form above.

## What to include

To help us triage quickly, include:

- Product and version affected (C client `src/class.c`, hydration engine
  `src/hydration.c`, Node binding `src/arkilian.cc`, or Go control plane
  in `server/`).
- A minimal reproducer (code, SQL schema, or environment variable
  combination).
- Expected vs. observed behavior.
- If you believe it is exploitable, your **best-effort impact
  assessment** (e.g. SSRF, auth bypass, data-at-rest integrity loss).

## Responsible disclosure process

1. **Report** — you send us a private report (advisory form or e-mail).
2. **Acknowledgment** — we confirm receipt within **3 business days** and
   assign a tracking ID.
3. **Triage** — we assess severity and respond with a fix *target*
   timeline within **7 days**:
   - **Critical / High**: fix normally targeted within **7 days**.
   - **Medium / Low**: fix normally targeted within **30 days**, or a
     documented rationale for the timeline.
4. **Fix & release** — we ship the fix in a tagged release. Releases carry
   SLSA provenance and an SBOM; the fix commit is disclosed.
5. **Disclosure** — if we cannot fix within the agreed window, we will
   disclose the issue publicly (with credit to the reporter) so the
   community can apply mitigations, unless the reporter requests
   coordinated delay.
6. **Credit** — reporters are credited in the release notes unless they
   prefer to stay anonymous.

We ask that you **refrain from public disclosure until we have shipped a
fix** (target ≤ 90 days total from initial report) or have explicitly
agreed to earlier coordinated disclosure.

## Scope

In scope:

- The C library: `src/class.c`, `src/hydration.c`, `src/sha256.c`.
- The Node.js binding: `src/arkilian.cc`.
- The Go control plane: `server/`.

Out of scope / not eligible:

- Vulnerabilities in upstream `sqlite3.c` amalgamation — report those to
  the SQLite project; we only track issues in our integration of it.
- Issues in a cloud provider's object storage or identity services.
- Scenarios requiring the attacker already holds a valid tenant API key
  for the target tenant (normal multi-tenancy abuse is out of scope; a
  **cross-tenant** escalation using a key is in scope).
- Phishing, physical attacks, or social engineering.
- Recently reported issues for which a fix already exists on `main`.

## Supported versions

Security fixes land on the latest tagged release. Backports to older
releases are handled case-by-case depending on severity and support
contract.

## Verification of releases

Every published release is accompanied by:

- **SLSA provenance** generated on GitHub-hosted runners, verifiable with
  `slsa-verifier`.
- An **SBOM** signed with sigstore, verifiable with `cosign verify-blob`.
- The release assets themselves plus a `sha256sum` manifest.

See `docs/` for the verification commands.