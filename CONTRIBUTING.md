# Contributing

The frozen sections of [V2_DESIGN.md](V2_DESIGN.md) are architectural contracts.
Its implementation status is historical; use [operations](docs/OPERATIONS.md) for
current workflows. Do not change an invariant to accommodate an implementation shortcut.
Keep reusable failure explanations in [lessons](docs/LESSONS.md); keep private
project details and deployment records outside the repository. Tests should use
synthetic task names, example hostnames and temporary directories.

The optional release-wrapper comparison accepts `FARM_TEST_REFERENCE_WRAPPER` and,
if needed, `FARM_TEST_REFERENCE_PYTHON`. Supply these explicitly for a local check;
the normal suite does not inspect an existing deployment.

Implementation rule:

1. Preserve Task Store authority.
2. Keep Worker Registry observational.
3. Never use mtime/timestamp ordering as a control signal.
4. Route authoritative task mutations through transition validation.
5. Keep mechanical orchestration free of scientific judgment.
6. Add a regression test for every historical or newly observed orchestration failure.
7. Prefer shadow evidence before adding production mechanisms.
