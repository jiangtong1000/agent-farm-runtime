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

Keep changes small by tracing the actual failure first, reusing existing helpers,
then preferring the standard library or native platform facilities before custom
machinery. Remove unused configuration and duplicate implementations where the
behavior is covered. This follows [Ponytail's design ladder](https://github.com/dietrichgebert/ponytail#how-it-works):
simplicity must preserve validation, error handling and data-loss protections.
