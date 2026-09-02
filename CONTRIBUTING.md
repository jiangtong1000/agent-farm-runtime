# Contributing

The frozen sections of `V2_DESIGN.md` are architectural contracts. Do not change them casually to accommodate an implementation shortcut.

Implementation rule:

1. Preserve Task Store authority.
2. Keep Worker Registry observational.
3. Never use mtime/timestamp ordering as a control signal.
4. Route authoritative task mutations through transition validation.
5. Keep mechanical orchestration free of scientific judgment.
6. Add a regression test for every historical or newly observed orchestration failure.
7. Prefer shadow evidence before adding production mechanisms.
