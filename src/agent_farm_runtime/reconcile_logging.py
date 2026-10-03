"""Optional daemon diagnostics mirrored to a daily rotating, non-authoritative log."""
from __future__ import annotations

import logging
import sys
import traceback
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path


class _FileHandler(TimedRotatingFileHandler):
    def handleError(self, record):
        # The logging default writes to stderr, which is itself being mirrored.
        # Propagate disk errors instead of recursively logging a logging failure.
        raise


class _Tee:
    def __init__(self, stream, handler):
        self.stream, self.handler = stream, handler
        self.pending = ""

    def write(self, text):
        self.stream.write(text)
        self.pending += text
        while "\n" in self.pending:
            line, self.pending = self.pending.split("\n", 1)
            self._record(line)
        return len(text)

    def _record(self, line):
        self.handler.handle(logging.LogRecord("reconciler", logging.INFO, "", 0, line, (), None))

    def flush(self):
        if self.pending:
            line, self.pending = self.pending, ""
            self._record(line)
        self.handler.flush()
        self.stream.flush()

    def __getattr__(self, name):
        return getattr(self.stream, name)


@contextmanager
def reconcile_output(log_path: str | Path | None):
    """Mirror Python stdout/stderr; the caller must hold the reconciler lock.

    Rotation happens on the first write after UTC midnight and retains 14 dated
    backups. This is diagnostic output, not the durable event/Task Store journal.
    """
    if log_path is None:
        yield None
        return
    path = Path(log_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    handler = _FileHandler(path, when="midnight", backupCount=14, utc=True, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(message)s"))
    stdout, stderr = _Tee(sys.stdout, handler), _Tee(sys.stderr, handler)
    try:
        with redirect_stdout(stdout), redirect_stderr(stderr):
            try:
                yield path
            except BaseException:
                traceback.print_exc()
                raise
    finally:
        try:
            stdout.flush()
            stderr.flush()
        finally:
            handler.close()
