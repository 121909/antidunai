from __future__ import annotations

import fcntl
import os
from pathlib import Path
from types import TracebackType


class SessionLock:
    def __init__(self, session_path: Path) -> None:
        self._path = session_path.with_suffix(session_path.suffix + ".lock")
        self._fd: int | None = None

    def __enter__(self) -> SessionLock:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._fd = os.open(self._path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(self._fd)
            self._fd = None
            raise RuntimeError("Telegram session is already in use by another process") from exc
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._fd is not None:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
            os.close(self._fd)
            self._fd = None
