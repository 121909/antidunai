from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field


@dataclass(slots=True)
class Metrics:
    _values: Counter[str] = field(default_factory=Counter)

    def increment(self, name: str, amount: int = 1) -> None:
        self._values[name] += amount

    def get(self, name: str) -> int:
        return self._values[name]

    def snapshot(self) -> dict[str, int]:
        return dict(self._values)
