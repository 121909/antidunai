from __future__ import annotations

import sys

from pydantic import ValidationError

from burst_guard.app import run


def main() -> None:
    try:
        run()
    except (ValidationError, RuntimeError) as exc:
        print(f"burst-guard startup failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    except KeyboardInterrupt:
        raise SystemExit(130) from None


if __name__ == "__main__":
    main()
