from __future__ import annotations

import asyncio
import os
import sys

from pydantic import ValidationError
from telethon import TelegramClient

from burst_guard.config import LoginSettings
from burst_guard.session_lock import SessionLock


async def login(settings: LoginSettings) -> None:
    path = settings.telegram_session_path
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with SessionLock(path):
        client = TelegramClient(
            str(path),
            settings.telegram_api_id,
            settings.telegram_api_hash.get_secret_value(),
        )
        try:
            await client.start()
            me = await client.get_me()
            if me is None or getattr(me, "bot", True):
                raise RuntimeError("the session must belong to a Telegram user account")
            print(f"Telegram user session initialized for account ID {me.id}")
        finally:
            await client.disconnect()
        os.chmod(path, 0o600)


def main() -> None:
    try:
        settings = LoginSettings()  # type: ignore[call-arg]
        asyncio.run(login(settings))
    except (ValidationError, RuntimeError, OSError) as exc:
        print(f"burst-guard login failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    except KeyboardInterrupt:
        raise SystemExit(130) from None


if __name__ == "__main__":
    main()
