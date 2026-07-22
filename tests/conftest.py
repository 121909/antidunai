import os

os.environ.setdefault("BOT_TOKEN", "1:test-token")
os.environ.setdefault("API_ID", "1")
os.environ.setdefault("API_HASH", "test-hash")
os.environ.setdefault("DATA_PATH", "/tmp/antidunai-test-data")
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:////tmp/antidunai-test-data/database.db")
