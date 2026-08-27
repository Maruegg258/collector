from __future__ import annotations

from .storage import TradeStore
from .storage_protocol import StorageBackend


class SQLiteTradeStore(TradeStore):
    backend_name = "sqlite"


def create_store(
    *,
    backend: str,
    db_path: str,
    database_url: str | None = None,
) -> StorageBackend:
    normalized = backend.strip().lower()
    if normalized == "sqlite":
        return SQLiteTradeStore(db_path)
    if normalized in {"postgres", "postgresql"}:
        if not database_url:
            raise ValueError("DATABASE_URL is required for PostgreSQL storage")
        from .storage_postgres import PostgresTradeStore

        return PostgresTradeStore(database_url)
    raise ValueError(f"unsupported storage backend: {backend!r}")
