"""Async connection pooling and query execution."""

from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import asynccontextmanager, suppress
from typing import Any, AsyncGenerator
from urllib.parse import urlparse

from sqlp.sql import (
    SelectQueryBuilder,
    InsertQueryBuilder,
    UpdateQueryBuilder,
    DeleteQueryBuilder,
    SQLStatement,
)
from sqlp.table import Table
from sqlp.snapshot import (
    SchemaRegistry,
    load_schema_registry,
    should_validate_with_snapshot,
)


class AsyncConnection(ABC):
    """Abstract base for database connections."""

    @abstractmethod
    async def execute(self, sql: str, parameters: list[Any]) -> int:
        """Execute a statement and return affected rows."""
        raise NotImplementedError

    @abstractmethod
    async def fetch_one(self, sql: str, parameters: list[Any]) -> dict[str, Any] | None:
        """Fetch a single row."""
        raise NotImplementedError

    @abstractmethod
    async def fetch_all(self, sql: str, parameters: list[Any]) -> list[dict[str, Any]]:
        """Fetch all rows."""
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        """Close the connection."""
        raise NotImplementedError


class PostgreSQLConnection(AsyncConnection):
    """PostgreSQL async connection using asyncpg."""

    def __init__(self, connection: Any) -> None:
        """Initialize with asyncpg connection object."""
        self._conn = connection
        assert connection is not None, "asyncpg connection cannot be None"

    async def execute(self, sql: str, parameters: list[Any]) -> int:
        """Execute statement and return affected row count."""
        result = await self._conn.execute(sql, *parameters)
        # asyncpg returns string like "DELETE 5", extract count
        if isinstance(result, str) and (parts := result.split()):
            with suppress(ValueError):
                return int(parts[-1])
        return 0

    async def fetch_one(self, sql: str, parameters: list[Any]) -> dict[str, Any] | None:
        """Fetch single row as dict."""
        row = await self._conn.fetchrow(sql, *parameters)
        if row is None:
            return None
        return dict(row)

    async def fetch_all(self, sql: str, parameters: list[Any]) -> list[dict[str, Any]]:
        """Fetch all rows as dicts."""
        rows = await self._conn.fetch(sql, *parameters)
        return [dict(row) for row in rows]

    async def close(self) -> None:
        """Close connection."""
        await self._conn.close()


class SQLiteConnection(AsyncConnection):
    """SQLite async connection using aiosqlite."""

    def __init__(self, connection: Any) -> None:
        """Initialize with aiosqlite connection object."""
        self._conn = connection
        assert connection is not None, "aiosqlite connection cannot be None"

    async def execute(self, sql: str, parameters: list[Any]) -> int:
        """Execute statement and return affected row count."""
        cursor = await self._conn.execute(sql, parameters)
        await self._conn.commit()
        return cursor.rowcount

    async def fetch_one(self, sql: str, parameters: list[Any]) -> dict[str, Any] | None:
        """Fetch single row as dict."""
        cursor = await self._conn.execute(sql, parameters)
        row = await cursor.fetchone()
        if row is None:
            return None
        # Get column names from cursor description
        columns = [desc[0] for desc in cursor.description]
        return dict(zip(columns, row))

    async def fetch_all(self, sql: str, parameters: list[Any]) -> list[dict[str, Any]]:
        """Fetch all rows as dicts."""
        cursor = await self._conn.execute(sql, parameters)
        rows = await cursor.fetchall()
        if not rows:
            return []
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in rows]

    async def close(self) -> None:
        """Close connection."""
        await self._conn.close()


class MySQLConnection(AsyncConnection):
    """MySQL async connection using aiomysql."""

    def __init__(self, connection: Any) -> None:
        """Initialize with aiomysql connection object."""
        self._conn = connection
        assert connection is not None, "aiomysql connection cannot be None"

    async def execute(self, sql: str, parameters: list[Any]) -> int:
        """Execute statement and return affected row count."""
        async with self._conn.cursor() as cursor:
            await cursor.execute(sql, parameters)
            await self._conn.commit()
            return cursor.rowcount

    async def fetch_one(self, sql: str, parameters: list[Any]) -> dict[str, Any] | None:
        """Fetch single row as dict."""
        async with self._conn.cursor() as cursor:
            await cursor.execute(sql, parameters)
            row = await cursor.fetchone()
            if row is None:
                return None
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            return dict(zip(columns, row))

    async def fetch_all(self, sql: str, parameters: list[Any]) -> list[dict[str, Any]]:
        """Fetch all rows as dicts."""
        async with self._conn.cursor() as cursor:
            await cursor.execute(sql, parameters)
            rows = await cursor.fetchall()
            if not rows:
                return []
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in rows]

    async def close(self) -> None:
        """Close connection."""
        self._conn.close()
        await self._conn.wait_closed()


class AsyncPool:
    """Async connection pool supporting PostgreSQL, SQLite, and MySQL."""

    def __init__(
        self,
        database_url: str,
        min_size: int = 5,
        max_size: int = 20,
        statement_cache_size: int = 100,
        registry: SchemaRegistry | None = None,
    ) -> None:
        """Initialize connection pool.

        Args:
            database_url: Database connection string (postgresql://, sqlite://, mysql://)
            min_size: Minimum connections in pool (unused for SQLite, single connection)
            max_size: Maximum connections in pool
            statement_cache_size: Max prepared statements to cache per connection
            registry: Optional SchemaRegistry for offline validation
        """
        assert database_url, "Database URL required"
        assert max_size > 0, "max_size must be positive"

        self.database_url = database_url
        self.min_size = min(min_size, max_size)
        self.max_size = max_size
        self.statement_cache_size = statement_cache_size

        # Use provided registry, or load from config if env var says to use snapshot
        if registry is not None:
            self.registry: SchemaRegistry | None = registry
        elif should_validate_with_snapshot():
            self.registry = load_schema_registry()
        else:
            self.registry = None

        # Parse database URL
        parsed = urlparse(database_url)
        self.db_type = parsed.scheme.lower()

        assert self.db_type in ("postgresql", "sqlite", "mysql"), (
            f"Unsupported database type: {self.db_type}"
        )

        self._pool: Any = None
        self._connection: AsyncConnection | None = None
        self._in_transaction = False

    async def connect(self) -> None:
        """Connect to database and initialize pool."""
        if self.db_type == "postgresql":
            await self._connect_postgresql()
        elif self.db_type == "sqlite":
            await self._connect_sqlite()
        elif self.db_type == "mysql":
            await self._connect_mysql()

    async def _connect_postgresql(self) -> None:
        """Connect to PostgreSQL."""
        import asyncpg

        # Extract connection params from URL
        parsed = urlparse(self.database_url)

        self._pool = await asyncpg.create_pool(
            user=parsed.username or "postgres",
            password=parsed.password or "",
            database=parsed.path.lstrip("/") or "postgres",
            host=parsed.hostname or "localhost",
            port=parsed.port or 5432,
            min_size=self.min_size,
            max_size=self.max_size,
        )
        assert self._pool is not None, "Failed to create PostgreSQL pool"

    async def _connect_sqlite(self) -> None:
        """Connect to SQLite."""
        import aiosqlite

        db_path = self.database_url.replace("sqlite://", "")
        if db_path == ":memory:":
            db_path = ":memory:"

        conn = await aiosqlite.connect(db_path)
        self._connection = SQLiteConnection(conn)

    async def _connect_mysql(self) -> None:
        """Connect to MySQL."""
        import aiomysql

        parsed = urlparse(self.database_url)

        conn = await aiomysql.connect(
            host=parsed.hostname or "localhost",
            port=parsed.port or 3306,
            user=parsed.username or "root",
            password=parsed.password or "",
            db=parsed.path.lstrip("/") or "mysql",
        )
        self._connection = MySQLConnection(conn)

    async def close(self) -> None:
        """Close all connections in pool."""
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

        if self._connection is not None:
            await self._connection.close()
            self._connection = None

    async def __aenter__(self) -> AsyncPool:
        """Context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        await self.close()

    @asynccontextmanager
    async def _get_connection(self) -> AsyncGenerator[AsyncConnection, None]:
        """Get a connection from the pool."""
        if self.db_type == "postgresql":
            conn = await self._pool.acquire()  # type: ignore
            try:
                yield PostgreSQLConnection(conn)
            finally:
                await self._pool.release(conn)  # type: ignore
        else:
            # SQLite and MySQL use single connection
            assert self._connection is not None, "Not connected to database"
            yield self._connection

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[None, None]:
        """Async context manager for transactions."""
        async with self._get_connection() as conn:
            if self.db_type == "postgresql":
                await conn._conn.execute("BEGIN")  # type: ignore
                try:
                    yield
                    await conn._conn.execute("COMMIT")  # type: ignore
                except Exception:
                    await conn._conn.execute("ROLLBACK")  # type: ignore
                    raise
            else:
                # SQLite/MySQL handle transactions at cursor level
                yield

    def select(self, *tables: type[Table]) -> SelectQueryBuilder:
        """Start a SELECT query."""
        assert tables, "At least one table required"
        return SelectQueryBuilder(
            list(tables),
            sql_dialect=self.db_type,
            registry=self.registry,
        )

    def insert(self, table: type[Table]) -> InsertQueryBuilder:
        """Start an INSERT query."""
        assert table is not None, "Table required"
        return InsertQueryBuilder(
            table, sql_dialect=self.db_type, registry=self.registry
        )

    def update(self, table: type[Table]) -> UpdateQueryBuilder:
        """Start an UPDATE query."""
        assert table is not None, "Table required"
        return UpdateQueryBuilder(
            table, sql_dialect=self.db_type, registry=self.registry
        )

    def delete(self, table: type[Table]) -> DeleteQueryBuilder:
        """Start a DELETE query."""
        assert table is not None, "Table required"
        return DeleteQueryBuilder(
            table, sql_dialect=self.db_type, registry=self.registry
        )

    async def execute(self, stmt: SQLStatement) -> int:
        """Execute a statement and return affected rows."""
        async with self._get_connection() as conn:
            return await conn.execute(stmt.text, stmt.parameters)

    async def fetch_one(self, stmt: SQLStatement) -> dict[str, Any] | None:
        """Fetch a single row."""
        async with self._get_connection() as conn:
            return await conn.fetch_one(stmt.text, stmt.parameters)

    async def fetch_all(self, stmt: SQLStatement) -> list[dict[str, Any]]:
        """Fetch all rows."""
        async with self._get_connection() as conn:
            return await conn.fetch_all(stmt.text, stmt.parameters)
