"""Tests for async connection pool and execution."""

import pytest
import asyncio

from sqlp.pool import AsyncPool
from sqlp.table import Table
from sqlp.types import Column


class TestAsyncPoolInitialization:
    """Test AsyncPool initialization and connection."""

    def test_postgresql_url_parsing(self) -> None:
        pool = AsyncPool("postgresql://user:pass@localhost:5432/mydb")
        assert pool.db_type == "postgresql"
        assert pool.max_size == 20

    def test_sqlite_url_parsing(self) -> None:
        pool = AsyncPool("sqlite:///path/to/db.sqlite")
        assert pool.db_type == "sqlite"

    def test_sqlite_memory_url(self) -> None:
        pool = AsyncPool("sqlite://:memory:")
        assert pool.db_type == "sqlite"

    def test_mysql_url_parsing(self) -> None:
        pool = AsyncPool("mysql://root:pass@localhost:3306/mydb")
        assert pool.db_type == "mysql"

    def test_invalid_db_type(self) -> None:
        with pytest.raises(AssertionError):
            AsyncPool("mongodb://localhost/mydb")

    def test_pool_config(self) -> None:
        pool = AsyncPool(
            "postgresql://localhost/mydb",
            min_size=2,
            max_size=10,
            statement_cache_size=50,
        )
        assert pool.min_size == 2
        assert pool.max_size == 10
        assert pool.statement_cache_size == 50

    def test_max_size_validation(self) -> None:
        with pytest.raises(AssertionError):
            AsyncPool("postgresql://localhost/mydb", max_size=0)

    def test_min_size_clamped_to_max(self) -> None:
        pool = AsyncPool(
            "postgresql://localhost/mydb",
            min_size=30,
            max_size=10,
        )
        assert pool.min_size == 10  # Clamped to max_size


class TestAsyncPoolQueryBuilders:
    """Test query builder methods on pool."""

    def test_select_builder(self) -> None:
        pool = AsyncPool("sqlite://:memory:")
        
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        builder = pool.select(User)
        assert builder is not None
        assert builder.sql_dialect == "sqlite"

    def test_insert_builder(self) -> None:
        pool = AsyncPool("postgresql://localhost/mydb")
        
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        builder = pool.insert(User)
        assert builder is not None
        assert builder.sql_dialect == "postgresql"

    def test_update_builder(self) -> None:
        pool = AsyncPool("mysql://localhost/mydb")
        
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        builder = pool.update(User)
        assert builder is not None
        assert builder.sql_dialect == "mysql"

    def test_delete_builder(self) -> None:
        pool = AsyncPool("sqlite://:memory:")
        
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        builder = pool.delete(User)
        assert builder is not None
        assert builder.sql_dialect == "sqlite"

    def test_select_multiple_tables(self) -> None:
        pool = AsyncPool("postgresql://localhost/mydb")
        
        class User(Table):
            id = Column[int](primary_key=True)

        class Post(Table):
            id = Column[int](primary_key=True)
            user_id = Column[int]()

        builder = pool.select(User, Post)
        assert len(builder.tables) == 2


class TestAsyncPoolSQLiteIntegration:
    """Test actual SQLite operations."""

    @pytest.mark.asyncio
    async def test_sqlite_create_and_query(self) -> None:
        """Test basic SQLite operations."""
        # Create in-memory database
        pool = AsyncPool("sqlite://:memory:")
        await pool.connect()

        try:
            class User(Table):
                id = Column[int](primary_key=True)
                email = Column[str]()
                name = Column[str]()

            # Create table
            create_sql = """
            CREATE TABLE user (
                id INTEGER PRIMARY KEY,
                email TEXT NOT NULL,
                name TEXT NOT NULL
            )
            """
            async with pool._get_connection() as conn:
                await conn.execute(create_sql, [])

            # Insert
            insert_builder = pool.insert(User)
            insert_builder.values(
                {"id": 1, "email": "alice@example.com", "name": "Alice"},
                {"id": 2, "email": "bob@example.com", "name": "Bob"},
            )
            stmt = insert_builder.build()
            await pool.execute(stmt)

            # Select all
            select_builder = pool.select(User)
            select_stmt = select_builder.build()
            rows = await pool.fetch_all(select_stmt)
            
            assert len(rows) == 2
            assert rows[0]["email"] == "alice@example.com"
            assert rows[1]["name"] == "Bob"

        finally:
            await pool.close()

    @pytest.mark.asyncio
    async def test_sqlite_select_with_where(self) -> None:
        """Test SELECT with WHERE clause."""
        pool = AsyncPool("sqlite://:memory:")
        await pool.connect()

        try:
            class User(Table):
                id = Column[int](primary_key=True)
                email = Column[str]()

            # Create and insert
            create_sql = """
            CREATE TABLE user (
                id INTEGER PRIMARY KEY,
                email TEXT NOT NULL
            )
            """
            async with pool._get_connection() as conn:
                await conn.execute(create_sql, [])
                await conn.execute(
                    "INSERT INTO user (id, email) VALUES (?, ?)",
                    [1, "alice@example.com"],
                )
                await conn.execute(
                    "INSERT INTO user (id, email) VALUES (?, ?)",
                    [2, "bob@example.com"],
                )

            # Select with where
            select_builder = pool.select(User)
            select_builder.where(User.email == "alice@example.com")
            stmt = select_builder.build()
            rows = await pool.fetch_all(stmt)

            assert len(rows) == 1
            assert rows[0]["email"] == "alice@example.com"

        finally:
            await pool.close()

    @pytest.mark.asyncio
    async def test_sqlite_update(self) -> None:
        """Test UPDATE query."""
        pool = AsyncPool("sqlite://:memory:")
        await pool.connect()

        try:
            class User(Table):
                id = Column[int](primary_key=True)
                email = Column[str]()
                status = Column[str]()

            # Create and insert
            create_sql = """
            CREATE TABLE user (
                id INTEGER PRIMARY KEY,
                email TEXT NOT NULL,
                status TEXT
            )
            """
            async with pool._get_connection() as conn:
                await conn.execute(create_sql, [])
                await conn.execute(
                    "INSERT INTO user (id, email, status) VALUES (?, ?, ?)",
                    [1, "alice@example.com", "active"],
                )

            # Update
            update_builder = pool.update(User)
            update_builder.set(status="inactive").where(User.id == 1)
            stmt = update_builder.build()
            await pool.execute(stmt)

            # Verify
            select_builder = pool.select(User)
            select_builder.where(User.id == 1)
            stmt = select_builder.build()
            rows = await pool.fetch_all(stmt)

            assert len(rows) == 1
            assert rows[0]["status"] == "inactive"

        finally:
            await pool.close()

    @pytest.mark.asyncio
    async def test_sqlite_delete(self) -> None:
        """Test DELETE query."""
        pool = AsyncPool("sqlite://:memory:")
        await pool.connect()

        try:
            class User(Table):
                id = Column[int](primary_key=True)
                email = Column[str]()

            # Create and insert
            create_sql = """
            CREATE TABLE user (
                id INTEGER PRIMARY KEY,
                email TEXT NOT NULL
            )
            """
            async with pool._get_connection() as conn:
                await conn.execute(create_sql, [])
                await conn.execute(
                    "INSERT INTO user (id, email) VALUES (?, ?)",
                    [1, "alice@example.com"],
                )
                await conn.execute(
                    "INSERT INTO user (id, email) VALUES (?, ?)",
                    [2, "bob@example.com"],
                )

            # Delete
            delete_builder = pool.delete(User)
            delete_builder.where(User.id == 1)
            stmt = delete_builder.build()
            await pool.execute(stmt)

            # Verify
            select_builder = pool.select(User)
            stmt = select_builder.build()
            rows = await pool.fetch_all(stmt)

            assert len(rows) == 1
            assert rows[0]["id"] == 2

        finally:
            await pool.close()

    @pytest.mark.asyncio
    async def test_sqlite_context_manager(self) -> None:
        """Test AsyncPool as context manager."""
        class User(Table):
            id = Column[int](primary_key=True)
            email = Column[str]()

        # Use context manager
        async with AsyncPool("sqlite://:memory:") as pool:
            create_sql = """
            CREATE TABLE user (
                id INTEGER PRIMARY KEY,
                email TEXT NOT NULL
            )
            """
            async with pool._get_connection() as conn:
                await conn.execute(create_sql, [])
                await conn.execute(
                    "INSERT INTO user (id, email) VALUES (?, ?)",
                    [1, "test@example.com"],
                )

            # Query
            stmt = pool.select(User).build()
            rows = await pool.fetch_all(stmt)
            assert len(rows) == 1
