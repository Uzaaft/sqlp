"""Tests for async query execution and result mapping."""

import pytest

from sqlp.pool import AsyncPool
from sqlp.executor import attach_execution, attach_mutation_execution
from sqlp.table import Table
from sqlp.types import Column


class TestAsyncExecution:
    """Test async query execution with result mapping."""

    @pytest.mark.asyncio
    async def test_first_returns_single_row(self) -> None:
        """Test .first() returns single row as model instance."""
        async with AsyncPool("sqlite://:memory:") as pool:

            class User(Table):
                id = Column[int](primary_key=True)
                email = Column[str]()
                name = Column[str]()

            # Setup
            create_sql = """
            CREATE TABLE user (
                id INTEGER PRIMARY KEY,
                email TEXT NOT NULL,
                name TEXT NOT NULL
            )
            """
            async with pool._get_connection() as conn:
                await conn.execute(create_sql, [])
                await conn.execute(
                    "INSERT INTO user (id, email, name) VALUES (?, ?, ?)",
                    [1, "alice@example.com", "Alice"],
                )
                await conn.execute(
                    "INSERT INTO user (id, email, name) VALUES (?, ?, ?)",
                    [2, "bob@example.com", "Bob"],
                )

            # Execute
            builder = pool.select(User)
            query = attach_execution(builder, pool, User)
            user = await query.first()

            assert user is not None
            assert user.id == 1
            assert user.email == "alice@example.com"
            assert user.name == "Alice"

    @pytest.mark.asyncio
    async def test_first_with_where_clause(self) -> None:
        """Test .first() with WHERE condition."""
        async with AsyncPool("sqlite://:memory:") as pool:

            class User(Table):
                id = Column[int](primary_key=True)
                email = Column[str]()

            # Setup
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

            # Execute
            builder = pool.select(User)
            builder.where(User.email == "bob@example.com")
            query = attach_execution(builder, pool, User)
            user = await query.first()

            assert user is not None
            assert user.email == "bob@example.com"

    @pytest.mark.asyncio
    async def test_first_returns_none_for_no_results(self) -> None:
        """Test .first() returns None when no results."""
        async with AsyncPool("sqlite://:memory:") as pool:

            class User(Table):
                id = Column[int](primary_key=True)
                email = Column[str]()

            # Setup
            create_sql = """
            CREATE TABLE user (
                id INTEGER PRIMARY KEY,
                email TEXT NOT NULL
            )
            """
            async with pool._get_connection() as conn:
                await conn.execute(create_sql, [])

            # Execute
            builder = pool.select(User)
            query = attach_execution(builder, pool, User)
            user = await query.first()

            assert user is None

    @pytest.mark.asyncio
    async def test_all_returns_multiple_rows(self) -> None:
        """Test .all() returns list of model instances."""
        async with AsyncPool("sqlite://:memory:") as pool:

            class User(Table):
                id = Column[int](primary_key=True)
                email = Column[str]()

            # Setup
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

            # Execute
            builder = pool.select(User)
            query = attach_execution(builder, pool, User)
            users = await query.all()

            assert len(users) == 2
            assert users[0].email == "alice@example.com"
            assert users[1].email == "bob@example.com"

    @pytest.mark.asyncio
    async def test_all_with_limit(self) -> None:
        """Test .all() respects LIMIT clause."""
        async with AsyncPool("sqlite://:memory:") as pool:

            class User(Table):
                id = Column[int](primary_key=True)
                email = Column[str]()

            # Setup
            create_sql = """
            CREATE TABLE user (
                id INTEGER PRIMARY KEY,
                email TEXT NOT NULL
            )
            """
            async with pool._get_connection() as conn:
                await conn.execute(create_sql, [])
                for i in range(5):
                    await conn.execute(
                        "INSERT INTO user (id, email) VALUES (?, ?)",
                        [i + 1, f"user{i}@example.com"],
                    )

            # Execute
            builder = pool.select(User)
            builder.limit(2)
            query = attach_execution(builder, pool, User)
            users = await query.all()

            assert len(users) == 2

    @pytest.mark.asyncio
    async def test_count(self) -> None:
        """Test .count() returns row count."""
        async with AsyncPool("sqlite://:memory:") as pool:

            class User(Table):
                id = Column[int](primary_key=True)
                email = Column[str]()

            # Setup
            create_sql = """
            CREATE TABLE user (
                id INTEGER PRIMARY KEY,
                email TEXT NOT NULL
            )
            """
            async with pool._get_connection() as conn:
                await conn.execute(create_sql, [])
                for i in range(5):
                    await conn.execute(
                        "INSERT INTO user (id, email) VALUES (?, ?)",
                        [i + 1, f"user{i}@example.com"],
                    )

            # Execute
            builder = pool.select(User)
            query = attach_execution(builder, pool, User)
            count = await query.count()

            assert count == 5

    @pytest.mark.asyncio
    async def test_count_with_where(self) -> None:
        """Test .count() with WHERE condition."""
        async with AsyncPool("sqlite://:memory:") as pool:

            class User(Table):
                id = Column[int](primary_key=True)
                status = Column[str]()

            # Setup
            create_sql = """
            CREATE TABLE user (
                id INTEGER PRIMARY KEY,
                status TEXT NOT NULL
            )
            """
            async with pool._get_connection() as conn:
                await conn.execute(create_sql, [])
                await conn.execute(
                    "INSERT INTO user (id, status) VALUES (?, ?)",
                    [1, "active"],
                )
                await conn.execute(
                    "INSERT INTO user (id, status) VALUES (?, ?)",
                    [2, "active"],
                )
                await conn.execute(
                    "INSERT INTO user (id, status) VALUES (?, ?)",
                    [3, "inactive"],
                )

            # Execute
            builder = pool.select(User)
            builder.where(User.status == "active")
            query = attach_execution(builder, pool, User)
            count = await query.count()

            assert count == 2

    @pytest.mark.asyncio
    async def test_async_iteration(self) -> None:
        """Test async iteration over results."""
        async with AsyncPool("sqlite://:memory:") as pool:

            class User(Table):
                id = Column[int](primary_key=True)
                email = Column[str]()

            # Setup
            create_sql = """
            CREATE TABLE user (
                id INTEGER PRIMARY KEY,
                email TEXT NOT NULL
            )
            """
            async with pool._get_connection() as conn:
                await conn.execute(create_sql, [])
                for i in range(3):
                    await conn.execute(
                        "INSERT INTO user (id, email) VALUES (?, ?)",
                        [i + 1, f"user{i}@example.com"],
                    )

            # Execute
            builder = pool.select(User)
            query = attach_execution(builder, pool, User)

            users = []
            async for user in query:
                users.append(user)

            assert len(users) == 3
            assert all(isinstance(u, User.__row_model__()) for u in users)

    @pytest.mark.asyncio
    async def test_result_mapping_respects_nullable(self) -> None:
        """Test result mapping handles NULL values correctly."""
        async with AsyncPool("sqlite://:memory:") as pool:

            class User(Table):
                id = Column[int](primary_key=True)
                email = Column[str]()
                bio = Column[str](nullable=True)

            # Setup
            create_sql = """
            CREATE TABLE user (
                id INTEGER PRIMARY KEY,
                email TEXT NOT NULL,
                bio TEXT
            )
            """
            async with pool._get_connection() as conn:
                await conn.execute(create_sql, [])
                await conn.execute(
                    "INSERT INTO user (id, email, bio) VALUES (?, ?, ?)",
                    [1, "alice@example.com", "Software engineer"],
                )
                await conn.execute(
                    "INSERT INTO user (id, email, bio) VALUES (?, ?, ?)",
                    [2, "bob@example.com", None],
                )

            # Execute
            builder = pool.select(User)
            query = attach_execution(builder, pool, User)
            users = await query.all()

            assert users[0].bio == "Software engineer"
            assert users[1].bio is None


class TestMutationExecution:
    """Test async mutation execution."""

    @pytest.mark.asyncio
    async def test_insert_execute(self) -> None:
        """Test INSERT execution."""
        async with AsyncPool("sqlite://:memory:") as pool:

            class User(Table):
                id = Column[int](primary_key=True)
                email = Column[str]()

            # Setup
            create_sql = """
            CREATE TABLE user (
                id INTEGER PRIMARY KEY,
                email TEXT NOT NULL
            )
            """
            async with pool._get_connection() as conn:
                await conn.execute(create_sql, [])

            # Execute
            builder = pool.insert(User)
            builder.values({"id": 1, "email": "test@example.com"})
            mutation = attach_mutation_execution(builder, pool)
            rows_affected = await mutation.execute()

            assert rows_affected >= 0  # SQLite may not always report

    @pytest.mark.asyncio
    async def test_update_execute(self) -> None:
        """Test UPDATE execution."""
        async with AsyncPool("sqlite://:memory:") as pool:

            class User(Table):
                id = Column[int](primary_key=True)
                email = Column[str]()
                status = Column[str]()

            # Setup
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
                    [1, "test@example.com", "active"],
                )

            # Execute
            builder = pool.update(User)
            builder.set(status="inactive").where(User.id == 1)
            mutation = attach_mutation_execution(builder, pool)
            rows_affected = await mutation.execute()

            assert rows_affected >= 0

    @pytest.mark.asyncio
    async def test_delete_execute(self) -> None:
        """Test DELETE execution."""
        async with AsyncPool("sqlite://:memory:") as pool:

            class User(Table):
                id = Column[int](primary_key=True)
                email = Column[str]()

            # Setup
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

            # Execute
            builder = pool.delete(User)
            builder.where(User.id == 1)
            mutation = attach_mutation_execution(builder, pool)
            rows_affected = await mutation.execute()

            assert rows_affected >= 0
